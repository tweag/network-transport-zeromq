{-# LANGUAGE DeriveGeneric, StandaloneDeriving, OverloadedStrings, DeriveDataTypeable #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecursiveDo #-}
module Network.Transport.ZeroMQ
  ( -- * Main API
    createTransport
  , ZeroMQParameters(..)
  , ZeroMQAuthType(..)
  , defaultZeroMQParameters
  -- * Internals
  -- * Design
  ) where

import           Control.Applicative
import           Control.Concurrent
       ( yield
       , threadDelay
       )
import qualified Control.Concurrent.Async as A
import           Control.Concurrent.Chan
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TMChan
import           Control.Monad
      ( void
      , forever
      , join
      , forM_
      )
import           Control.Monad.Catch
      ( bracket 
      , finally
      , onException
      , try
      , SomeException
      )
import           Control.Monad.IO.Class

import           Data.Binary
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B8
import           Data.List.NonEmpty
import           Data.Maybe
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Typeable
import           Data.Void
import           GHC.Generics 
import           System.Mem.Weak

import Network.Transport
import Network.Transport.ZeroMQ.Types
import qualified System.ZMQ4.Monadic as ZMQ
import qualified System.ZMQ4.Utils   as ZMQ

-- ---------------------------------------------------------------------------
-- Missing instances
-- ---------------------------------------------------------------------------
deriving instance Generic Reliability
deriving instance Typeable Reliability
instance Binary Reliability


-- XXX: we may want to introduce a new level of indirection: socket -> endpoint
-- XXX: when incrementing endpoint we need to check that we have no node
-- XXX: do we want to keep secret number to protect connection from beign
--      hijecked

-- =========================================================================== 
-- =    Internal datatypes                                                   =
-- ===========================================================================

-- In zeromq backend we are using next address scheme:
--
-- scheme://host:port/EndPointId
--  |       |     |
--  |       +-----+---------------- can be configured by user, one host,
--  |                               port pair per distributed process
--  |                               instance
--  +------------------------------ reserved for future use, depending on
--                                  the socket type, currently 
--
-- Reliable connections:
--
-- As all communication with ZeroMQ should be provided in special threads that
-- were started under runZMQ or ZMQ.async, we are using communication channel
-- to send messages to that threads, this will have a little overhead over direct
-- usage of zmq, if we'd carry zmq context with send.
--
-- XXX: really it's possible to do, but this will require changes in zeromq4-haskell API
--
-- Main-thread contains 3 subthreads:
--   monitor     - monitors incomming and disconnected connections
--   main-thread - polls on incomming messages from ZeroMQ
--   queue       - polls on incomming messages from distributed-process
--
-- 
--
-- Connections.
--    ZeroMQ automatically handles connection liveness. This breaks some assumptions
-- about connectivity and leads to the problems with connection handling.
--
-- Heaviweight connection states:
--
--   Init    -- connection is creating
--   Valid   -- connection is created and validated
--   Closed  -- connection is closed 
--
-- To create a global connection between hosts we need to perform:
--
--    1. Local side:  calls ZMQ.connect 
--    2. Remote side: handles ZMQ.Accepted event with address of the local node,
--                    and replies back with message "CONNECT hostid", 
--
-- From this moment both hosts may use this connection.
--
-- *NOTE:* current implementation do not try to create doubledirected connection instread
--         it creates 2 unidirected connection from each side, this will simplify first 
--         implementation version, but may be changed in the future.
--    
-- To create new lightweigh connection:
--
--    1. Local side: sends control message MessageInitConnection Reliability EndPoint Id
--    2. Remote side: registers incomming connection and replies with new connection Id
--        MessageInitConnectionOK Word64
--    3. Local side: receives control message
--                     
-- *NOTE:* Current implementation uses unpinned types where it's possible to prevent
--    memory fragmentation. It was not measured if it have a good impact on the performance.
--
-- Structure of message:
--
--   host-identifier:MessageType:Payload
--      |                |        |
--      |                |        +----------- [[ByteString]] 
--      |                +-------------------- ZeroMQControl Message
--      +------------------------------------- Unique host-id (basically host url)

type TransportAddress = ByteString

-- | Transport data type.
data ZeroMQTransport = ZeroMQTransport
    { transportAddress :: !TransportAddress
    -- ^ Transport address (used as identifier).
    , _transportState  :: !(MVar TransportState)
    -- ^ Internal state.
    }

-- | Transport state.
data TransportState
      = TransportValid !ValidTransportState         -- ^ Transport is in active state.
      | TransportClosed                             -- ^ Transport is closed.

-- | Transport state.
data ValidTransportState = ValidTransportState
      { _transportChan   :: !(Chan TransportEvents)
      , _transportEndPoints :: Map Int LocalEndPoint
      }

-- | Messages
data ZMQMessage 
      = MessageConnect -- ^ Connection greeting
      | MessageConnectOk !ByteString
      | MessageInitConnection !ConnectionId !EndPointAddress !Reliability
      | MessageInitConnectionOk !ConnectionId !ConnectionId
      | MessageCloseConnection !ConnectionId
      | MessageData !ConnectionId
      | MessageEndPointClose
      deriving (Generic)

instance Binary ZMQMessage

data ZMQAction
        = ActionConnectHost !ByteString !(MVar RemoteHost)
        | ActionCloseEP !ByteString !EndPointAddress

data TransportEvents
        = TransportEndPointCreate (MVar (Either (TransportError NewEndPointErrorCode) LocalEndPoint))
        | TransportEndPointClose Int
        | TransportClose

createTransport :: ZeroMQParameters -- ^ Transport features.
                -> ByteString       -- ^ Host.
                -> IO (Either (TransportError Void) Transport)
createTransport params host = do
    chan   <- newChan
    let vstate = ValidTransportState chan M.empty
    mstate <- newMVar $ TransportValid vstate
    let transport = ZeroMQTransport addr mstate 

    try $ do
      closed <- newEmptyMVar
      a <- A.async $ ZMQ.runZMQ (mainloop mstate chan `finally` shutdown closed mstate)
      A.link a
      return $ Transport
          { newEndPoint    = apiNewEndPoint transport
          , closeTransport = do
              writeChan chan TransportClose
              void $ readMVar closed
          } 
  where
    addr = B.concat ["tcp://",host]
    mainloop mstate chan = liftIO (readChan chan) >>= go
      where 
        go (TransportEndPointCreate reply) = do
            eEndPoint <- endPointCreate params (B8.unpack host)
            liftIO $ putMVar reply =<< case eEndPoint of
              Right (port,ep) -> liftIO $ modifyMVar mstate $ \case
                TransportValid i -> return 
                  ( TransportValid i{_transportEndPoints = M.insert port ep (_transportEndPoints i)}
                  , Right ep)
                TransportClosed -> return 
                  ( TransportClosed
                  , Left $ TransportError NewEndPointFailed "Transport is closed.")
              Left e -> return $ Left e
            mainloop mstate chan
        go (TransportEndPointClose idx) = do
            liftIO $ modifyMVar_ mstate $ \case
              s@(TransportValid (ValidTransportState c m)) -> do
                case idx `M.lookup` m of
                  Nothing -> return s
                  Just lep  -> do
                    old <- modifyMVar (_localEndPointState lep) (\x -> return (LocalEndPointClosed, x))
                    case old of
                      LocalEndPointValid (ValidLocalEndPointState _ _ a) -> A.cancel a
                      _ -> return () 
                    return $ TransportValid $ ValidTransportState c (M.delete idx m)
              TransportClosed -> return TransportClosed
            mainloop mstate chan
        go TransportClose = return ()
    shutdown closed mstate = liftIO $ do
      modifyMVar_ mstate $ \case
        TransportValid (ValidTransportState _ m) -> do
          forM_ (M.elems m) $ \lep -> do
            old <- modifyMVar (_localEndPointState lep) (\x -> return (LocalEndPointClosed, x))
            case old of
              LocalEndPointValid (ValidLocalEndPointState _ _ a) -> A.cancel a
              _ -> return () 
          return $ TransportClosed
        TransportClosed -> return $ TransportClosed 
      liftIO $ putMVar closed ()

apiNewEndPoint :: ZeroMQTransport -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint transport = join $ withMVar (_transportState transport) inner
  where
    inner TransportClosed = return $
        return $ Left $ TransportError NewEndPointFailed "Transport is closed."
    inner (TransportValid (ValidTransportState ch _)) = do
        reply <- newEmptyMVar 
        writeChan ch (TransportEndPointCreate reply)
        return $ do
          elep <- takeMVar reply
          case elep of
            Right ep ->
              withMVar (_localEndPointState ep) $ \case
                LocalEndPointValid (ValidLocalEndPointState chIn chOut _) ->
                  return $ Right 
                         $ EndPoint
                    { receive = atomically $ do
                        mx <- readTMChan chOut
                        case mx of
                          Nothing -> error "channel is closed"
                          Just x  -> return x
                    , address = _localEndPointAddress ep
                    , connect = apiConnect ep
                    , closeEndPoint = do
                        modifyMVar_ (_localEndPointState ep) (return . const LocalEndPointClosed)
                        writeChan chIn LocalEndPointClose
                        atomically $ closeTMChan chOut
                    , newMulticastGroup     = return . Left $
                        TransportError NewMulticastGroupUnsupported "Multicast not supported"
                    , resolveMulticastGroup = return . return . Left $ 
                        TransportError ResolveMulticastGroupUnsupported "Multicast not supported"
                    }
                _ -> return $ Left $ TransportError NewEndPointFailed "Endpoint is closed."
            Left e -> return $ Left e

apiConnect :: LocalEndPoint
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect ourEp theirAddr reliability _hints = join $
    withMVar (_localEndPointState ourEp) $ \case
      LocalEndPointValid (ValidLocalEndPointState chIn _ _) -> do
        reply <- newEmptyMVar
        writeChan chIn (LocalEndPointConnectionOpen ourEp theirAddr reliability reply)
        return $ takeMVar reply
      LocalEndPointClosed -> return $ return $ Left $ TransportError ConnectFailed "LocalEndPoint is closed"

data EndPointThreadState = EndPointThreadState
        { endPointConnections :: Counter ConnectionId ZMQConnection
        , endPointRemotes     :: Map EndPointAddress RemoteEndPoint
        }

endPointCreate :: ZeroMQParameters -> String -> ZMQ.ZMQ z (Either (TransportError NewEndPointErrorCode) (Int,LocalEndPoint))
endPointCreate params address = do
    em <- try $ accure
    case em of
      Right (port,pull) -> do
          chIn  <- liftIO $ newChan
          chOut <- liftIO $ newTMChanIO
          state <- liftIO $ newMVar (EndPointThreadState (Counter 0 M.empty) M.empty)
          let addr = EndPointAddress $ B8.pack (address ++ ":" ++ show port)
          receiverThread <- ZMQ.async $ receiver pull addr state chOut `finally` release port pull
          mainThread     <- ZMQ.async $ go pull addr state chIn `finally` finalizeEndPoint state receiverThread
          mt <- liftIO $ newMVar (LocalEndPointValid $ ValidLocalEndPointState chIn chOut mainThread)
          return $ Right (port, LocalEndPoint addr mt)
      Left (_::SomeException)  -> return $ Left $ TransportError NewEndPointInsufficientResources "no free sockets"
  where
    receiver pull ourEp mstate chan = forever $ do
      (identity:cmd:msgs) <- ZMQ.receiveMulti pull 
      let theirAddress  = EndPointAddress identity
      case decode' cmd of
        MessageData idx -> join $ liftIO $ 
          withMVar mstate $ \(EndPointThreadState (Counter _ c) _) ->
            case idx `M.lookup` c of
              Just _  -> do
                atomically $ writeTMChan chan (Received idx msgs)
                return $ return ()
              Nothing -> return $ markRemoteHostFailed mstate theirAddress 
        MessageConnect -> do
          void $ createOrGetRemoteEndPoint mstate ourEp theirAddress
        MessageInitConnection theirId theirEp rel -> join $ liftIO $ 
          modifyMVar mstate $ \c@(EndPointThreadState (Counter i m) r) ->
            case theirEp `M.lookup` r of
              Nothing  -> return (c, markRemoteHostFailed mstate theirAddress)
              Just rep -> withMVar (remoteEndPointState rep) $ \case
                RemoteEndPointClosed -> undefined 
                RemoteEndPointValid (ValidRemoteEndPoint ch _) -> do                                  -- XXX: count incomming
                  writeChan ch [encode' $ MessageInitConnectionOk theirId (succ i)]
                  conn <- ZMQConnection <$> pure rep
                                        <*> pure rel
                                        <*> newMVar (ZMQConnectionValid $ ValidZMQConnection (succ i))
                                        <*> newEmptyMVar
                  return $ (EndPointThreadState (Counter (succ i) (M.insert (succ i) conn m)) r, return ())
        MessageCloseConnection idx ->
          remoteEndPointCloseConnection mstate idx
        MessageInitConnectionOk ourId theirId -> do
          rep <- createOrGetRemoteEndPoint mstate ourEp theirAddress
          remoteEndPointRegisterConnection rep ourId theirId
        MessageEndPointClose -> do
          rep <- createOrGetRemoteEndPoint mstate ourEp theirAddress
          remoteEndPointClose rep
    go pull ourAddress mstate chIn = liftIO (readChan chIn) >>= \case
      LocalEndPointConnectionOpen ourEp theirAddress rel reply -> do
        host <- createOrGetRemoteEndPoint mstate ourAddress theirAddress
        econn <- remoteEndPointOpenConnection host rel
        liftIO . putMVar reply $ 
            case econn of
              Left e  -> Left e
              Right c -> Right $ Connection 
                  { send  = apiSend c
                  , close = apiClose c
                  }
        go pull ourAddress mstate chIn
      LocalEndPointConnectionClose idx  -> do
        remoteEndPointCloseConnection mstate idx
        go pull ourAddress mstate chIn
      LocalEndPointClose{}           -> return ()
    finalizeEndPoint mstate receiver = liftIO $ do
      withMVar mstate $ \(EndPointThreadState _ rp) ->
        forM_ (M.elems rp) $ \(RemoteEndPoint _ x _) -> A.cancel x
      A.cancel receiver
    accure = do
      pull <- ZMQ.socket ZMQ.Pull
      case authorizationType params of
          ZeroMQNoAuth -> return ()
          ZeroMQAuthPlain p u -> do
              ZMQ.setPlainServer True pull 
              ZMQ.setPlainPassword (ZMQ.restrict p) pull
              ZMQ.setPlainUserName (ZMQ.restrict u) pull
      ZMQ.setSendHighWM (ZMQ.restrict (highWaterMark params)) pull
      ZMQ.setLinger (ZMQ.restrict (lingerPeriod params)) pull

      port <- ZMQ.bindFromRangeRandom pull address (minPort params) (maxPort params) (maxTries params)
      return (port, pull)
    release port pull = do
      ZMQ.unbind pull (address ++ ":" ++ show port)
      ZMQ.close pull


apiSend :: ZMQConnection -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
apiSend c@(ZMQConnection e _ s r) b = join $ withMVar (remoteEndPointState e) $ \case
  RemoteEndPointClosed  -> return $ return $ Left $ TransportError SendClosed "Remote end point closed."
  RemoteEndPointPending -> return $ yield >> apiSend c b
  RemoteEndPointValid  (ValidRemoteEndPoint ch _) -> withMVar s $ \case
    ZMQConnectionInit   -> return $ yield >> apiSend c b
    ZMQConnectionClosed -> return $ return $ Left $ TransportError SendClosed "Connection is closed"
    ZMQConnectionValid  (ValidZMQConnection idx) -> do 
      writeChan ch $ (encode' $ MessageData idx):b
      return $ return $ Right ()

-- TODO: move this functionality to the internal function
apiClose :: ZMQConnection -> IO ()
apiClose c@(ZMQConnection e _ s r) = withMVar (remoteEndPointState e) $ \case
  RemoteEndPointClosed -> return ()
  RemoteEndPointPending -> return () -- XXX: problem
  RemoteEndPointValid (ValidRemoteEndPoint ch _) -> modifyMVar_ s $ \case
    ZMQConnectionInit -> return ZMQConnectionInit -- XXX: problem
    ZMQConnectionClosed -> return ZMQConnectionClosed
    ZMQConnectionValid (ValidZMQConnection idx) -> do
        writeChan ch $ [encode' $ MessageCloseConnection idx]
        -- XXX: notify localEndPointProcess
        return ZMQConnectionClosed

-- | Remote end point connection encapsulated into a thread
createOrGetRemoteEndPoint :: MVar EndPointThreadState
                          -> EndPointAddress
                          -> EndPointAddress
                          -> ZMQ.ZMQ z RemoteEndPoint
createOrGetRemoteEndPoint mstate ourEp ep = do
    m <- liftIO $ takeMVar mstate
    go m `onException` (liftIO $ putMVar mstate m)
  where
    ident = endPointAddressToByteString ourEp
    go m = case ep `M.lookup` endPointRemotes m of
       Nothing -> do
          push  <- ZMQ.socket ZMQ.Push
          chan  <- liftIO newChan
          state <- liftIO $ newMVar RemoteEndPointPending
          a  <- ZMQ.async $ run push state chan `finally` shutdown push state
          let rp = RemoteEndPoint ep a state
          liftIO (putMVar mstate $ m{endPointRemotes = M.insert ep rp (endPointRemotes m)})
          return rp
       Just t -> do
          liftIO (putMVar mstate m)
          return t
    run push state chan = initialize >> mainloop
      where
        initialize = do
            ZMQ.sendMulti push $ ident :| [encode' MessageConnect]                          -- XXX: [msg]
            void . liftIO $ swapMVar state (RemoteEndPointValid (ValidRemoteEndPoint chan (Counter 0 M.empty)))
        mainloop   = forever $ do
            x <- liftIO $ readChan chan
            ZMQ.sendMulti push $ ident :| x                                                 -- XXX: [msg]
            liftIO yield
    shutdown push state = do
        void . liftIO $ swapMVar state RemoteEndPointClosed
        ZMQ.sendMulti push $ ident :| [encode' MessageEndPointClose]                        -- XXX: [msg]
        ZMQ.disconnect push (B8.unpack $ endPointAddressToByteString ep)
        ZMQ.close push

-- | XXX: This function is not asynchronous as possible
remoteEndPointOpenConnection :: RemoteEndPoint -> Reliability -> ZMQ.ZMQ z (Either (TransportError ConnectErrorCode) ZMQConnection)
remoteEndPointOpenConnection x@(RemoteEndPoint addr _ state) rel = join . liftIO $
  modifyMVar state $ \case
    RemoteEndPointClosed ->
      return (RemoteEndPointClosed, return $ Left $ TransportError ConnectFailed "Transport is closed.")
    RemoteEndPointValid (ValidRemoteEndPoint c (Counter i m)) -> do
      conn <- ZMQConnection <$> pure x 
                            <*> pure rel
                            <*> newMVar ZMQConnectionInit
                            <*> newEmptyMVar
      let i' = succ i
      return ( RemoteEndPointValid (ValidRemoteEndPoint c (Counter i' (M.insert i' conn m)))
             , do liftIO $ writeChan c [encode' $ MessageInitConnection i' addr rel]
                  return $ Right conn
             )
    RemoteEndPointPending -> return (RemoteEndPointPending, remoteEndPointOpenConnection x rel)


remoteEndPointCloseConnection :: MVar EndPointThreadState -> ConnectionId -> ZMQ.ZMQ z a
remoteEndPointCloseConnection = undefined 
{-
  case cid `M.lookup` connections of
    Nothing -> return connections
        Just cn -> atomically $ writeTMChan ch $ ConnectionClosed idx
                   -- XXX: notify RemoteEndPoint
                   return (Counter n (M.delete idx m), conn)
-}                   

-- Use locks: TransportConnection
closeIncommingConnection :: ValidTransportState
                         -> ConnectionId -> ByteString -> IO (Maybe ZMQConnection)
closeIncommingConnection v idx ident = undefined
{-        
    modifyMVar (_transportConnections v) $ \i@(Counter n m) -> do
      case idx `M.lookup` m of
        Nothing -> return (i, Nothing)
        Just cn -> deRefWeak cn >>= \case
            Just conn@(ZMQConnection _ lep _ _) ->
              withMVar (_localEndPointState lep) $ \case
                LocalEndPointValid (ValidLocalEndPointState ch _ _) -> do
                  atomically $ writeTMChan ch $ ConnectionClosed idx
                  -- XXX: notify socket maybe we want to close it
                  return (Counter n (M.delete idx m), Just conn)
                _ -> return (i, Nothing)
            Nothing -> do
              return (i, Nothing) -- XXX: notify
-}              

remoteEndPointRegisterConnection :: RemoteEndPoint -> ConnectionId -> ConnectionId -> ZMQ.ZMQ z a
remoteEndPointRegisterConnection = undefined


remoteEndPointClose :: RemoteEndPoint -> ZMQ.ZMQ z a
remoteEndPointClose = undefined

markRemoteHostFailed :: a -> EndPointAddress -> ZMQ.ZMQ z b
markRemoteHostFailed = undefined

remoteEndPointSendMessage = undefined
{-
  modifyMVar (_remoteHostState host) $ \case
    RemoteHostValid (ValidRemoteHost x m) -> do
        (rep, m') <- case ep `M.lookup` m of
          Just x -> return (x, m)
          Nothing -> do
            x <- RemoteEndPoint <$> pure ep
                                <*> pure host
                                <*> newMVar (RemoteEndPointValid $! ValidRemoteEndPoint M.empty)
            return (x, M.insert ep x m)
        return (RemoteHostValid (ValidRemoteHost x m'), rep)
    _ -> error "RemoteHost is not valid"
-}

encode' :: Binary a => a  -> ByteString
encode' = B.concat . BL.toChunks . encode

decode' :: Binary a => ByteString -> a
decode' s = decode . BL.fromChunks $ [s]

{-
---------------------------------------------------------------------------------
-- Remote host
---------------------------------------------------------------------------------
-- Remote host is represented by a process in ZMQ context that keep send
-- socket and RemoteHost enty in Transport hierarchy, the main reason for
-- this is that Rank2Types will not allow socket to escape ZMQ context

registerRemoteHost :: ValidTransportState -> TransportAddress -> IO RemoteHost
registerRemoteHost v uri = do
  x <- newEmptyMVar
  writeChan (transportChannel v)
            (ActionConnectHost uri x)
  takeMVar x

createOrGetHostById :: ValidTransportState -> ByteString -> IO RemoteHost
createOrGetHostById vstate uri = 
    modifyMVar (_remoteHosts vstate) $ \m -> do
      case uri `M.lookup` m of
        Nothing -> do
          x <- registerRemoteHost vstate uri 
          return (M.insert uri x m, x)
        Just x -> return (m, x)
-}


{-
remoteHostAddEndPointConnection :: RemoteEndPoint -> ZMQConnection -> IO ()
remoteHostAddEndPointConnection rep conn = do
  withMVar (connectionState conn) $ \case
    ZMQConnectionValid (ValidZMQConnection idx) -> do
      modifyMVar_ (_remoteEndPointState rep) $ \case
        RemoteEndPointClosed -> return RemoteEndPointClosed -- XXX: possibly we need to set connection state to closed
        RemoteEndPointValid (ValidRemoteEndPoint v) ->
          return (RemoteEndPointValid (ValidRemoteEndPoint $ M.insert idx conn v))
    _ -> return () -- XXX notify

remoteHostCloseConnection :: RemoteHost -> ZMQConnection -> ConnectionId -> IO ()
remoteHostCloseConnection host (ZMQConnection rep _ _ _) cid = do
    withMVar (_remoteHostState host ) $ \case
      RemoteHostValid (ValidRemoteHost _ m) ->
          modifyMVar_ (_remoteEndPointState rep) $ \case
            RemoteEndPointValid (ValidRemoteEndPoint m) -> return $
                    RemoteEndPointValid $ ValidRemoteEndPoint (cid `M.delete` m)
            x -> return x
      _ -> return () -- XXX: notify about error?

-- Use lock pending
-- RequireLock: TransportStat
remoteHostOpenConnection :: ValidTransportState 
                         -> RemoteHost
                         -> LocalEndPoint
                         -> RemoteEndPoint 
                         -> Reliability
                         -> IO (Either (TransportError ConnectErrorCode) (ConnectionId, ZMQConnection))
remoteHostOpenConnection v host ourEp theirEp rel = do
    x <- modifyMVar (_remoteHostState host) $ \case
      RemoteHostValid w@(ValidRemoteHost c m) -> do
        (theirEp, m') <- case theirAddr `M.lookup` m of
           Just theirEp -> return (theirEp, m)
           Nothing -> do
             rep <- RemoteEndPoint <$> pure theirAddr
                                   <*> pure host
                                   <*> newMVar (RemoteEndPointValid (ValidRemoteEndPoint M.empty))
             return $ (rep, M.insert theirAddr rep m)
        conn <- ZMQConnection <$> pure theirEp
                              <*> pure ourEp
                              <*> newMVar ZMQConnectionInit
                              <*> newEmptyMVar
        (cid, _) <- modifyMVar (_transportPending v) $
          nextElementM (fmap isNothing . deRefWeak) (const $ mkWeakPtr conn Nothing)
        remoteHostSendMessage w
          [encode' $ MessageInitConnection cid (_localEndPointAddress ourEp) rel theirAddr]
        return $ (RemoteHostValid (ValidRemoteHost c m'), Right (cid, conn))
      RemoteHostPending -> return  (RemoteHostClosed, Left $ TransportError ConnectFailed "We are here")
      RemoteHostClosed -> return $ (RemoteHostClosed, Left $ TransportError ConnectFailed "Remote host is closed.")
    return x
  where
    theirAddr = remoteEndPointAddress theirEp

remoteHostSendMessageLock :: RemoteHost -> [ByteString] -> IO ()
remoteHostSendMessageLock host msg = join $ withMVar (_remoteHostState host) $ \case
    RemoteHostValid v -> remoteHostSendMessage v msg >> return (return ())
    RemoteHostPending -> readMVar (_remoteHostReady host) >> return (remoteHostSendMessageLock host msg)
    RemoteHostClosed  -> remoteHostReconnect host >> return (remoteHostSendMessageLock host msg)
-}
