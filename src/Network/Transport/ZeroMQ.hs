{-# LANGUAGE DeriveGeneric, StandaloneDeriving, OverloadedStrings, DeriveDataTypeable #-}
{-# LANGUAGE LambdaCase #-}
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
import           Control.Exception
      ( try
      , throwIO
      )
import           Control.Monad
      ( when
      , void
      , forever
      , join
      , replicateM_
      )
import           Control.Monad.Catch
      ( bracket 
      )
import           Control.Monad.IO.Class

import           Data.Binary
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B8
import           Data.IORef
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

import           Text.Printf

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
      , _transportEndPoints :: Map Int LocalEndPoints
      }

type LocalEndPoints = Counter Word32 LocalEndPoint

type IncommingConnections = Counter ConnectionId (Weak ZMQConnection)

type PendingConnections = Counter ConnectionId (Weak ZMQConnection)

-- | Messages
data ZMQMessage 
      = MessageConnect -- ^ Connection greeting
      | MessageConnectOk !ByteString
      | MessageInitConnection !ConnectionId !EndPointAddress !Reliability !EndPointAddress
      | MessageInitConnectionOk !ConnectionId !ConnectionId
      | MessageCloseConnection !ConnectionId
      | MessageData !ConnectionId
      deriving (Generic)

instance Binary ZMQMessage

data ZMQAction
        = ActionConnectHost !ByteString !(MVar RemoteHost)
        | ActionCloseEP !ByteString !EndPointAddress

data TransportEvents
        = TransportEndPointCreate
        | TransportEndPointClose
        | TransportClose


createTransport :: ZeroMQParameters -- ^ Transport features.
                -> ByteString       -- ^ Host.
                -> IO (Either (TransportError Void) Transport)
createTransport params host port = do
    transportChannel <- newChan
    let vstate = ValidTransportState transportChannel M.empty
    mstate <- newMVar $ TransportValid vstate
    let transport = ZeroMQTransport addr mstate 
    closed <- newEmptyMVar

    try $ do
      needContinue <- newIORef True
      a <- A.async $ do
          readChan transportChannel >>= \case
            TransportEndPointCreate ->
              {- XXX: create endpoint -} 
              return ()
            TransportEndPointClose ->
              return ()
            TransportClose ->
              {- XXX: close transport -}
              -- notify all endpoints about endpoint close
              -- notify all remote hosts about endpoint close
              return ()
          putMVar closed ()
      A.link a
      return $ Transport
          { newEndPoint    = error "apiNewEndPoint transport"
          , closeTransport = do
              writeIORef needContinue False
              void $ readMVar closed
          } 
  where
    addr = B.concat ["tcp://",host]
{-
    socketAddr = addr

    accure transport = do
      router <- ZMQ.socket ZMQ.Pull
      case authorizationType params of
          ZeroMQNoAuth -> return ()
          ZeroMQAuthPlain p u -> do
              ZMQ.setPlainServer True router
              ZMQ.setPlainPassword (ZMQ.restrict p) router
              ZMQ.setPlainUserName (ZMQ.restrict u) router
      ZMQ.setSendHighWM (ZMQ.restrict (highWaterMark params)) router
      ZMQ.setLinger (ZMQ.restrict (lingerPeriod params)) router

      ZMQ.setIdentity (ZMQ.restrict socketAddr) router
      ZMQ.bind router (B8.unpack addr)
      queue <- ZMQ.async $ processQueue transport
      liftIO $ A.link queue
      return (router, queue)

    release transport (router, queue) = do
      liftIO $ do
        modifyMVar_ (_transportState transport) $ \_ -> return TransportClosed
        mapM_ (A.cancel) [queue]
      ZMQ.unbind router (B8.unpack addr)
      ZMQ.close router
      -- XXX: verify that router is really closed

    processQueue transport = do
      (TransportValid (ValidTransportState _ _ chan _ _)) <- liftIO $ readMVar (_transportState transport)
      forever $ do
        action <- liftIO $ readChan chan
        case action of
          ActionConnectHost ident box -> do
              liftIO $ printf "[%s]: [internal] action connect host\n" (B8.unpack socketAddr)
              host <- createRemoteHost transport ident
              liftIO $ putMVar box host
          ActionCloseEP _ident _addr -> do
              liftIO $ printf "[%s]: [internal] action close ep" (B8.unpack socketAddr)
              return ()
-}

{-
apiNewEndPoint :: ZeroMQTransport -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint transport = do
    withTransportState transport $ \case
      TransportStateValid vstate -> modifyMVar (transportEndPoint vstate) $ do
        runZMQ $ do
          -- channel
          chan <- newTMChanIO

          pull <- socket ZMQ.Pull
          port <- bindFromRangeRandom pull
                      (transportAddress vstate)
                      (transportMinPort vstate)
                      (transportMaxTries vstate)

          -- sender
          lep <- rec do
            sender <- A.async (sender pull lep)
            lep <- LocalEndPoint
                      <$> pure (EndPointAddress $ B.concat 
                                  [ transportAddress transport
                                  , ":"
                                  , B8.pack $ show port])
                      <*> newMVar (LocalEndPointValid (ValidLocalEndPointState chan M.empty))
                      <*> sender
            return lep
          let result = Right $ EndPoint
                { receive = atomically $ do
                    mx <- readTMChan chan
                    case mx of
                      Nothing -> error "channel is closed"
                      Just x  -> return x
                , address = _localEndPointAddress ep
                , connect = apiConnect transport ep
                , closeEndPoint = do
                      modifyMVar_ (_localEndPointState ep) (return . const LocalEndPointClosed)
                      atomically $ do
                          writeTMChan chan EndPointClosed
                          closeTMChan chan
                      -- XXX: unsubscribe and close sockets
                , newMulticastGroup     = return . Left $
                      TransportError NewMulticastGroupUnsupported "Multicast not supported"
                , resolveMulticastGroup = return . return . Left $ 
                      TransportError ResolveMulticastGroupUnsupported "Multicast not supported"
                }

          return (M.insert port LocalEndPoint m, result)
    where
      recipient lep pull = ZMQ.runZMQ $ forever
          (identity:cmd:msgs) <- ZMQ.receiveMulti pull 
          case decode' cmd of
            MessageConnect -> liftIO $ do
              printf "[%s]: [socket] message connect %s \n" 
                     (B8.unpack socketAddr)
                     (B8.unpack identity)
              void $ createOrGetRemoteEP (localEndPointState lep) identity
            MessageInitConnection theirId theirEp rel ep -> liftIO $ do
              printf "[%s]: message init connection: {theirId: %i, theirEp: %s, ep: %s) \n" 
                 (B8.unpack socketAddr)
                 theirId
                 (B8.unpack . endPointAddressToByteString $ theirEp)
                 (B8.unpack . endPointAddressToByteString $ ep)
              modifyMVar (localEndPointState lep) $ \vstate ->
                LocalEndPointValid x -> do
                    rep  <- createOrGetRemoteEP (localEndPointState lep) theirEp
                    (idx, m) <- nextElementM (const $ return True) -- FIXME: check if connection is Valid
                                   (\n' -> ZMQConnection 
                                              <$> pure rep
                                              <*> newMVar (ZMQConnectionValid (ValidZMQConnection n')))
                    atomically $ writeTMChan chan $ ConnectionOpened idx rel theirEp
                    -- TODO: update state
                    -- TODO: send new id
                    return $ LocalEndPointValid i{localEndPointConnection=m}
              {-
              case ret of
                Nothing -> return () -- XXX: reply with error
                Just (ourId, conn) -> do
                  remoteHostAddEndPointConnection rep conn
                  remoteHostSendMessageLock host [encode' $ MessageInitConnectionOk theirId ourId]
              -}
            MessageCloseConnection idx -> liftIO $ do
              printf "[%s]: message close connection\n" (B8.unpack socketAddr)
              mconn <- closeIncommingConnection vstate idx identity
              case mconn of
                Nothing -> return ()
                Just conn -> do
                  host <- createOrGetHostById vstate identity
                  remoteHostCloseConnection host conn idx
            MessageInitConnectionOk ourId theirId -> liftIO $ do
              printf "[%s]: [mainloop] message init connection ok: {ourId: %i, theirId: %i}\n"
                    (B8.unpack socketAddr)
                    ourId
                    theirId
              liftIO $ modifyMVar_ (_transportPending vstate) $ \pconns ->
                case ourId `M.lookup` counterValue pconns of
                    Nothing -> do
                        printf "[%s]: [mainloop] pending connection not found\n" (B8.unpack socketAddr)
                        return pconns 
                    Just c -> deRefWeak c >>= \case
                      Just cn -> do
                        rd <- modifyMVar (connectionState cn) $ \case
                                ZMQConnectionInit -> do
                                  liftIO $ printf "[%s]: [mainloop] connection initialized\n" (B8.unpack socketAddr)
                                  return (ZMQConnectionValid (ValidZMQConnection theirId), True)
                                st -> do
                                  liftIO $ printf "[%s]: [mainloop] incorrect state\n" (B8.unpack socketAddr)
                                  return (st, False)
                        when rd $ putMVar (connectionReady cn) ()
                        return (pconns{counterValue=ourId `M.delete` (counterValue pconns)})
                      Nothing -> do
                        liftIO $ printf "[%s]: [mainloop] connection freed\n" (B8.unpack socketAddr)
                        return (pconns{counterValue=ourId `M.delete` (counterValue pconns)})
            MessageData idx -> liftIO $ do
              printf "[%s]: [mainloop] message data (%i) \n" (B8.unpack socketAddr) idx
              withMVar (_transportConnections vstate) $ \(Counter _ x) ->
                case idx `M.lookup` x of
                  Nothing -> return ()
                  Just cn -> deRefWeak cn >>= \case
                      Just (ZMQConnection _ lep _ _) ->
                        withMVar (_localEndPointState lep) $ \case
                          LocalEndPointValid (ValidLocalEndPointState ch _ _)  ->
                            atomically $ writeTMChan ch (Received idx msgs)
                      Nothing -> return () -- XXX: report error
      send push = bracket accure release $ \(push, ch) -> forever $ do
           msgs  <- liftIO $ readChan ch
           ZMQ.sendMulti push $ ident :| msgs
           liftIO yield
        where
          accure = do
            push <- ZMQ.socket ZMQ.Push
            ZMQ.connect push (B8.unpack addr)
            ch <- liftIO newChan
            ZMQ.sendMulti push $ ident :| [encode' MessageConnect]
            _ <- liftIO $ swapMVar state (RemoteHostValid (ValidRemoteHost ch M.empty))
            liftIO $ putMVar ready ()
            return (push, ch)
          release (push, ch) = do
            liftIO $ modifyMVar_ state $ const $ return RemoteHostClosed
            ZMQ.disconnect push (B8.unpack addr)
            ZMQ.close push


apiConnect :: LocalEndPoint
           -> ZeroMQTransport
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect ourEp transport theirAddr reliability _hints = do
    host <- withTransportState transport
               (error "transport is closed") -- XXX: return Left
               $ \v -> do
      modifyMVar (_remoteHosts v) $ \m -> do
        case uri `M.lookup` m of
          Nothing -> do
            x <- registerRemoteHost v uri
            return (M.insert uri x m, x)
          Just x -> return (m, x)
    _    <- readMVar (_remoteHostReady host)
    theirEp <- createOrGetRemoteEP host theirAddr
    etr <- withTransportState transport
                              (return $ Left $ TransportError ConnectFailed "Transport is closed")
                              $ \v -> 
        modifyMVar (_localEndPointState ourEp) $ \case
          LocalEndPointClosed -> return (LocalEndPointClosed, Left $ TransportError ConnectFailed "LocalEndPoint is closed")
          w@(LocalEndPointValid i) -> do
            res <- remoteHostOpenConnection v host ourEp theirEp reliability
            case res of
              Right (cid, c) ->
                return (LocalEndPointValid i{_localEndPointOutgoingConnections=M.insert cid c (_localEndPointOutgoingConnections i)}, Right c)
              Left e -> return  (w, Left e)
    case etr of
      Left te -> return (Left te)
      Right conn -> do
        return . Right $ Connection
          { send = apiSend transport conn
          , close = do
              print "connetion close\n"
              apiCloseConnection transport conn
          }
  where
    uri = apiGetUri theirAddr reliability -- XXX: add reliabitily support

apiSend :: ZeroMQTransport -> ZMQConnection -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
apiSend _transport connection bs = do
    printf "apiSend\n"
    withMVar (connectionState connection) $ \case -- XXX: check lock order
      ZMQConnectionValid (ValidZMQConnection cid) -> do
        printf "locking remotehost\n"
        z <- withMVar (_remoteHostState . remoteEndPointHost . connectionRemoteEndPoint $ connection) $ \case
          RemoteHostValid (ValidRemoteHost ch _) -> do
              writeChan ch (encode' (MessageData cid):bs)
              return $ Right ()
          RemoteHostPending -> do
              printf "pending\n"
              -- XXX: make it asynchronous
              return $ Left $ TransportError SendFailed "Connection is not enstablished."
          RemoteHostClosed  -> do
              printf "closed\n"
              return $ Left $ TransportError SendClosed "Remote host is closed."
        printf "unlocked\n"
        return z
      ZMQConnectionInit -> return $ Left $ TransportError SendFailed "Connection is in initialization phase." --XXXL check
      ZMQConnectionClosed -> return $ Left $ TransportError SendClosed "Connection is closed!"

-- Use locks: ConnectionState/RemoteHostState
apiCloseConnection :: ZeroMQTransport -> ZMQConnection -> IO ()
apiCloseConnection _transport connection = do
    modifyMVar_ (connectionState connection) $ \case
      ZMQConnectionValid (ValidZMQConnection cid) -> do
        withMVar (_remoteHostState . remoteEndPointHost .  connectionRemoteEndPoint $ connection) $ \case
          RemoteHostValid (ValidRemoteHost ch _) -> do
              writeChan ch [encode' (MessageCloseConnection cid)]
              return ZMQConnectionClosed
          _ -> return ZMQConnectionClosed
      x -> return x

-- Use locks: TransportConnection
closeIncommingConnection :: ValidTransportState
                         -> ConnectionId -> ByteString -> IO (Maybe ZMQConnection)
closeIncommingConnection v idx ident =
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

apiGetUri :: EndPointAddress -> Reliability -> ByteString
apiGetUri addr _rel = B8.init $ fst $ B8.breakEnd (=='/') $ endPointAddressToByteString addr

apiGetEndPointId :: EndPointAddress -> Word32
apiGetEndPointId epa = read . B8.unpack . snd $ B8.breakEnd (=='/') $ endPointAddressToByteString epa

encode' :: Binary a => a  -> ByteString
encode' = B.concat . BL.toChunks . encode

decode' :: Binary a => ByteString -> a
decode' s = decode . BL.fromChunks $ [s]

-- Helpers
-}
repeatWhile :: MonadIO m => m Bool -> m () -> m ()
repeatWhile f g = f >>= flip when (g >> repeatWhile f g)

{-
withTransportState :: ZeroMQTransport -> IO a -> (ValidTransportState -> IO a) -> IO a
withTransportState t err f = withMVar (_transportState t) $ \case
  TransportClosed -> err
  TransportValid v -> f v

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

createOrGetRemoteEP :: RemoteHost -> EndPointAddress -> IO RemoteEndPoint
createOrGetRemoteEP host ep =
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

remoteHostSendMessage :: ValidRemoteHost -> [ByteString] -> IO ()
remoteHostSendMessage (ValidRemoteHost ch _) msgs = writeChan ch msgs

remoteHostReconnect :: RemoteHost -> IO ()
remoteHostReconnect = error "remoteHostReconnect"

-- | 
-- Locks: */RemoteHostState
-- RequireLocks: RemoteHosts
createRemoteHost :: ZeroMQTransport -> ByteString -> ZMQ.ZMQ z RemoteHost
createRemoteHost transport addr = do
    state <- liftIO (newMVar RemoteHostPending)
    ready <- liftIO newEmptyMVar
    asnk  <- liftIO newEmptyMVar
    x <- ZMQ.async $ bracket 
       (do push <- ZMQ.socket ZMQ.Push
           ZMQ.connect push (B8.unpack addr)
           ch <- liftIO newChan
           ZMQ.sendMulti push $ ident :| [encode' MessageConnect]
           _ <- liftIO $ swapMVar state (RemoteHostValid (ValidRemoteHost ch M.empty))
           liftIO $ putMVar ready ()
           return (push, ch))
       (\(push, ch) -> do
           liftIO $ modifyMVar_ state $ const $ return RemoteHostClosed
           ZMQ.disconnect push (B8.unpack addr)
           ZMQ.close push
       ) $ \(push, ch) -> forever $ do -- XXX: close someday
       msgs  <- liftIO $ readChan ch
       ZMQ.sendMulti push $ ident :| msgs
       liftIO yield
    liftIO $ do
      A.link x
      putMVar asnk x
    return $ RemoteHost addr state ready
  where
    ident = transportAddress transport
-}
