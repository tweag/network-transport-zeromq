{-# LANGUAGE DeriveGeneric, StandaloneDeriving, OverloadedStrings, DeriveDataTypeable #-}
module Network.Transport.ZMQ
  ( -- * Main API
    createTransport
  , ZMQParameters(..)
  , ZMQAuthType(..)
  , defaultZMQParameters
  -- * Internals
  -- * Design
  ) where

import Network.Transport.ZMQ.Types

import           Control.Applicative
import           Control.Concurrent
       ( yield
       , threadDelay
       )
import qualified Control.Concurrent.Async as Async
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
import           Data.Binary.Put
import           Data.Binary.Get
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B8
import           Data.IORef
      ( newIORef
      , modifyIORef
      , readIORef
      )
import           Data.List.NonEmpty
import           Data.Maybe
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Typeable
import           Data.Void
import           GHC.Generics 
import           System.Mem.Weak

import Network.Transport
import Network.Transport.ZMQ.Types
import qualified System.ZMQ4.Monadic as ZMQ
import qualified System.ZMQ4.Utils   as ZMQ

import Text.Printf

-- XXX: we may want to introduce a new level of indirection: socket -> endpoint
-- XXX: when incrementing endpoint we need to check that we have no node
-- XXX: do we want to keep secret number to protect connection from beign
--      hijecked

--------------------------------------------------------------------------------
--- Internal datatypes                                                        --
--------------------------------------------------------------------------------

-- In the zeromq backend we are using following address scheme:
--
-- scheme://host:port/EndPointId
--  |       |     |
--  |       +-----+---------------- can be configured by user, one host,
--  |                               port pair per distributed process
--  |                               instance
--  +------------------------------ The transport used by 0MQ.
--
-- Reliable connections:
--
-- As all communication with 0MQ should be provided in special threads that were
-- started under runZMQ or ZMQ.async, we are using communication channel to send
-- messages to those threads. This adds a little overhead over direct usage of
-- zmq, if we'd carry zmq context with send.
--
-- XXX: really it's possible to do, but this will require changes in zeromq4-haskell API
--
-- Main-thread contains 3 subthreads:
--   monitor     - monitors incomming and disconnected connections
--   main-thread - polls on incomming messages from 0MQ
--   queue       - polls on incomming messages from distributed-process
--
-- 
--
-- Connections.
--    0MQ automatically handles connection liveness. This breaks some
--    assumptions about connectivity and leads to problems with connection
--    handling.
--
-- Heavyweight connection states:
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
-- *NOTE:* current implementation do not try to create bidirectional connection.
--     Instead, it creates 2 unidirectional connection from each side. This will
--     simplify first implementation version, but may change in the future.
--
-- To create new lightweigh connection:
--
--    1. Local side: sends control message MessageInitConnection Reliability
--    EndPoint Id
--
--    2. Remote side: registers incomming connection and replies with new
--    connection Id MessageInitConnectionOK Word64
--
--    3. Local side: receives control message
--
-- *NOTE:* Current implementation uses unpinned types where it's possible to
--     prevent memory fragmentation. It was not measured if it have a good
--     impact on performance.
--
-- Structure of message:
--
--   host-identifier:MessageType:Payload
--      |                |        |
--      |                |        +----------- [[ByteString]]
--      |                +-------------------- ZMQControl Message
--      +------------------------------------- Unique host-id (basically host url)

type TransportAddress = ByteString

-- | Transport data type.
data ZMQTransport = ZMQTransport
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

-- $socket-system socket system is differ from the one that is used in
-- network-transport-tcp:
--
-- Each endpoint obtains one pull socket and push socket for each remote
-- end point, all lightweight threads abrigged into one heavyweight
-- connection.
--
-- Virually connections looks like the following plot:
--
-- +------------------+                                +--------------+
-- |  Endpoint        |                                |  Endpoint    |
-- |                  |                                |              |
-- |               +------+                        +------+           |
-- |               | pull |<-----------------------| push |           |
-- +               +------+                        +------+           |
-- |                  |     +--------------------+     |              |
-- +               +------+/~~~~ connection 1 ~~~~\+------+           |
-- |               | push |~~~~~ connection 2 ~~~~~| pull |           |
-- |               +------+\                      /+------+           |
-- |                  |     +--------------------+     |              |
-- +------------------+                                +--------------+
--
-- Physically zeromq may choose better representations and mapping on
-- a real connections.
--
-- Such design allowes to do not allow to distinguish messages from
-- different remote endpoint, so as a result it leads to different message
-- passing scheme

-- | Messages
data ZMQMessage
      = MessageConnect !EndPointAddress -- ^ Connection greeting
      | MessageInitConnection !EndPointAddress !ConnectionId !Reliability
      | MessageInitConnectionOk !EndPointAddress !ConnectionId !ConnectionId
      | MessageCloseConnection !ConnectionId
      | MessageData !ConnectionId
      | MessageEndPointClose !EndPointAddress
      deriving (Generic)

{- 
instance Binary ZMQMessage where
  put (MessageConnect ep) = putWord64le 0 >> put ep
  put (MessageInitConnection ep cid rel)   = putWord64le 2 >> put ep >> put cid >> put rel
  put (MessageInitConnectionOk ep cid rid) = putWord64le 3 >> put ep >> put cid >> put rid
  put (MessageCloseConnection cid)         = putWord64le 4 >> put cid
  put (MessageEndPointClose ep)            = putWord64le 5 >> put ep
  put (MessageData cid)                    = putWord64le cid
  get = do x <- getWord64be
           case x of
             0 -> MessageConnect <$> get
             2 -> MessageInitConnection   <$> get <*> get <*> get
             3 -> MessageInitConnectionOk <$> get <*> get <*> get
             4 -> MessageCloseConnection  <$> get
             5 -> MessageEndPointClose    <$> get
             6 -> MessageData <$> pure x

reservedConnectionId = 7
-}

instance Binary ZMQMessage

data TransportEvents
        = TransportEndPointCreate (MVar (Either (TransportError NewEndPointErrorCode) LocalEndPoint))
        | TransportEndPointClose Int
        | TransportClose

createTransport :: ZMQParameters -- ^ Transport features.
                -> ByteString       -- ^ Host.
                -> IO (Either (TransportError Void) Transport)
createTransport params host = do
    chan   <- newChan
    let vstate = ValidTransportState chan Map.empty
    mstate <- newMVar $ TransportValid vstate
    let transport = ZMQTransport addr mstate

    try $ do
      closed <- newEmptyMVar
      a <- Async.async $ ZMQ.runZMQ (mainloop mstate chan `finally` shutdown closed mstate)
      Async.link a
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
            liftIO $ print "[transport] endpoint create"
            eEndPoint <- endPointCreate params (B8.unpack addr)
            liftIO $ putMVar reply =<< case eEndPoint of
              Right (port,ep) -> liftIO $ modifyMVar mstate $ \case
                TransportValid i -> return
                  ( TransportValid i{_transportEndPoints = Map.insert port ep (_transportEndPoints i)}
                  , Right ep)
                TransportClosed -> return
                  ( TransportClosed
                  , Left $ TransportError NewEndPointFailed "Transport is closed.")
              Left e -> return $ Left e
            mainloop mstate chan
        go (TransportEndPointClose idx) = do
            liftIO $ modifyMVar_ mstate $ \case
              s@(TransportValid (ValidTransportState c m)) -> do
                case idx `Map.lookup` m of
                  Nothing -> return s
                  Just lep  -> do
                    old <- modifyMVar (_localEndPointState lep) (\x -> return (LocalEndPointClosed, x))
                    case old of
                      LocalEndPointValid (ValidLocalEndPointState _ _ a) -> Async.cancel a
                      _ -> return ()
                    return $ TransportValid $ ValidTransportState c (Map.delete idx m)
              TransportClosed -> return TransportClosed
            mainloop mstate chan
        go TransportClose = return ()
    shutdown closed mstate = liftIO $ do
      modifyMVar_ mstate $ \case
        TransportValid (ValidTransportState _ m) -> do
          forM_ (Map.elems m) $ \lep -> do
            old <- modifyMVar (_localEndPointState lep) (\x -> return (LocalEndPointClosed, x))
            case old of
              LocalEndPointValid (ValidLocalEndPointState _ _ a) -> Async.cancel a
              _ -> return ()
          return $ TransportClosed
        TransportClosed -> return $ TransportClosed
      liftIO $ putMVar closed ()

apiNewEndPoint :: ZMQTransport -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
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
apiConnect ourEp theirAddr reliability _hints = join $ do
    putStrLn "[user] API connect"
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

endPointCreate :: ZMQParameters -> String -> ZMQ.ZMQ z (Either (TransportError NewEndPointErrorCode) (Int,LocalEndPoint))
endPointCreate params address = do
    em <- try $ accure
    case em of
      Right (port,pull) -> do
          liftIO $ printf "[end-point-create] > Right \n"
          chIn  <- liftIO $ newChan
          chOut <- liftIO $ newTMChanIO
          state <- liftIO $ newMVar (EndPointThreadState (Counter 0 Map.empty) Map.empty)
          let addr = EndPointAddress $ B8.pack (address ++ ":" ++ show port)
          liftIO $ printf "[end-point-create] addr: %s\n" (B8.unpack $ endPointAddressToByteString addr)
          receiverThread <- ZMQ.async $ receiver pull addr state chOut `finally` release port pull
          mainThread     <- ZMQ.async $ go pull addr state chIn `finally` finalizeEndPoint state receiverThread
          mt <- liftIO $ newMVar (LocalEndPointValid $ ValidLocalEndPointState chIn chOut mainThread)
          liftIO $ do
              Async.link receiverThread
              Async.link mainThread
          return $ Right (port, LocalEndPoint addr mt)
      Left (e::SomeException)  -> do
          liftIO $ printf "[end-point-create] > Left \n"
          liftIO $ print e
          return $ Left $ TransportError NewEndPointInsufficientResources "no free sockets"
  where
    receiver pull ourEp mstate chan = forever $ do
      (cmd:msgs) <- ZMQ.receiveMulti pull
      case decode' cmd of
        MessageData idx -> liftIO $ atomically $ writeTMChan chan (Received idx msgs)
        MessageConnect theirAddress -> do
          liftIO $ printf "[%s] message connect from %s\n"
                          (B8.unpack $ endPointAddressToByteString ourEp)
                          (B8.unpack $ endPointAddressToByteString theirAddress)
          void $ createOrGetRemoteEndPoint mstate ourEp theirAddress
        MessageInitConnection theirAddress theirId rel -> do
          liftIO $ printf "[%s] message init connection from %s\n"
                     (B8.unpack $ endPointAddressToByteString ourEp)
                     (B8.unpack $ endPointAddressToByteString theirAddress)
          join $ liftIO $ do
              modifyMVar mstate $ \c@(EndPointThreadState (Counter i m) r) ->
                case theirAddress `Map.lookup` r of
                  Nothing  -> return (c, markRemoteHostFailed mstate theirAddress)
                  Just rep -> withMVar (remoteEndPointState rep) $ \case
                        RemoteEndPointClosed -> undefined
                        RemoteEndPointValid v -> do
                          conn <- ZMQConnection <$> pure rep
                                                <*> pure rel
                                                <*> newMVar (ZMQConnectionValid $ ValidZMQConnection i)
                                                <*> newEmptyMVar
                          register (succ i) rep v
                          return ( EndPointThreadState (Counter (succ i) (Map.insert (succ i) conn m)) r
                                 , return ())
                        RemoteEndPointPending v -> do
                          conn <- ZMQConnection <$> pure rep
                                                <*> pure rel
                                                <*> newMVar (ZMQConnectionValid $ ValidZMQConnection i)
                                                <*> newEmptyMVar
                          modifyIORef v (\xs -> (register (succ i) rep) : xs)
                          return ( EndPointThreadState (Counter (succ i) (Map.insert (succ i) conn m)) r
                                 , return ()
                                 )
          liftIO $ printf "[%s] message init connection           [ok] \n"
                          (B8.unpack $ endPointAddressToByteString ourEp)
          where
            register i rep (ValidRemoteEndPoint ch _) = do
              writeChan ch $ encode' (MessageInitConnectionOk ourEp theirId i) :| []
              atomically $ writeTMChan chan (ConnectionOpened i rel theirAddress)
        MessageCloseConnection idx -> join $ liftIO $
          modifyMVar mstate $ \c@(EndPointThreadState (Counter i m) r) ->
            case idx `Map.lookup` m of
              Nothing  -> return (c, return () {-markRemoteHostFailed mstate theirAddress-})
              Just conn -> do
                old <- modifyMVar (connectionState conn) (\c -> return (ZMQConnectionClosed, c))
                case old of
                  ZMQConnectionClosed -> return (c, return ())
                  ZMQConnectionValid (ValidZMQConnection _) -> do
                      atomically $ writeTMChan chan (ConnectionClosed idx)
                      return (EndPointThreadState (Counter i (idx `Map.delete` m)) r, return ())
        MessageInitConnectionOk theirAddress ourId theirId -> do
          liftIO $ printf "[%s] message init connection ok: %i -> %i\n"
                          (B8.unpack $ endPointAddressToByteString ourEp)
                          ourId
                          theirId
          join $ liftIO $ withMVar mstate $ \c@(EndPointThreadState _ r) ->
            case theirAddress `Map.lookup` r of
              Nothing  -> return (return ()) -- XXX: send message to the host
              Just rep -> modifyMVar (remoteEndPointState rep) $ \case
                RemoteEndPointClosed -> undefined
                t@(RemoteEndPointValid (ValidRemoteEndPoint ch (Counter x m))) -> do
                  case ourId `Map.lookup` m of
                      Nothing -> return (t, return ())     -- XXX: send message to the hostv
                      Just c  -> do
                        return (RemoteEndPointValid (ValidRemoteEndPoint ch (Counter x (ourId `Map.delete` m)))
                               , liftIO $ do 
                                    modifyMVar_ (connectionState c) $ \case
                                      ZMQConnectionInit -> return $ ZMQConnectionValid (ValidZMQConnection theirId)
                                      ZMQConnectionClosed -> do
                                          -- decrement value
                                          writeChan ch $ encode' (MessageCloseConnection theirId) :| []
                                          return ZMQConnectionClosed
                                      ZMQConnectionValid _ -> error "INVARIANT BREAK"
                                    void $ tryPutMVar (connectionReady c) ()
                               )
                RemoteEndPointPending p -> return (RemoteEndPointPending p, undefined)
          liftIO $ printf "[%s] message init connection ok                      [ok]\n"
                          (B8.unpack $ endPointAddressToByteString ourEp)
        MessageEndPointClose theirAddress -> do
          rep <- createOrGetRemoteEndPoint mstate ourEp theirAddress
          remoteEndPointClose rep
    go pull ourAddress mstate chIn = liftIO (readChan chIn) >>= \case
      LocalEndPointConnectionOpen ourEp theirAddress rel reply -> do
        liftIO $ printf "[%s][go] connection open to %s\n"
                        (B8.unpack $ endPointAddressToByteString ourAddress)
                        (B8.unpack $ endPointAddressToByteString theirAddress)
        host <- createOrGetRemoteEndPoint mstate ourAddress theirAddress
        econn <- remoteEndPointOpenConnection ourEp host rel
        liftIO . putMVar reply $
            case econn of
              Left e  -> Left e
              Right c -> Right $ Connection
                  { send  = apiSend c
                  , close = apiClose c
                  }
        go pull ourAddress mstate chIn
      LocalEndPointConnectionClose conn -> do
        oldC <- liftIO $ modifyMVar (connectionState conn) $ \s -> return (ZMQConnectionClosed, s)
        liftIO $ case oldC of
          ZMQConnectionClosed    -> return ()
          ZMQConnectionInit      -> undefined
          ZMQConnectionValid (ValidZMQConnection idx) ->
            withMVar (remoteEndPointState . connectionRemoteEndPoint $ conn) $ \case
              RemoteEndPointClosed  -> return ()  -- XXX: violation
              RemoteEndPointPending p -> return ()  -- XXX: ???
              RemoteEndPointValid (ValidRemoteEndPoint ch _) ->
                  writeChan ch $ encode' (MessageCloseConnection idx) :| []
                  -- TODO: remove connection from counter
        go pull ourAddress mstate chIn
      LocalEndPointClose{}           -> return ()
    finalizeEndPoint mstate receiver = liftIO $ do
      withMVar mstate $ \(EndPointThreadState _ rp) ->
        forM_ (Map.elems rp) $ \(RemoteEndPoint _ x _) -> Async.cancel x
      Async.cancel receiver
    accure = do
      pull <- ZMQ.socket ZMQ.Pull
      case authorizationType params of
          ZMQNoAuth -> return ()
          ZMQAuthPlain p u -> do
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
  RemoteEndPointPending p -> return $ yield >> apiSend c b
  RemoteEndPointValid  (ValidRemoteEndPoint ch _) -> withMVar s $ \case
    ZMQConnectionInit   -> return $ yield >> apiSend c b
    ZMQConnectionClosed -> return $ return $ Left $ TransportError SendClosed "Connection is closed"
    ZMQConnectionValid  (ValidZMQConnection idx) -> do
      writeChan ch $ encode' (MessageData idx):|b
      return $ return $ Right ()

-- 'apiClose' function is asynchronous, as connection may not exists by the
-- time of the calling to this function. In this case function just marks
-- connection as closed, so all subsequent calls from the user side will
-- "think" that the connection is closed, and remote side will be contified
-- only after connection will be up.
apiClose :: ZMQConnection -> IO ()
apiClose c@(ZMQConnection e _ s r) = join $ modifyMVar s $ \case
   ZMQConnectionInit   -> return (ZMQConnectionClosed, return ())
   ZMQConnectionClosed -> return (ZMQConnectionClosed, return ())
   ZMQConnectionValid (ValidZMQConnection idx) -> do
     return (ZMQConnectionClosed, do
       modifyMVar_ (remoteEndPointState e) $ \case
         v@RemoteEndPointClosed -> return v
         v@(RemoteEndPointValid w) -> notify idx w >> return v
         v@(RemoteEndPointPending p) -> modifyIORef p (\xs -> notify idx : xs) >> return v)
  where
    notify idx (ValidRemoteEndPoint ch _) = writeChan ch $ encode' (MessageCloseConnection idx) :| []

-- | Remote end point connection encapsulated into a thread
createOrGetRemoteEndPoint :: MVar EndPointThreadState
                          -> EndPointAddress
                          -> EndPointAddress
                          -> ZMQ.ZMQ z RemoteEndPoint
createOrGetRemoteEndPoint mstate ourEp ep = do
    liftIO $ printf "[%s][create-remote-endpoint] to %s\n"
                    (B8.unpack $ endPointAddressToByteString ourEp)
                    (B8.unpack $ endPointAddressToByteString ep)
    m <- liftIO $ takeMVar mstate
    go m `onException` (liftIO $ putMVar mstate m)
  where
    ident = endPointAddressToByteString ourEp
    saddr = B8.unpack $ endPointAddressToByteString ep
    go m = case ep `Map.lookup` endPointRemotes m of
       Nothing -> do
          state <- liftIO $ newMVar . RemoteEndPointPending =<< newIORef []
          a  <- ZMQ.async $ serve state
          liftIO $ Async.link a
          let rp = RemoteEndPoint ep a state
          liftIO (putMVar mstate $ m{endPointRemotes = Map.insert ep rp (endPointRemotes m)})
          return rp
       Just t -> do
          liftIO $ printf  "[%s][create-remote-end-point] found\n"
                            (B8.unpack $ endPointAddressToByteString ourEp)
          liftIO $ putMVar mstate m
          return t
    serve state = do
        chan  <- liftIO newChan
        bracket 
          (ZMQ.socket ZMQ.Push >>= \p -> ZMQ.connect p saddr >> return p)
          (\p -> ZMQ.disconnect p saddr >> ZMQ.close p) $ \push -> 
            run push state chan `finally` shutdown push state
      where
        run push state chan = initialize >> mainloop
          where
            initialize = do
                liftIO $ printf "[%s][create-remote-endpoint][initialize]\n"
                                (B8.unpack $ endPointAddressToByteString ourEp)
                ZMQ.send push [] $ encode' $ MessageConnect ourEp
                liftIO $ do
                  let v = ValidRemoteEndPoint chan (Counter 0 Map.empty)
                  old <- swapMVar state (RemoteEndPointValid v)
                  case old of
                    RemoteEndPointPending p -> mapM_ (\x -> x v) =<< readIORef p
                    RemoteEndPointValid v -> undefined
                    RemoteEndPointClosed  -> undefined
            mainloop   = forever $ do
                x <- liftIO $ readChan chan
                ZMQ.sendMulti push x
                liftIO yield
        shutdown push state = do
            liftIO $ putStrLn "[create-remote-end-point] shutdown\n"
            void . liftIO $ swapMVar state RemoteEndPointClosed
            ZMQ.send push [] $ encode' $ MessageEndPointClose ourEp

-- | XXX: This function is not asynchronous as possible
remoteEndPointOpenConnection :: LocalEndPoint -> RemoteEndPoint -> Reliability -> ZMQ.ZMQ z (Either (TransportError ConnectErrorCode) ZMQConnection)
remoteEndPointOpenConnection ep x@(RemoteEndPoint addr _ state) rel = join . liftIO $ do
  modifyMVar state $ \case
    RemoteEndPointClosed -> do
      return (RemoteEndPointClosed, return $ Left $ TransportError ConnectFailed "Transport is closed.")
    RemoteEndPointValid (ValidRemoteEndPoint c (Counter i m)) -> do
      conn <- ZMQConnection <$> pure x
                            <*> pure rel
                            <*> newMVar ZMQConnectionInit
                            <*> newEmptyMVar
      let i' = succ i
      return ( RemoteEndPointValid (ValidRemoteEndPoint c (Counter i' (Map.insert i' conn m)))
             , do liftIO $ writeChan c $ encode' (MessageInitConnection epa i' rel) :| []
                  return $ Right conn
             )
    RemoteEndPointPending z -> do
      return (RemoteEndPointPending z, remoteEndPointOpenConnection ep x rel)
  where
    epa = _localEndPointAddress ep

remoteEndPointClose :: RemoteEndPoint -> ZMQ.ZMQ z a
remoteEndPointClose = undefined

markRemoteHostFailed :: a -> EndPointAddress -> ZMQ.ZMQ z ()
markRemoteHostFailed _ _ = return ()

encode' :: Binary a => a  -> ByteString
encode' = B.concat . BL.toChunks . encode

decode' :: Binary a => ByteString -> a
decode' s = decode . BL.fromChunks $ [s]

{-
--------------------------------------------------------------------------------
-- Remote host                                                                --
--------------------------------------------------------------------------------
--
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
      case uri `Map.lookup` m of
        Nothing -> do
          x <- registerRemoteHost vstate uri
          return (Map.insert uri x m, x)
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
                    RemoteEndPointValid $ ValidRemoteEndPoint (cid `Map.delete` m)
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
        (theirEp, m') <- case theirAddr `Map.lookup` m of
           Just theirEp -> return (theirEp, m)
           Nothing -> do
             rep <- RemoteEndPoint <$> pure theirAddr
                                   <*> pure host
                                   <*> newMVar (RemoteEndPointValid (ValidRemoteEndPoint Map.empty))
             return $ (rep, Map.insert theirAddr rep m)
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

withMVarL s m f = do
  putStrLn $ " [ ]   %s [lock]" ++ s
  x <- withMVar m f
  putStrLn $ " [*]   %s [unlock]" ++ s
  return x


modifyMVarL s m f = do
  putStrLn $ " [ ]   %s [lock]" ++ s
  x <- modifyMVar m f
  putStrLn $ " [*]   %s [unlock]" ++ s
  return x

modifyMVarL_ s m f = do
  putStrLn $ " [ ]   %s [lock]" ++ s
  x <- modifyMVar_ m f
  putStrLn $ " [*]   %s [unlock]" ++ s
