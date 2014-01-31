{-# LANGUAGE DeriveGeneric, StandaloneDeriving, OverloadedStrings, DeriveDataTypeable #-}
{-# LANGUAGE LambdaCase #-}
module Network.Transport.ZeroMQ
  ( -- * Main API
    createTransport
  , ZeroMQParameters(..)
  , defaultZeroMQParameters
  -- * Internals
  -- * Design
  ) where

import           Control.Applicative
import           Control.Concurrent
       ( yield
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

import Network.Transport
import qualified System.ZMQ4.Monadic as ZMQ

import           Text.Printf

-- ---------------------------------------------------------------------------
-- Missing instances
-- ---------------------------------------------------------------------------
deriving instance Generic Reliability
deriving instance Typeable Reliability
instance Binary Reliability

-- | Parameters for ZeroMQ connection
data ZeroMQParameters = ZeroMQParameters

defaultZeroMQParameters :: ZeroMQParameters
defaultZeroMQParameters = ZeroMQParameters

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
--                                    TCP for reliable connections
--                                    UDP for unreliable connections
--
-- *NOTE:* in current implementation ONLY TCP is being used
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
-- XXX: current version of zeromq4-haskell do not export disconnect method, so
--      nodes are never really got disconneted (but it will be solved)
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
      { _localEndPoints  :: !(MVar (LocalEndPoints))
      -- ^ List of local Endpoints.
      , _remoteHosts     :: !(MVar (Map TransportAddress RemoteHost))
      -- ^ List of remote hosts we are connected to
      , transportChannel :: !(Chan ZMQAction)
      -- ^ Message for actions (XXX: use closable channel?)
      , _transportPending    :: !(MVar PendingConnections)
      , _transportConnections :: !(MVar IncommingConnections)
      }

data LocalEndPoints = LocalEndPoints
      { _nextEndPoint :: !Word32
      , _endPointsMap :: !(Map Word32 LocalEndPoint)
      }

data IncommingConnections = IncommingConnections
      { _connectionsNextId :: !ConnectionId
      , _connectionsConnections :: !(Map ConnectionId ZMQIncommingConnection)
      }

data PendingConnections = PendingConnections
      { _nextConnection :: !Word32
      , _pendingConnections :: Map Word32 ZMQConnection
      }

-- | End points allocated localy. 
data LocalEndPoint = LocalEndPoint
      { _localEndPointAddress :: !EndPointAddress
      , _localEndPointState :: MVar LocalEndPointState 
      }

data LocalEndPointState
      = LocalEndPointValid !ValidLocalEndPointState
      | LocalEndPointClosed

data ValidLocalEndPointState = ValidLocalEndPointState !(TMChan Event)

data RemoteHost = RemoteHost 
      { _remoteHostUrl   :: !ByteString
      , _remoteHostState :: !(MVar RemoteHostState)
      , _remoteHostReady :: !(MVar ())
      }

data RemoteHostState
        = RemoteHostValid
        | RemoteHostPending
--        | RemoteHostClosed

-- Note Incomming connection is always valid
data ZMQIncommingConnection = ZMQIncommingConnection !Word64 !(TMChan Event)

data ZMQConnection = ZMQConnection
      { connectionHost  :: !RemoteHost
      , connectionState :: !(MVar ZMQConnectionState)
      , connectionReady :: !(MVar ())
      }

data ZMQConnectionState
      = ZMQConnectionInit
      | ZMQConnectionValid !ValidZMQConnection
      | ZMQConnectionClosed

data ValidZMQConnection = ValidZMQConnection !Word64
 
data ZMQMessage 
      = MessageConnect !ByteString
      | MessageConnectOk !ByteString
      | MessageInitConnection !Word32 !EndPointAddress !Reliability !EndPointAddress
      | MessageInitConnectionOk !Word32 !Word64
      | MessageCloseConnection !ConnectionId
      | MessageData !ConnectionId
      deriving (Generic)

instance Binary ZMQMessage

data ZMQAction
        = ActionMessage !ByteString !ConnectionId ![ByteString]
        | ActionConnectHost !ByteString
        | ActionCloseEP !ByteString !EndPointAddress
        | ActionConnect !ByteString !ZMQConnection !LocalEndPoint !Reliability !EndPointAddress
        | ActionCloseConnection !ByteString !ConnectionId

createTransport :: ZeroMQParameters -- ^ Transport features.
                -> ByteString       -- ^ Host.
                -> ByteString       -- ^ Port.
                -> IO (Either (TransportError Void) Transport)
createTransport _params host port = do
    vstate <- ValidTransportState
                 <$> newMVar (LocalEndPoints 0 M.empty)
                 <*> newMVar (M.empty)
                 <*> newChan
                 <*> newMVar (PendingConnections 0 M.empty)
                 <*> newMVar (IncommingConnections 0 M.empty)
    transport <- ZeroMQTransport 
                    <$> pure addr
                    <*> newMVar (TransportValid vstate)
    try $ do
      needContinue <- newIORef True
      a <- ZMQ.runZMQ $ ZMQ.async $ do
        -- Initialization.
        router <- ZMQ.socket ZMQ.Router
        ZMQ.setIdentity (ZMQ.restrict socketAddr) router
        ZMQ.bind router (B8.unpack addr)

        -- Start worker threads.
        mon   <- ZMQ.async $ processMonitor router
        liftIO $ A.link mon
        queue <- ZMQ.async $ processQueue transport router 
        liftIO $ A.link queue
        mainloop vstate router needContinue

        -- TODO: Close all endpoints
        ZMQ.unbind router (B8.unpack addr)
        liftIO $ do
          modifyMVar_ (_transportState transport) $ \_ -> return TransportClosed
          mapM_ (A.cancel) [mon, queue]

      A.link a
      return $ Transport
          { newEndPoint    = apiNewEndPoint transport
          , closeTransport = writeIORef needContinue False
          } 
  where
    addr = B.concat ["tcp://",host, ":",port]
    socketAddr = addr
    repeatWhile :: MonadIO m => m Bool -> m () -> m ()
    repeatWhile f g = f >>= flip when (g >> repeatWhile f g)
    processMonitor router = do
      evalMonitor <- ZMQ.monitor [ZMQ.AllEvents] router
      forever $ do
        ev <- liftIO $ evalMonitor True
        case ev of
          Just (ZMQ.Connected h _) -> do
            liftIO $ printf "[%s]: [monitor] connected %s\n" (B8.unpack socketAddr) (B8.unpack h)
            -- We are only replying with our address to the client, as we
            -- can't disconnect him.
            sendControlMessage router h (MessageConnect socketAddr)
          _ -> return ()
    processQueue transport router = do
      (TransportValid (ValidTransportState _ _ chan ps _)) <- liftIO $ readMVar (_transportState transport)
      forever $ do
        action <- liftIO $ readChan chan
        case action of
          ActionMessage ident ix message -> do
              liftIO $ dbg' "<ActionMessage>"
              sendMessage router ident ix message
          ActionCloseEP _ident _addr -> do
              liftIO $ dbg' "<ActionCloseEP>"
              -- Notify the other side about the fact that connection is
              -- closed.
              undefined
          ActionConnect ident conn ourEp rel addr' -> do
              liftIO $ dbg' "<ActionConnect>"
              idx <- liftIO $ modifyMVar ps $ \(PendingConnections n m) ->
                let n' = succ n
                    m'  = M.insert n' conn m
                in return (PendingConnections n m', n')
              sendControlMessage router ident (MessageInitConnection idx (_localEndPointAddress ourEp) rel addr')
          ActionConnectHost ident -> do
              liftIO $ printf "[%s]: ActionConnectHost\n" (B8.unpack socketAddr)
              ZMQ.connect router (B8.unpack ident)
          ActionCloseConnection ident cid -> do
              liftIO $ dbg' "<ActionCloseConnection>"
              sendControlMessage router ident (MessageCloseConnection cid)
    mainloop vstate router needContinue =
        repeatWhile (ZMQ.liftIO $ readIORef needContinue) $ do
          events <- ZMQ.poll (-1) [ZMQ.Sock router [ZMQ.In] Nothing]
          case events of
            [] -> return ()
            _  -> do
              liftIO $ printf "[%s]: got message\n" (B8.unpack socketAddr)
              identity <- ZMQ.receive router
              (cmd:msgs) <- ZMQ.receiveMulti router
              case decode' cmd of
                MessageConnect ident -> do
                  liftIO $ printf "[%s]: message connect\n" (B8.unpack socketAddr)
                  sendControlMessage router ident (MessageConnectOk socketAddr)
                MessageConnectOk ident -> do
                  liftIO $ printf "[%s]: message connect ok\n" (B8.unpack socketAddr)
                  liftIO $ withMVar remoteHosts $ \h -> do
                    case ident `M.lookup` h of
                      Nothing -> do
                        liftIO $ printf "[%s][ERROR]: remote host is not in list: (%s) \n" (B8.unpack socketAddr) (B8.unpack ident)
                      Just x  -> do
                        _ <- swapMVar (_remoteHostState x) RemoteHostValid
                        putMVar (_remoteHostReady x) ()
                MessageInitConnection theirId theirEp rel ep -> do
                  liftIO $ printf "[%s]: message init connection\n" (B8.unpack socketAddr)
                  let epId = apiGetEndPointId ep
                  liftIO $ printf "[%s]: endpoint id is: %i \n" (B8.unpack socketAddr) (epId)
                  ret <- liftIO $ withMVar (_localEndPoints vstate) $ \(LocalEndPoints _ eps) ->
                     case epId `M.lookup` eps of
                      Nothing -> do
                          liftIO $ printf "[%s]: no such endpoint\n" (B8.unpack socketAddr)
                          return Nothing -- XXX: reply with error message
                      Just x  -> modifyMVar (_localEndPointState x) $ \s -> do
                          case s of
                            LocalEndPointValid (ValidLocalEndPointState chan)  -> do
                              idx <- modifyMVar (_transportConnections vstate) $ \(IncommingConnections n m) -> do
                                  let n' = succ n
                                      c  = ZMQIncommingConnection n' chan
                                  return (IncommingConnections n' (M.insert n' c m), n')
                              atomically $ writeTMChan chan $ ConnectionOpened idx rel theirEp
                              return (s, Just idx)
                            LocalEndPointClosed  -> return (s, Nothing) -- XXX: reply with error message
                  case ret of
                    Nothing -> return () -- XXX: reply with error
                    Just ourId -> sendControlMessage router identity (MessageInitConnectionOk theirId ourId)
                MessageCloseConnection idx -> liftIO $ 
                  modifyMVar_ (_transportConnections vstate) $ \i@(IncommingConnections n m) -> do
                    case idx `M.lookup` m of
                      Nothing -> return i -- XXX: errror no such connection
                      Just (ZMQIncommingConnection _ ch) -> do
                         atomically $ writeTMChan ch $ ConnectionClosed idx
                         return $ (IncommingConnections n (M.delete idx m)) -- Note incomming connections may be only valid (it seems not correct)
                MessageInitConnectionOk ourId theirId -> liftIO $ do
                  liftIO $ printf "[%s]: message init connection ok\n" (B8.unpack socketAddr)
                  modifyMVar_ (_transportPending vstate) $ \pconns ->
                    case ourId `M.lookup` (_pendingConnections pconns) of
                        Nothing -> do
                            liftIO $ printf "[%s]: no such connection\n" (B8.unpack socketAddr)
                            return pconns 
                        Just cn -> do
                            liftIO $ printf "[%s]: found pending\n" (B8.unpack socketAddr)
                            rd <- modifyMVar (connectionState cn) $ \st ->
                              case st of
                                ZMQConnectionInit -> do
                                    liftIO $ printf "[%s]: correct state\n" (B8.unpack socketAddr)
                                    return (ZMQConnectionValid (ValidZMQConnection theirId), True)
                                _ -> return (st, False)
                            if rd
                            then do putMVar (connectionReady cn) ()
                                    return (pconns{_pendingConnections=ourId `M.delete` (_pendingConnections pconns)})
                            else return pconns
                MessageData idx -> liftIO $
                  withMVar (_transportConnections vstate) $ \(IncommingConnections _ x) ->
                    case idx `M.lookup` x of
                      Nothing -> undefined
                      Just (ZMQIncommingConnection _ ch) -> atomically $ writeTMChan ch (Received idx msgs)
          liftIO $ yield
      where
        remoteHosts = _remoteHosts vstate


sendControlMessage router ident msg = ZMQ.sendMulti router $ ident :| [encode' msg, ""]

sendMessage router ident cn msg = ZMQ.sendMulti router $ ident :| (encode' (MessageData cn):msg)


apiNewEndPoint :: ZeroMQTransport -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint transport = do
    chan <- newTMChanIO
    ep <- withMVar (_transportState transport) $ \st ->
      case st of
        TransportClosed ->
           throwIO $ userError "Transport is closed"  --- XXX: should we return left with error here?
        TransportValid (ValidTransportState leps _ _ _ _) ->
           modifyMVar leps $ \(LocalEndPoints nxt eps) ->
             let nxt' = succ nxt
                 addr = EndPointAddress $
                          B.concat 
                            [ transportAddress transport
                            , "/"
                            , B8.pack $ show nxt' ]
             in do
                ep <- LocalEndPoint 
                        <$> pure addr
                        <*> newMVar (LocalEndPointValid (ValidLocalEndPointState chan))
                let eps' = M.insert nxt' ep eps
                return (LocalEndPoints nxt' eps', ep)
    return . Right $ EndPoint
      { receive = fromMaybe EndPointClosed <$> atomically (readTMChan chan)
      , address = _localEndPointAddress ep
      , connect = apiConnect ep transport
      , closeEndPoint         = do
          modifyMVar_ (_localEndPointState ep) (return . const LocalEndPointClosed)
          atomically $ closeTMChan chan
      , newMulticastGroup     = return . Left $
            TransportError NewMulticastGroupUnsupported "Multicast not supported"
      , resolveMulticastGroup = return . return . Left $ 
            TransportError ResolveMulticastGroupUnsupported "Multicast not supported"
      }

apiConnect :: LocalEndPoint
           -> ZeroMQTransport
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect ourEp transport theirEp reliability _hints = do
        let uri = apiGetUri theirEp reliability -- XXX: add reliabitily support
        if uri == transportAddress transport
        then mkLocalConnection
        else do
            -- printf "[%s] connecting to <%s>\n" (B8.unpack . endPointAddressToByteString $ ourEp) (B8.unpack uri)
            host <- withMVar (_transportState transport) $ \state ->
              case state of
                TransportClosed  -> do
                    error "transport is closed" -- TODO: return Left
                TransportValid v -> do
                  dbg' "transport is valid."
                  modifyMVar (_remoteHosts v) $ \m -> do
                    case uri `M.lookup` m of
                      Nothing -> do
                        x <- RemoteHost <$> pure uri
                                        <*> newMVar RemoteHostPending
                                        <*> newEmptyMVar
                        writeChan (transportChannel v)
                                  (ActionConnectHost uri)
                        return (M.insert uri x m, x)
                      Just x  -> return (m, x)
            -- printf "[%s] waiting for connection to remote\n" (B8.unpack . endPointAddressToByteString $ ourEp)
            _    <- readMVar (_remoteHostReady host)
            -- printf "[%s] creating connection\n" (B8.unpack . endPointAddressToByteString $ ourEp)
            conn <- ZMQConnection <$> pure host
                                  <*> newMVar ZMQConnectionInit
                                  <*> newEmptyMVar
            withMVar (_transportState transport) $ \state ->
              case state of
                TransportClosed -> error "transport is closed" --return $ Left $ TransportError ConnectFailed "Transport is closed" 
                TransportValid v -> do
                  writeChan (transportChannel v)
                            (ActionConnect (_remoteHostUrl host) conn ourEp reliability theirEp)
            _    <- readMVar (connectionReady conn)
            return . Right $ Connection
              { send = apiSend transport conn
              , close = apiCloseConnection transport conn
              }
  where
    eid = apiGetEndPointId theirEp
    oid = apiGetEndPointId $ _localEndPointAddress ourEp
    mkLocalConnection = do
        withMVar (_transportState transport) $ \case
          TransportClosed -> return $ Left $ TransportError ConnectFailed "Transport is closed."
          TransportValid v -> do
            withMVar (_localEndPoints v) $ \(LocalEndPoints _ eps) -> do
              case eid `M.lookup` eps of
                Nothing -> return $ Left $ TransportError ConnectFailed "Endpoint not found."
                Just  e -> do
                  withMVar (_localEndPointState ourEp) $ \case
                    LocalEndPointClosed -> return $ Left $ TransportError ConnectFailed "Iur endpoint is closed."
                    LocalEndPointValid ourV ->
                      let action = 
                            if _localEndPointAddress ourEp == theirEp
                            then (\f -> f ourV)
                            else (\f -> withMVar (_localEndPointState e) $ \case
                                          LocalEndPointClosed -> return $ Left $ TransportError ConnectFailed "Their endpoint is closed."
                                          LocalEndPointValid theirV -> f theirV)
                      in action $ \(ValidLocalEndPointState ch) -> do
                           idx <- modifyMVar (_transportConnections v) $ \(IncommingConnections n m) -> do
                                    let n' = succ n
                                        c  = ZMQIncommingConnection n' ch
                                    return (IncommingConnections n' (M.insert n' c m), n')
                           atomically $ writeTMChan ch $ ConnectionOpened idx reliability (_localEndPointAddress ourEp)
                           return . Right $ Connection
                             { send = \bs -> do
                                 withMVar (_transportConnections v) $ \(IncommingConnections n m) -> do
                                   withMVar (_localEndPointState ourEp) $ \case
                                     LocalEndPointValid _ -> 
                                       case idx `M.lookup` m of
                                         Nothing -> return $ Left $ TransportError SendClosed "Connection is closed."
                                         Just _ ->
                                           atomically $ do
                                             closed <- isClosedTMChan ch
                                             if closed
                                             then return $ Left $ TransportError SendFailed "Connection is closed." 
                                             else writeTMChan ch (Received idx bs) >> return (Right ())
                                     LocalEndPointClosed -> return $ Left $ TransportError SendFailed "Our endpoint is closed."
                             , close =
                                 modifyMVar_ (_transportConnections v) $ \(IncommingConnections n m) -> do
                                   case idx `M.lookup` m of
                                     Nothing -> return () -- already closed
                                     Just (ZMQIncommingConnection _ ch)  ->
                                       atomically $ writeTMChan ch $ ConnectionClosed idx
                                   return $ (IncommingConnections n (M.delete idx m)) -- Note incomming connections may be only valid (it seems not correct)
                             }

apiSend :: ZeroMQTransport -> ZMQConnection -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
apiSend transport connection bs = do
    withMVar (connectionState connection) $ \state ->
      case state of
        ZMQConnectionValid (ValidZMQConnection cid) -> 
          withMVar (_transportState transport) $ \state' ->
            case state' of
              TransportClosed  -> return $ Left $ TransportError SendClosed "Transport is closed."
              TransportValid v -> do
                writeChan (transportChannel v)
                          (ActionMessage hid cid bs)
                return $ Right ()
        ZMQConnectionInit   -> return $ Left $ TransportError SendFailed "Connection is in initialization phase."
        ZMQConnectionClosed -> return $ Left $ TransportError SendClosed "Connection is closed."
  where
    hid = _remoteHostUrl $ connectionHost connection

apiCloseConnection :: ZeroMQTransport -> ZMQConnection -> IO ()
apiCloseConnection transport connection = do
    withMVar (connectionState connection) $ \state ->
      case state of 
        ZMQConnectionValid (ValidZMQConnection cid) ->
          withMVar (_transportState transport) $ \state' ->
            case state' of
              TransportClosed -> return ()
              TransportValid v -> do
                writeChan (transportChannel v)
                          (ActionCloseConnection hid cid)
        _ -> return () -- FIXME
  where
    hid = _remoteHostUrl $ connectionHost connection

apiGetUri :: EndPointAddress -> Reliability -> ByteString
apiGetUri addr _rel = B8.init $ fst $ B8.breakEnd (=='/') $ endPointAddressToByteString addr -- XXX: properly support reliability

apiGetEndPointId :: EndPointAddress -> Word32
apiGetEndPointId epa = read . B8.unpack . snd $ B8.breakEnd (=='/') $ endPointAddressToByteString epa

encode' :: Binary a => a  -> ByteString
encode' = B.concat . BL.toChunks . encode

decode' :: Binary a => ByteString -> a
decode' s = decode . BL.fromChunks $ [s]

dbg' = liftIO . putStrLn
