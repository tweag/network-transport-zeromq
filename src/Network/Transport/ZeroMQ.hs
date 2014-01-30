{-# LANGUAGE DeriveGeneric, StandaloneDeriving, OverloadedStrings #-}
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
      , IOException
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
import           Data.Word
import           GHC.Generics 

import Network.Transport
import qualified System.ZMQ4.Monadic as ZMQ

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

-- XXX: can we reopen transport?
-- XXX: we may want to introduce a new level of indirection: socket -> endpoint
-- XXX: when incrementing endpoint we need to check that we have no node
-- with that address.

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

type HostId = ByteString

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
      { _localEndPoints  :: !(Map EndPointAddress LocalEndPoint)
      -- ^ List of local Endpoints.
      , _nextEndPoint    :: !Word32
      -- ^ Endpoint counter.
      , _remoteHosts     :: !(MVar (Map TransportAddress RemoteHost))
      -- ^ List of remote hosts we are connected to
      , transportChannel :: !(Chan ZMQAction)
      -- ^ Message for actions (XXX: use closable channel?)
      , _transportPendingConnections :: !(MVar PendingConnections)
      }

data PendingConnections = PendingConnections
      { _nextConnection :: !Word32
      , _pendingConnections :: Map Word32 Connection
      }

-- | End points allocated localy. 
data LocalEndPoint
      = LocalEndPointValid !ValidLocalEndPointState
      | LocalEndPointClosed

data ValidLocalEndPointState = ValidLocalEndPointState
      { _localNextConnOutId :: !Word32
      , _nextConnInId       :: !Word32
      , _localConnections   :: !(Map EndPointAddress ZMQConnection)
      }

data RemoteHost = RemoteHost 
      { _remoteHostUrl   :: ByteString
      , _remoteHostState :: RemoteHostState
      , _remoteHostReady :: MVar ()
      }

data RemoteHostState
        = RemoteHostValid
        | RemoteHostPending
        | RemoteHostClosed

data ZMQConnection = ZMQConnection
      { connectionId      :: !Word64
      , connectionHost    :: RemoteHost
      , connectionState   :: ZMQConnectionState
      , _connectionReady   :: MVar ()
      }

data ZMQConnectionState
      = ZMQConnectionValid
      | ZMQConnectionClosed
 
data ZMQMessage 
      = MessageConnect !ByteString
      | MessageOk
      | MessageInitConnection !Word64 !Reliability !EndPointAddress
      | MessageInitConnectionOk !Word64 !Word64
      | MessageCloseConnection !EndPointAddress
      | MessageData !ConnectionId
      deriving (Generic)

instance Binary ZMQMessage

data ZMQAction
        = ActionMessage !ByteString !ConnectionId ![ByteString]
        | ActionCloseEP !ByteString !EndPointAddress
        | ActionConnect !ByteString !Reliability !EndPointAddress
        | ActionCloseConnection !ByteString !ConnectionId

createTransport :: ZeroMQParameters -- ^ Transport features.
                -> ByteString       -- ^ Host.
                -> ByteString       -- ^ Port.
                -> IO (Either (TransportError Void) Transport)
createTransport _params host port = do
    state     <- TransportValid <$>
                    (ValidTransportState <$> 
                           pure M.empty
                       <*> pure 0
                       <*> newMVar (M.empty)
                       <*> newChan
                       <*> newMVar (PendingConnections 0 M.empty)
                       )
    transport <- ZeroMQTransport 
                    <$> pure addr
                    <*> newMVar state
    try $ do
      needContinue <- newIORef True
      void $ ZMQ.runZMQ $ ZMQ.async $ do
        -- Initialization.
        router <- ZMQ.socket ZMQ.Router
        ZMQ.setIdentity (ZMQ.restrict socketAddr) router
        ZMQ.bind router (B8.unpack addr)

        -- Start worker threads.
        mon   <- ZMQ.async $ processMonitor router
        queue <- ZMQ.async $ processQueue transport router 
        mainloop transport router needContinue

        -- TODO: Close all endpoints
        ZMQ.unbind router (B8.unpack addr)
        liftIO $ do
          modifyMVar_ (_transportState transport) $ \_ -> return TransportClosed
          A.cancel queue
          A.cancel mon

      return $ Transport
          { newEndPoint    = apiNewEndPoint transport
          , closeTransport = writeIORef needContinue False
          } 
  where
    addr = B.concat [host, ":",port]
    socketAddr = B.concat ["tcp://", addr]
    repeatWhile :: MonadIO m => m Bool -> m () -> m ()
    repeatWhile f g = f >>= flip when (g >> repeatWhile f g)
    processMonitor router = do
      evalMonitor <- ZMQ.monitor [ZMQ.AllEvents] router
      forever $ do
        ev <- liftIO $ evalMonitor True
        case ev of
          Just (ZMQ.Connected h _) -> 
            -- We are only replying with our address to the client, as we
            -- can't disconnect him.
            ZMQ.sendMulti router $ h :| [encode' (MessageConnect socketAddr),""]
          _ -> return ()
    processQueue transport router = do
      (TransportValid (ValidTransportState _ _ _ chan _)) <- liftIO $ readMVar (_transportState transport)
      forever $ do
        action <- liftIO $ readChan chan
        case action of
          ActionMessage ident ix message -> do
              ZMQ.sendMulti router $ ident :| (encode' $ MessageData ix):message
          ActionCloseEP ident addr    -> do
              -- Notify the other side about the fact that connection is
              -- closed.
              ZMQ.sendMulti router $ ident :| [encode' $ MessageCloseConnection addr, ""]
          ActionConnect ident rel addr    -> do
              msg <- registerPendingConnection transport rel addr
              ZMQ.sendMulti router $ ident :| [encode' msg, ""]
    mainloop transport router needContinue = do
      repeatWhile (ZMQ.liftIO $ readIORef needContinue) $ do
        events <- ZMQ.poll 0 [ZMQ.Sock router [ZMQ.In] Nothing]
        case events of
          [] -> return ()
          _  -> do
             identity <- ZMQ.receive router
             (cmd:msgs) <- ZMQ.receiveMulti router
             case decode' cmd of
               MessageConnect ident ->
                 -- 1. Change RemoteHost state to connected
                 -- 2. Put ready MVar variable
                 undefined
               MessageInitConnection theirId rel ep ->
                 -- 1. Check if we have endpoint
                 -- 2. Create new connection identifier
                 -- 3. Add identifier to endpoint connections (?)
                 -- 4. reply with new connection ID
                 undefined
               MessageCloseConnection idx ->
                 -- 1. mark connection state as closed.
                 -- 2. remove connection from endpoint connections (?)
                 undefined
               MessageInitConnectionOk ourId theirId ->
                 -- 1. find pending connection
                 -- 2. mark connection as valid
                 -- 3. Put ready MVar Variable
                 undefined
               MessageData idx -> do
                 -- 1. find channel by connection id
                 -- 2. put data into channel
                 undefined
        liftIO $ yield
    registerPendingConnection :: Monad m => ZeroMQTransport -> Reliability -> EndPointAddress -> m ZMQMessage
    registerPendingConnection = undefined

apiNewEndPoint :: ZeroMQTransport -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint transport = do
    chan <- newTMChanIO
    addr <- modifyMVar (_transportState transport) $ \st ->
      case st of
        TransportClosed ->
           throwIO $ userError "Transport is closed"  --- XXX: should we return left with error here?
        TransportValid v@(ValidTransportState eps nxt _ _ _) ->
           let nxt' = succ nxt
               addr = EndPointAddress $
                        (transportAddress transport) `B.append` (B8.pack $ show nxt')
               eps' = M.insert addr (LocalEndPointValid (ValidLocalEndPointState 0 0 M.empty)) eps
           in return (TransportValid v{_localEndPoints=eps'
                                      ,_nextEndPoint=nxt'}, addr)
    return . Right $ EndPoint
      { receive = fromMaybe EndPointClosed <$> atomically (readTMChan chan)
      , address = addr
      , connect = apiConnect addr transport
      , closeEndPoint         = atomically $ closeTMChan chan
      , newMulticastGroup     = return . Left $
            TransportError NewMulticastGroupUnsupported "Multicast not supported"
      , resolveMulticastGroup = return . return . Left $ 
            TransportError ResolveMulticastGroupUnsupported "Multicast not supported"
      }

apiConnect :: EndPointAddress
           -> ZeroMQTransport
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect ourEp transport theirEp reliability _hints = do
    let uri = apiGetUri theirEp reliability -- XXX: add reliabitily support
    host <- withMVar (_transportState transport) $ \state ->
      case state of
        TransportClosed  -> error "transport is closed" -- TODO: return Left
        TransportValid v -> do
          modifyMVar (_remoteHosts v) $ \m -> do
            case uri `M.lookup` m of
              Nothing -> do
                x <- initConnection uri
                return (M.insert uri x m, x)
              Just x  -> return (m, x)
    _    <- readMVar (_remoteHostReady host)
    conn <- createConnection transport host theirEp reliability
    withMVar (_transportState transport) $ \state ->
      case state of
        TransportClosed -> error "transport is closed" -- TODO: returl left
        TransportValid v -> 
          writeChan (transportChannel v)
                    (ActionConnect (_remoteHostUrl host) reliability theirEp)
    _    <- readMVar (_connectionReady conn)
    return . Right $ Connection
      { send = apiSend transport conn
      , close = apiCloseConnection transport conn
      }
  where
    initConnection = undefined
    createConnection = undefined

apiSend :: ZeroMQTransport -> ZMQConnection -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
apiSend transport connection bs = do
    withMVar (_transportState transport) $ \state ->
      case state of
        TransportClosed  -> error "transport is closed" -- TODO: return left
        TransportValid v -> do
          writeChan (transportChannel v)
                    (ActionMessage hid cid bs)
          return $ Right ()
  where
    cid = connectionId connection
    hid = _remoteHostUrl (connectionHost connection)

apiCloseConnection :: ZeroMQTransport -> ZMQConnection -> IO ()
apiCloseConnection transport connection = do
    withMVar (_transportState transport) $ \state ->
      case state of
        TransportClosed -> return ()
        TransportValid v -> do
          writeChan (transportChannel v)
                    (ActionCloseConnection hid cid)
  where
    cid = connectionId connection
    hid = _remoteHostUrl (connectionHost connection)

apiGetUri :: EndPointAddress -> Reliability -> ByteString
apiGetUri _addr _rel = undefined -- error "apiGetUri"

encode' :: Binary a => a  -> ByteString
encode' = B.concat . BL.toChunks . encode

decode' :: Binary a => ByteString -> a
decode' = undefined
