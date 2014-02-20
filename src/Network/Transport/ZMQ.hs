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
       )
import qualified Control.Concurrent.Async as Async
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TMChan
import           Control.Monad
      ( void
      , forever
      , join
      , forM_
      , foldM
      )
import           Control.Monad.Catch
      (  finally
      , try
      , throwM
      , Exception
      , SomeException
      )

import           Data.Binary
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
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Typeable
import           Data.Void
import           GHC.Generics 

import Network.Transport
import Network.Transport.ZMQ.Types
import           System.ZMQ4
      ( Context )
import qualified System.ZMQ4 as ZMQ
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
      { _transportContext   :: !ZMQ.Context
      , _transportEndPoints :: !(Map Int LocalEndPoint)
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

data ZMQError = InvariantBreak
              | IncorrectState String
              deriving (Typeable, Show)

instance Exception ZMQError

createTransport :: ZMQParameters    -- ^ Transport features.
                -> ByteString       -- ^ Host.
                -> IO (Either (TransportError Void) Transport)
createTransport params host = do
    -- FIXME: catch exceptions return error
    ctx       <- ZMQ.context
    transport <- ZMQTransport 
    	<$> pure addr 
        <*> newMVar (TransportValid $ ValidTransportState ctx Map.empty)
    return $ Right $ Transport
      { newEndPoint    = apiNewEndPoint params transport
      , closeTransport = apiTransportClose transport
      }
  where
    addr = B.concat ["tcp://",host]


apiTransportClose :: ZMQTransport
		  -> IO ()
apiTransportClose transport = do
    old <- swapMVar (_transportState transport) TransportClosed
    case old of
      TransportClosed -> return ()
      TransportValid (ValidTransportState ctx m) -> do
        forM_ (Map.elems m) $ apiCloseEndPoint transport
	ZMQ.term ctx

apiNewEndPoint :: ZMQParameters -> ZMQTransport -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint params transport = do
    printf "[transport] endpoint create\n"
    elep <- modifyMVar (_transportState transport) $ \case
       TransportClosed -> return (TransportClosed, Left $ TransportError NewEndPointFailed "Transport is closed.")
       v@(TransportValid i@(ValidTransportState ctx m)) -> do
         eEndPoint <- endPointCreate params ctx (B8.unpack addr)
         case eEndPoint of
           Right (port,ep) -> return 
	   	  ( TransportValid i{_transportEndPoints = Map.insert port ep (_transportEndPoints i)}
                  , Right (ep,ctx))
           Left e -> return (v, Left $ TransportError NewEndPointFailed "Failed to create new endpoint.")
    case elep of
      Right (ep,ctx) ->
        withMVar (_localEndPointState ep) $ \case
          LocalEndPointValid (ValidLocalEndPointState chOut _ _ _) ->
            return $ Right $ EndPoint
              { receive = atomically $ do
                  mx <- readTMChan chOut
                  case mx of
                    Nothing -> error "channel is closed"
                    Just x  -> return x
              , address = _localEndPointAddress ep
              , connect = apiConnect ctx ep
              , closeEndPoint = apiCloseEndPoint transport ep
              , newMulticastGroup     = return . Left $
                   TransportError NewMulticastGroupUnsupported "Multicast not supported"
              , resolveMulticastGroup = return . return . Left $
                   TransportError ResolveMulticastGroupUnsupported "Multicast not supported"
              }
          _ -> return $ Left $ TransportError NewEndPointFailed "Endpoint is closed."
  where
    addr = transportAddress transport

-- | Asynchronous operation, shutdown of the remote end point may take a while
apiCloseEndPoint :: ZMQTransport
                 -> LocalEndPoint
		 -> IO ()
apiCloseEndPoint transport lep = do
    printf "[%s][go] close endpoint\n"
           (B8.unpack $ endPointAddressToByteString $ _localEndPointAddress lep)
    withMVar (_localEndPointState lep) $ \case
       LocalEndPointValid (ValidLocalEndPointState _ _ _ threadId) -> Async.cancel threadId
       LocalEndPointClosed -> return ()
    modifyMVar_ (_transportState transport) $ \case
      TransportClosed  -> return TransportClosed 
      TransportValid v -> return $ TransportValid
        v{_transportEndPoints = Map.delete port (_transportEndPoints v)}
  where
    port = localEndPointPort lep

endPointCreate :: ZMQParameters
               -> Context
               -> String
               -> IO (Either (TransportError NewEndPointErrorCode) (Int,LocalEndPoint))
endPointCreate params ctx address = do
    em <- try $ accure
    case em of
      Right (port,pull) -> do
          chOut <- newTMChanIO
          let addr = EndPointAddress $ B8.pack (address ++ ":" ++ show port)
          lep   <- LocalEndPoint <$> pure addr
	  	   		 <*> newEmptyMVar
				 <*> pure port
          printf "[end-point-create] addr: %s\n" (B8.unpack $ endPointAddressToByteString addr)
          thread <- Async.async $ receiver pull lep chOut `finally` finalizeEndPoint lep port pull
          putMVar (_localEndPointState lep) $ LocalEndPointValid 
            (ValidLocalEndPointState chOut (Counter 0 Map.empty) Map.empty thread)
          Async.link thread
          return $ Right (port, lep)
      Left (e::SomeException)  -> do
          return $ Left $ TransportError NewEndPointInsufficientResources "no free sockets"
  where
    receiver :: ZMQ.Socket ZMQ.Pull
             -> LocalEndPoint
             -> TMChan Event
	     -> IO ()
    receiver pull ourEp chan = forever $ do
      (cmd:msgs) <- ZMQ.receiveMulti pull
      case decode' cmd of
        MessageData idx -> atomically $ writeTMChan chan (Received idx msgs)
        MessageConnect theirAddress -> do
          printf "[%s] message connect from %s\n"
                 (B8.unpack $ endPointAddressToByteString ourAddr)
                 (B8.unpack $ endPointAddressToByteString theirAddress)
          void $ createOrGetRemoteEndPoint ctx ourEp theirAddress
        MessageInitConnection theirAddress theirId rel -> do
          printf "[%s] message init connection from %s\n"
                     (B8.unpack $ endPointAddressToByteString ourAddr)
                     (B8.unpack $ endPointAddressToByteString theirAddress)
          join $ do
            modifyMVar (_localEndPointState ourEp) $ \case
                LocalEndPointValid v ->
                    case theirAddress `Map.lookup` r of
                      Nothing -> return (LocalEndPointValid v, throwM InvariantBreak)
                      Just rep -> withMVar (remoteEndPointState rep) $ \case
                          RemoteEndPointClosed -> throwM $ InvariantBreak
                          RemoteEndPointValid w -> do
                            conn <- ZMQConnection <$> pure rep
                                                  <*> pure rel
                                                  <*> newMVar (ZMQConnectionValid $ ValidZMQConnection i)
                                                  <*> newEmptyMVar
                            register (succ i) (RemoteEndPointValid w)
                            return ( LocalEndPointValid v{ endPointConnections = Counter (succ i) (Map.insert (succ i) conn m) }
                                   , return ())
                          RemoteEndPointPending w -> do
                            conn <- ZMQConnection <$> pure rep
                                                  <*> pure rel
                                                  <*> newMVar (ZMQConnectionValid $ ValidZMQConnection i)
                                                  <*> newEmptyMVar
                            modifyIORef w (\xs -> (register (succ i))  : xs)
                            return ( LocalEndPointValid v{ endPointConnections = Counter (succ i) (Map.insert (succ i) conn m) }
                                   , return ()
                                   )
                  where
                    r = endPointRemotes v
                    (Counter i m) = endPointConnections v
                _ -> throwM InvariantBreak
          printf "[%s] message init connection           [ok] \n"
                          (B8.unpack $ endPointAddressToByteString ourAddr)
          where
            register _ RemoteEndPointClosed = return RemoteEndPointClosed
            register _ RemoteEndPointPending{} = throwM InvariantBreak
            register i v@(RemoteEndPointValid (ValidRemoteEndPoint sock _)) = do
              ZMQ.send sock [] $ encode' (MessageInitConnectionOk ourAddr theirId i)
              atomically $ writeTMChan chan (ConnectionOpened i rel theirAddress)
              return v 
        MessageCloseConnection idx ->
          modifyMVar_ (_localEndPointState ourEp) $ \case
            LocalEndPointValid v ->
                case idx `Map.lookup` m of
                  Nothing  -> return $ LocalEndPointValid v
                  Just conn -> do
                    old <- modifyMVar (connectionState conn) (\c -> return (ZMQConnectionClosed, c))
                    case old of
		      ZMQConnectionInit -> return $ LocalEndPointValid v -- throwM InvariantViolation
                      ZMQConnectionClosed -> return $ LocalEndPointValid v
                      ZMQConnectionValid (ValidZMQConnection _) -> do
                          atomically $ writeTMChan chan (ConnectionClosed idx)
                          return $ LocalEndPointValid v{ endPointConnections = Counter i (idx `Map.delete` m)}
              where 
                r = endPointRemotes v
                (Counter i m) = endPointConnections v
	    LocalEndPointClosed -> return LocalEndPointClosed
        MessageInitConnectionOk theirAddress ourId theirId -> do
          printf "[%s] message init connection ok: %i -> %i\n"
                 (B8.unpack $ endPointAddressToByteString ourAddr)
                 ourId
                 theirId
          join $ withMVar (_localEndPointState ourEp) $ \case
            LocalEndPointValid v -> 
                case theirAddress `Map.lookup` r of
                  Nothing  -> return (return ()) -- XXX: send message to the host
                  Just rep -> modifyMVar (remoteEndPointState rep) $ \case
                    RemoteEndPointClosed -> throwM InvariantBreak
                    t@(RemoteEndPointValid (ValidRemoteEndPoint sock (Counter x m))) -> do
                      case ourId `Map.lookup` m of
                          Nothing -> return (t, return ())     -- XXX: send message to the hostv
                          Just c  -> do
                            return (RemoteEndPointValid (ValidRemoteEndPoint sock (Counter x (ourId `Map.delete` m)))
                                   , do 
                                        modifyMVar_ (connectionState c) $ \case
                                          ZMQConnectionInit -> return $ ZMQConnectionValid (ValidZMQConnection theirId)
                                          ZMQConnectionClosed -> do
                                              ZMQ.send sock [] $ encode' (MessageCloseConnection theirId)
                                              -- decrement value
                                              return ZMQConnectionClosed
                                          ZMQConnectionValid _ -> throwM InvariantBreak
                                        void $ tryPutMVar (connectionReady c) ()
                                   )
                    RemoteEndPointPending p -> return (RemoteEndPointPending p, undefined)
              where 
                r = endPointRemotes v
          printf "[%s] message init connection ok                      [ok]\n"
                          (B8.unpack $ endPointAddressToByteString ourAddr)
        MessageEndPointClose theirAddress -> do
          eRep <- createOrGetRemoteEndPoint ctx ourEp theirAddress
	  case eRep of
	       Left{} -> throwM InvariantBreak
	       Right rep -> remoteEndPointClose ourEp rep
      where
        ourAddr = _localEndPointAddress ourEp
    finalizeEndPoint ourEp port pull = do
      printf "finalize-end-point\n"
      -- TODO should failed be set to closed
      old <- swapMVar (_localEndPointState ourEp) LocalEndPointClosed
      case old of
      	 LocalEndPointClosed -> return ()
	 LocalEndPointValid v -> do
	   forM_ (Map.elems $ endPointRemotes v) (remoteEndPointClose ourEp)
      ZMQ.unbind pull (address ++ ":" ++ show port)
      ZMQ.close pull
    accure = do
      pull <- ZMQ.socket ctx ZMQ.Pull
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


apiSend :: ZMQConnection -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
apiSend c@(ZMQConnection e _ s _) b = join $ withMVar (remoteEndPointState e) $ \case
  RemoteEndPointClosed    -> return $ return $ Left $ TransportError SendClosed "Remote end point closed."
  RemoteEndPointPending{} -> return $ yield >> apiSend c b
  RemoteEndPointValid  (ValidRemoteEndPoint sock _) -> withMVar s $ \case
    ZMQConnectionInit   -> return $ yield >> apiSend c b
    ZMQConnectionClosed -> return $ return $ Left $ TransportError SendClosed "Connection is closed"
    ZMQConnectionValid  (ValidZMQConnection idx) -> do
      ZMQ.sendMulti sock $ encode' (MessageData idx):|b
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
         v@RemoteEndPointValid{} -> notify idx v
         v@(RemoteEndPointPending p) -> modifyIORef p (\xs -> notify idx : xs) >> return v)
  where
    notify _ RemoteEndPointClosed    = return RemoteEndPointClosed
    notify _ RemoteEndPointPending{} = throwM InvariantBreak
    notify idx w@(RemoteEndPointValid (ValidRemoteEndPoint sock _)) = do
      ZMQ.send sock [] $ encode' (MessageCloseConnection idx)
      return w

apiConnect :: Context
           -> LocalEndPoint
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect ctx ourEp theirAddr reliability _hints = do
    printf "[%s] apiConnect to %s\n"
           saddr
           (B8.unpack $ endPointAddressToByteString theirAddr)
    -- here we have a problem, is exception arrives then push socket may
    -- not me properly closed.
    eRep <- createOrGetRemoteEndPoint ctx ourEp theirAddr
    case eRep of
      Left{} -> return $ Left $ TransportError ConnectFailed "LocalEndPoint is closed."
      Right rep -> do 
        conn <- ZMQConnection <$> pure rep
                              <*> pure reliability
                              <*> newMVar ZMQConnectionInit
                              <*> newEmptyMVar
        let apiConn = Connection 
              { send = apiSend conn
              , close = apiClose conn
              }
        modifyMVar (remoteEndPointState rep) $ \w -> case w of
          RemoteEndPointClosed -> do
            return (RemoteEndPointClosed
                   , Left $ TransportError ConnectFailed "Transport is closed.")
          RemoteEndPointValid ValidRemoteEndPoint{} -> do
            s' <- go conn w
            return (s', Right apiConn)
          RemoteEndPointPending z -> do
            modifyIORef z (\zs -> go conn : zs)
            return (RemoteEndPointPending z, Right apiConn)
  where
    ourAddr = _localEndPointAddress ourEp
    saddr = B8.unpack $ endPointAddressToByteString ourAddr
    go _ RemoteEndPointClosed    = return RemoteEndPointClosed
    go _ RemoteEndPointPending{} = throwM InvariantBreak
    go conn (RemoteEndPointValid (ValidRemoteEndPoint sock (Counter i m))) = do
        ZMQ.send sock [] $ encode' (MessageInitConnection ourAddr i' reliability)
        return $ RemoteEndPointValid (ValidRemoteEndPoint sock (Counter i' (Map.insert i' conn m)))
      where i' = succ i


createOrGetRemoteEndPoint :: Context
                          -> LocalEndPoint
                          -> EndPointAddress
                          -> IO (Either ZMQError RemoteEndPoint)
createOrGetRemoteEndPoint ctx ourEp theirAddr = join $ do
    printf "[%s] apiConnect to %s\n"
           saddr
           (B8.unpack $ endPointAddressToByteString theirAddr)
    -- here we have a problem, is exception arrives then push socket may
    -- not me properly closed.
    modifyMVar (_localEndPointState ourEp) $ \case
      LocalEndPointValid v@(ValidLocalEndPointState _ _ m _) -> do
        case theirAddr `Map.lookup` m of
          Nothing -> do
            printf "[%s] apiConnect: remoteEndPoint not found, creating %s\n"
                    saddr
                   (B8.unpack $ endPointAddressToByteString theirAddr)
            push <- ZMQ.socket ctx ZMQ.Push
            ZMQ.connect push (B8.unpack $ endPointAddressToByteString theirAddr)
            state <- newMVar . RemoteEndPointPending =<< newIORef []
            let rep = RemoteEndPoint theirAddr state
            return ( LocalEndPointValid v{ endPointRemotes = Map.insert theirAddr rep m}
                   , initialize push rep >> return (Right rep)
                   )
          Just rep -> return (LocalEndPointValid v, return $ Right rep)
      LocalEndPointClosed ->
        return  ( LocalEndPointClosed
                , return $ Left $ IncorrectState "EndPoint is closed" 
                )
  where
    ourAddr = _localEndPointAddress ourEp
    saddr = B8.unpack $ endPointAddressToByteString ourAddr
    initialize push rep = do
      printf "[%s][create-remote-endpoint][initialize]\n" saddr 
      ZMQ.send push [] $ encode' $ MessageConnect ourAddr
      let v = ValidRemoteEndPoint push (Counter 0 Map.empty)
      modifyMVar_ (remoteEndPointState rep) $ \case
        RemoteEndPointPending p -> foldM (\x f -> f x) (RemoteEndPointValid v) =<< readIORef p
        RemoteEndPointValid _   -> throwM $ InvariantBreak
        RemoteEndPointClosed    -> throwM $ InvariantBreak

remoteEndPointClose :: LocalEndPoint -> RemoteEndPoint -> IO ()
remoteEndPointClose ourEp rep = do
   old <- swapMVar (remoteEndPointState rep) RemoteEndPointClosed
   case old of
     RemoteEndPointClosed    -> return ()
     RemoteEndPointPending _ -> return ()
     RemoteEndPointValid  v  -> do
       ZMQ.disconnect (_remoteEndPointChan v) (B8.unpack . endPointAddressToByteString $ remoteEndPointAddress rep)
       ZMQ.close (_remoteEndPointChan v)
       modifyMVar_ (_localEndPointState ourEp) $ \case
         LocalEndPointClosed -> return LocalEndPointClosed
	 LocalEndPointValid w -> do
           -- TODO: notify about closed connections
           -- TODO: remove closed connections
           return $ LocalEndPointValid w{endPointRemotes = Map.delete (remoteEndPointAddress rep) (endPointRemotes w)}	
         
encode' :: Binary a => a -> ByteString
encode' = B.concat . BL.toChunks . encode

decode' :: Binary a => ByteString -> a
decode' s = decode . BL.fromChunks $ [s]
