{-# LANGUAGE DeriveGeneric, StandaloneDeriving, OverloadedStrings, DeriveDataTypeable #-}
module Network.Transport.ZMQ
  ( -- * Main API
    createTransport       -- :: ZMQParameters -> ByteString -> IO (Either ZMQError Transport)
  , ZMQParameters(..)     
  , ZMQAuthType(..)
  , defaultZMQParameters  -- :: ZMQParameters
  -- * Internals
  -- * $Design
  ) where

import Network.Transport.ZMQ.Types

import           Control.Applicative
import           Control.Concurrent
       ( yield
       , threadDelay
       )
import qualified Control.Concurrent.Async as Async
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TMChan
import           Control.Monad
      ( void
      , forever
      , unless
      , join
      , forM_
      , foldM
      , replicateM
      , when
      )
import           Control.Monad.Catch
      ( finally
      , bracket
      , try
      , throwM
      , Exception
      , SomeException
      , mask
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
-- import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Typeable
import           Data.Traversable
import           Data.Void
import           GHC.Generics 

import Network.Transport
import Network.Transport.ZMQ.Types
import           System.ZMQ4
      ( Context )
import qualified System.ZMQ4 as ZMQ
import qualified System.ZMQ4.Utils   as ZMQ

import Text.Printf

--------------------------------------------------------------------------------
--- Internal datatypes                                                        --
--------------------------------------------------------------------------------
-- $design
--
-- In the zeromq backend we are using following address scheme:
--
-- tcp://host:port/
--  |       |   |
--  |       +---+---------------- can be configured by user, one host,
--  |                             port pair per distributed process
--  |                             instance
--  +---------------------------- In feature it will be possible to add
--                                another hosts.
--
-- Transport specifies host that will be used, and port will be
-- automatically generated by the transport.                               
--
-- Connection reliability.
--
-- Currently only reliable connections are supportred (so in case if you
-- pass want unreliable connection it will also be reliable), this will be
-- changed in future, by creating additional address for each endpoint.
--
-- Network-transport-zeromq maintains 1 thread for each endpoint that is
-- used to read incomming requests, all send and connection requests are
-- handled from the user thread
--
-- Connections.
--    0MQ automatically handles connection liveness. This breaks some
--    assumptions about connectivity and leads to problems with connection
--    handling.
--

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

-- Naming conventions.
-- api* -- functions that can be called by user
-- localEndPoint -- internal functions that are used in endpoints
-- remoteEndPoint -- internal functions that are used in endpoints
--
-- Transport 
--   |--- Valid                transport itself
--   |      |--- newEndpoint
--   |      |--- close         
--   |--- Closed
--
-- LocaEndPoint
-- RemoteEndPoint
-- Connection




-- | Messages
data ZMQMessage
      = MessageConnect !EndPointAddress -- ^ Connection greeting
      | MessageInitConnection !EndPointAddress !ConnectionId !Reliability
      | MessageInitConnectionOk !EndPointAddress !ConnectionId !ConnectionId
      | MessageCloseConnection !ConnectionId
      | MessageData !ConnectionId
      | MessageEndPointClose !EndPointAddress !Bool
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
              | ConnectionFailed
              deriving (Typeable, Show)

instance Exception ZMQError

createTransport :: ZMQParameters    -- ^ Transport features.
                -> ByteString       -- ^ Host name or IP address
                -> IO (Either (TransportError Void) Transport)
createTransport params host = do
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

-- Synchronous
apiTransportClose :: ZMQTransport -> IO ()
apiTransportClose transport = do
    old <- swapMVar (_transportState transport) TransportClosed
    case old of
      TransportClosed -> return ()
      TransportValid (ValidTransportState ctx m) -> do
        forM_ (Map.elems m) $ apiCloseEndPoint True transport
	ZMQ.term ctx

apiNewEndPoint :: ZMQParameters -> ZMQTransport -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint params transport = do
--    printf "[transport] endpoint create\n"
    elep <- modifyMVar (_transportState transport) $ \case
       TransportClosed -> return (TransportClosed, Left $ TransportError NewEndPointFailed "Transport is closed.")
       v@(TransportValid i@(ValidTransportState ctx _)) -> do
         eEndPoint <- endPointCreate params ctx (B8.unpack addr)
         case eEndPoint of
           Right (port, ep, chan) -> return 
	   	  ( TransportValid i{_transportEndPoints = Map.insert port ep (_transportEndPoints i)}
                  , Right (ep, ctx, chan))
           Left _ -> return (v, Left $ TransportError NewEndPointFailed "Failed to create new endpoint.")
    case elep of
      Right (ep,ctx, chOut) ->
        return $ Right $ EndPoint
          { receive = atomically $ do
              mx <- readTMChan chOut
              case mx of
                Nothing -> error "channel is closed"
                Just x  -> return x
          , address = _localEndPointAddress ep
          , connect = apiConnect ctx ep
          , closeEndPoint = apiCloseEndPoint True transport ep
          , newMulticastGroup     = return . Left $
              TransportError NewMulticastGroupUnsupported "Multicast not supported"
          , resolveMulticastGroup = return . return . Left $
              TransportError ResolveMulticastGroupUnsupported "Multicast not supported"
          }
      Left x -> return $ Left x
  where
    addr = transportAddress transport

-- | Asynchronous operation, shutdown of the remote end point may take a while
apiCloseEndPoint :: Bool
                 -> ZMQTransport
                 -> LocalEndPoint
		 -> IO ()
apiCloseEndPoint fast transport lep = do
--    printf "[%s][go] close endpoint\n"
--           (B8.unpack $ endPointAddressToByteString $ _localEndPointAddress lep)
    old <- readMVar (_localEndPointState lep)
    case old of
      LocalEndPointValid (ValidLocalEndPointState x _ _ threadId) -> do
         let a = do Async.cancel threadId
                    void $ Async.waitCatch threadId
             b = atomically $ do
                    writeTMChan x EndPointClosed
                    closeTMChan x
         if fast
         then b >> a
         else a >> b
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
               -> IO (Either (TransportError NewEndPointErrorCode) (Int,LocalEndPoint, TMChan Event))
endPointCreate params ctx address = do
    em <- try $ accure
    case em of
      Right (port,pull) -> do
          chOut <- newTMChanIO
          let addr = EndPointAddress $ B8.pack (address ++ ":" ++ show port)
          lep   <- LocalEndPoint <$> pure addr
                                 <*> newEmptyMVar
                                 <*> pure port
--          printf "[end-point-create] addr: %s\n" (B8.unpack $ endPointAddressToByteString addr)
          thread <- mask $ \restore ->
             Async.async $ (restore (receiver pull lep chOut)) 
                           `finally` finalizeEndPoint lep port pull
          putMVar (_localEndPointState lep) $ LocalEndPointValid 
            (ValidLocalEndPointState chOut (Counter 0 Map.empty) Map.empty thread)
          return $ Right (port, lep, chOut)
      Left (_e::SomeException)  -> do
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
--          printf "[%s] message connect from %s\n"
--                 (B8.unpack $ endPointAddressToByteString ourAddr)
--                 (B8.unpack $ endPointAddressToByteString theirAddress)
          void $ createOrGetRemoteEndPoint ctx ourEp theirAddress
        MessageInitConnection theirAddress theirId rel -> do
--        printf "[%s] message init connection from %s\n"
--                (B8.unpack $ endPointAddressToByteString ourAddr)
--                (B8.unpack $ endPointAddressToByteString theirAddress)
          join $ do
            modifyMVar (_localEndPointState ourEp) $ \case
                LocalEndPointValid v ->
                    case theirAddress `Map.lookup` r of
                      Nothing -> return (LocalEndPointValid v, throwM InvariantBreak)
                      Just rep -> modifyMVar (remoteEndPointState rep) $ \case
                          RemoteEndPointFailed -> throwM $ InvariantBreak
                          RemoteEndPointClosed -> throwM $ InvariantBreak
                          w@RemoteEndPointValid{} -> do
                            conn <- ZMQConnection <$> pure ourEp
                                                  <*> pure rep
                                                  <*> pure rel
                                                  <*> newMVar (ZMQConnectionValid $ ValidZMQConnection i)
                                                  <*> newEmptyMVar
                            w' <- register (succ i) w
                            return ( w'
                                   , ( LocalEndPointValid v{ endPointConnections = Counter (succ i) (Map.insert (succ i) conn m) }
                                     , return ())
                                   )
                          z@(RemoteEndPointPending w) -> do
                            conn <- ZMQConnection <$> pure ourEp
                                                  <*> pure rep
                                                  <*> pure rel
                                                  <*> newMVar (ZMQConnectionValid $ ValidZMQConnection i)
                                                  <*> newEmptyMVar
                            modifyIORef w (\xs -> (register (succ i))  : xs)
                            return ( z
                                   , ( LocalEndPointValid v{ endPointConnections = Counter (succ i) (Map.insert (succ i) conn m) }
                                     , return ())
                                   )
                  where
                    r = endPointRemotes v
                    (Counter i m) = endPointConnections v
                _ -> throwM InvariantBreak
--          printf "[%s] message init connection           [ok] \n"
--                          (B8.unpack $ endPointAddressToByteString ourAddr)
          where
            register i RemoteEndPointFailed = do
              atomically $ do
                writeTMChan chan (ConnectionOpened i rel theirAddress)
                writeTMChan chan (ConnectionClosed i)
              return RemoteEndPointFailed
            register i RemoteEndPointClosed = do
              atomically $ do
                writeTMChan chan (ConnectionOpened i rel theirAddress)
                writeTMChan chan (ConnectionClosed i)                        
              return RemoteEndPointClosed
            register _ RemoteEndPointPending{} = throwM InvariantBreak
            register i (RemoteEndPointValid v@(ValidRemoteEndPoint sock _ s _)) = do
              ZMQ.send sock [] $ encode' (MessageInitConnectionOk ourAddr theirId i)
              atomically $ writeTMChan chan (ConnectionOpened i rel theirAddress)
              return $ RemoteEndPointValid
                v{_remoteEndPointIncommingConnections = Set.insert i s}
        MessageCloseConnection idx -> join $ do
--          printf "[%s] message init connection: %i\n"
--                 (B8.unpack $ endPointAddressToByteString ourAddr)
--                 idx
          modifyMVar (_localEndPointState ourEp) $ \case
            LocalEndPointValid v ->
                case idx `Map.lookup` m of
                  Nothing  -> return (LocalEndPointValid v, return ())
                  Just conn -> do
                    old <- swapMVar (connectionState conn) ZMQConnectionFailed
                    return ( LocalEndPointValid v{ endPointConnections = Counter i (idx `Map.delete` m)}
                           , case old of
                               ZMQConnectionFailed -> return ()
                               ZMQConnectionInit -> return  () -- throwM InvariantViolation
                               ZMQConnectionClosed -> return ()
                               ZMQConnectionValid (ValidZMQConnection _) -> do
                                  atomically $ writeTMChan chan (ConnectionClosed idx)
                                  connectionCleanup (connectionRemoteEndPoint conn) idx)
              where
                (Counter i m) = endPointConnections v
	    LocalEndPointClosed -> return (LocalEndPointClosed, return ())
        MessageInitConnectionOk theirAddress ourId theirId -> do
--          printf "[%s] message init connection ok: %i -> %i\n"
--                 (B8.unpack $ endPointAddressToByteString ourAddr)
--                 ourId
--                 theirId
          join $ withMVar (_localEndPointState ourEp) $ \case
            LocalEndPointValid v -> 
                case theirAddress `Map.lookup` r of
                  Nothing  -> return (return ()) -- XXX: send message to the host
                  Just rep -> modifyMVar (remoteEndPointState rep) $ \case
                    RemoteEndPointFailed -> return (RemoteEndPointFailed, return ())
                    RemoteEndPointClosed -> throwM InvariantBreak
                    t@(RemoteEndPointValid (ValidRemoteEndPoint sock (Counter x m) s z)) -> do
                      case ourId `Map.lookup` m of
                          Nothing -> return (t, return ())     -- XXX: send message to the hostv
                          Just c  -> do
                            return (RemoteEndPointValid (ValidRemoteEndPoint sock (Counter x (ourId `Map.delete` m)) s (z+1))
                                   , do 
                                        modifyMVar_ (connectionState c) $ \case
                                          ZMQConnectionFailed -> return ZMQConnectionFailed
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
            LocalEndPointClosed -> return $ return ()
--          printf "[%s] message init connection ok                      [ok]\n"
--                          (B8.unpack $ endPointAddressToByteString ourAddr)
        MessageEndPointClose theirAddress True -> getRemoteEndPoint ourEp theirAddress >>= \case
          Nothing  -> return ()
          Just rep -> remoteEndPointClose True (Right ourEp) rep
        MessageEndPointClose theirAddress False -> getRemoteEndPoint ourEp theirAddress >>= \case
          Nothing  -> return ()
          Just rep -> do
            modifyMVar_ (_localEndPointState ourEp) $ \case
              LocalEndPointValid v -> do
                atomically $ writeTMChan (_localEndPointOutputChan v) $
                  ErrorEvent $ TransportError (EventConnectionLost (_localEndPointAddress ourEp)) "exception on remote side"
                old <- swapMVar (remoteEndPointState rep) RemoteEndPointFailed
                case old of
                  RemoteEndPointValid w -> do
                    let (Counter i cn) = _remoteEndPointPendingConnections w
                    traverse (\c -> void $ swapMVar (connectionState c) ZMQConnectionFailed) cn        
                    cn' <- foldM 
                          (\(Counter i cn) idx -> case idx `Map.lookup` cn of
                              Nothing -> return (Counter i cn)
                              Just c -> swapMVar (connectionState c) ZMQConnectionFailed >> return (Counter i (Map.delete idx cn))
                          )
                          (endPointConnections v) 
                          (Set.toList (_remoteEndPointIncommingConnections w))
                    ZMQ.close (_remoteEndPointChan w)
                    return $ LocalEndPointValid v{ endPointConnections=cn'
                                                 , endPointRemotes = Map.delete theirAddress (endPointRemotes v)}
                  _ -> return $ LocalEndPointValid v
              c -> return c
             
      where
        ourAddr = _localEndPointAddress ourEp
    finalizeEndPoint ourEp port pull = do
--      printf "[%s] finalize-end-point\n"
--             (B8.unpack $ endPointAddressToByteString $ _localEndPointAddress ourEp)
      old <- swapMVar (_localEndPointState ourEp) LocalEndPointClosed
      case old of
        LocalEndPointClosed -> return ()
        LocalEndPointValid v -> do
          forM_ (Map.elems $ endPointRemotes v) $ (remoteEndPointClose False (Left (v, _localEndPointAddress ourEp)))
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
apiSend c@(ZMQConnection l e _ s _) b = join $ withMVar (remoteEndPointState e) $ \case
  RemoteEndPointFailed    -> withMVar s $ \case
    ZMQConnectionClosed -> afterP $ Left $ TransportError SendClosed "RemoteEndPoint closed"
    _                   -> afterP $ Left $ TransportError SendFailed "RemoteEndPointFailed"
  RemoteEndPointClosed    -> do
    void $ swapMVar (connectionState c) ZMQConnectionFailed
    afterP $ Left $ TransportError SendFailed "Remote end point closed."
  RemoteEndPointPending{} -> return $ yield >> apiSend c b
  RemoteEndPointValid  (ValidRemoteEndPoint sock _ _ _) -> withMVar s $ \case
    ZMQConnectionInit   -> return $ yield >> apiSend c b
    ZMQConnectionClosed -> afterP $ Left $ TransportError SendClosed "Connection is closed"
    ZMQConnectionValid  (ValidZMQConnection idx) -> do
      x <- try $ ZMQ.sendMulti sock $ encode' (MessageData idx):|b
      case x of
        Right () -> afterP $ Right ()
        Left  ex  -> do
          ZMQ.send sock [] $ encode' (MessageEndPointClose (_localEndPointAddress l) False)
          return $ do
            -- move to some function
            old <- swapMVar (remoteEndPointState e) RemoteEndPointFailed
            modifyMVar_ (_localEndPointState l) $ \case
              LocalEndPointValid v -> do
                atomically $ writeTMChan (_localEndPointOutputChan v) $
                  ErrorEvent $ TransportError (EventConnectionLost (remoteEndPointAddress e)) (show ex)
                return $ LocalEndPointValid v{endPointRemotes = Map.delete (remoteEndPointAddress e) (endPointRemotes v)}
              y -> return y
            return $ Left $ TransportError SendFailed (show (ex::SomeException))
    ZMQConnectionFailed -> afterP $ Left $ TransportError SendFailed "Remote end point closed."

-- 'apiClose' function is asynchronous, as connection may not exists by the
-- time of the calling to this function. In this case function just marks
-- connection as closed, so all subsequent calls from the user side will
-- "think" that the connection is closed, and remote side will be contified
-- only after connection will be up.
apiClose :: ZMQConnection -> IO ()
apiClose (ZMQConnection _ e _ s _) = join $ do
   modifyMVar s $ \case
     ZMQConnectionInit   -> return (ZMQConnectionClosed, return ())
     ZMQConnectionClosed -> return (ZMQConnectionClosed, return ())
     ZMQConnectionFailed -> return (ZMQConnectionClosed, return ())
     ZMQConnectionValid (ValidZMQConnection idx) -> do
       return (ZMQConnectionClosed, do
         modifyMVar_ (remoteEndPointState e) $ \case
           v@RemoteEndPointClosed -> return v
           v@RemoteEndPointFailed -> return v
           v@RemoteEndPointValid{} -> notify idx v
           v@(RemoteEndPointPending p) -> modifyIORef p (\xs -> notify idx : xs) >> return v
         )
  where
    notify _ RemoteEndPointFailed    = return RemoteEndPointFailed
    notify _ RemoteEndPointClosed    = return RemoteEndPointClosed
    notify _ RemoteEndPointPending{} = throwM InvariantBreak
    notify idx w@(RemoteEndPointValid (ValidRemoteEndPoint sock _ _ _)) = do
      ZMQ.send sock [] $ encode' (MessageCloseConnection idx)
      return w

apiConnect :: Context
           -> LocalEndPoint
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect ctx ourEp theirAddr reliability _hints = do
--  printf "[%s] apiConnect to %s\n"
--       (B8.unpack $ endPointAddressToByteString $ _localEndPointAddress ourEp)
--       (B8.unpack $ endPointAddressToByteString theirAddr)
    -- here we have a problem, is exception arrives then push socket may
    -- not me properly closed.
    eRep <- createOrGetRemoteEndPoint ctx ourEp theirAddr
    case eRep of
      Left{} -> return $ Left $ TransportError ConnectFailed "LocalEndPoint is closed."
      Right rep -> do 
        conn <- ZMQConnection <$> pure ourEp
                              <*> pure rep
                              <*> pure reliability
                              <*> newMVar ZMQConnectionInit
                              <*> newEmptyMVar
        let apiConn = Connection 
              { send = apiSend conn
              , close = apiClose conn
              }
        join $ modifyMVar (remoteEndPointState rep) $ \w -> case w of
          RemoteEndPointClosed -> do
            return (RemoteEndPointClosed
                   , return $ Left $ TransportError ConnectFailed "Transport is closed.")
          RemoteEndPointValid ValidRemoteEndPoint{} -> do
            s' <- go conn w
            return (s', waitReady conn apiConn)
          RemoteEndPointPending z -> do
            modifyIORef z (\zs -> go conn : zs)
            return (RemoteEndPointPending z, waitReady conn apiConn)
          RemoteEndPointFailed ->
            return ( RemoteEndPointFailed
                   , return $ Left $ TransportError ConnectFailed "RemoteEndPoint failed.")
  where
    waitReady conn apiConn = join $ withMVar (connectionState conn) $ \case
      ZMQConnectionInit{}   -> return $ waitReady conn apiConn
      ZMQConnectionValid{}  -> afterP $ Right apiConn
      ZMQConnectionFailed{} -> afterP $ Left $ TransportError ConnectFailed "Connection failed."
      ZMQConnectionClosed{} -> throwM $ InvariantBreak
    ourAddr = _localEndPointAddress ourEp
    go _ RemoteEndPointClosed    = putStrLn ">~>" >> return RemoteEndPointClosed
    go _ RemoteEndPointPending{} = throwM InvariantBreak
    go _ RemoteEndPointFailed    = putStrLn ">->" >> return RemoteEndPointFailed
    go conn (RemoteEndPointValid (ValidRemoteEndPoint sock (Counter i m) s z)) = do
        ZMQ.send sock [] $ encode' (MessageInitConnection ourAddr i' reliability)
        return $ RemoteEndPointValid (ValidRemoteEndPoint sock (Counter i' (Map.insert i' conn m)) s z)
      where i' = succ i


getRemoteEndPoint :: LocalEndPoint -> EndPointAddress -> IO (Maybe RemoteEndPoint)
getRemoteEndPoint ourEp theirAddr = do
    withMVar (_localEndPointState ourEp) $ \case
      LocalEndPointValid v -> return $ theirAddr `Map.lookup` (endPointRemotes v)
      LocalEndPointClosed  -> return Nothing

createOrGetRemoteEndPoint :: Context
                          -> LocalEndPoint
                          -> EndPointAddress
                          -> IO (Either ZMQError RemoteEndPoint)
createOrGetRemoteEndPoint ctx ourEp theirAddr = join $ do
--    printf "[%s] apiConnect to %s\n"
--           saddr
--           (B8.unpack $ endPointAddressToByteString theirAddr)
    modifyMVar (_localEndPointState ourEp) $ \case
      LocalEndPointValid v@(ValidLocalEndPointState _ _ m _) -> do
        case theirAddr `Map.lookup` m of
          Nothing -> do
--            printf "[%s] apiConnect: remoteEndPoint not found, creating %s\n"
--                   (B8.unpack $ endPointAddressToByteString $ _localEndPointAddress ourEp)
--                  (B8.unpack $ endPointAddressToByteString theirAddr)
            push <- ZMQ.socket ctx ZMQ.Push
            state <- newMVar . RemoteEndPointPending =<< newIORef []
            let rep = RemoteEndPoint theirAddr state
            return ( LocalEndPointValid v{ endPointRemotes = Map.insert theirAddr rep m}
                   , initialize push rep >> return (Right rep))
          Just rep -> return (LocalEndPointValid v, return $ Right rep)
      LocalEndPointClosed ->
        return  ( LocalEndPointClosed
                , return $ Left $ IncorrectState "EndPoint is closed" 
                )
  where
    ourAddr = _localEndPointAddress ourEp
    initialize push rep = do
      lock <- newEmptyMVar
      x <- Async.async $ do
         takeMVar lock
         ZMQ.connect push (B8.unpack $ endPointAddressToByteString theirAddr)
      monitor <- ZMQ.monitor [ZMQ.ConnectedEvent, ZMQ.ConnectRetriedEvent] ctx push
      putMVar lock ()
      r <- Async.race (threadDelay 1000000) (monitor True)
      let eres = case r of
            Right (Just ZMQ.Connected{}) -> Right ()
            _  -> Left ()
      monitor False
      case eres of
        Left _ -> do
          _ <- swapMVar (remoteEndPointState rep) RemoteEndPointFailed
          Async.cancel x
          ZMQ.close push
        Right _ -> do
          ZMQ.send push [] $ encode' $ MessageConnect ourAddr
          let v = ValidRemoteEndPoint push (Counter 0 Map.empty) Set.empty 0
          modifyMVar_ (remoteEndPointState rep) $ \case
            RemoteEndPointPending p -> foldM (\y f -> f y) (RemoteEndPointValid v) . Prelude.reverse =<< readIORef p
            RemoteEndPointValid _   -> throwM $ InvariantBreak
            RemoteEndPointClosed    -> return RemoteEndPointClosed
            RemoteEndPointFailed    -> return RemoteEndPointFailed


remoteEndPointClose :: Bool -> (Either (ValidLocalEndPointState,EndPointAddress) LocalEndPoint) -> RemoteEndPoint -> IO ()
remoteEndPointClose silent eOurEp rep = do
--   printf "[???] remoteEndPointClose %s\n"          
--          (B8.unpack $ endPointAddressToByteString $ lAddr)
--          (B8.unpack $ endPointAddressToByteString $ remoteEndPointAddress rep)
   old <- swapMVar (remoteEndPointState rep) RemoteEndPointFailed
   go old
 where
   go old = case old of
     RemoteEndPointFailed    -> return ()
     RemoteEndPointClosed    -> return ()
     RemoteEndPointPending x -> go =<< foldM (\x f-> f x) RemoteEndPointFailed =<< readIORef x
     RemoteEndPointValid  v@(ValidRemoteEndPoint _ _ s i)  -> do
       forM_ (Set.toList s) $ \cid -> do
         -- XXX: introduce generic connectionClose method
         case eOurEp of
           Left (state,_) -> do
             atomically $ writeTMChan (_localEndPointOutputChan state) $ ConnectionClosed cid                
           Right ourEp -> do
             modifyMVar_ (_localEndPointState ourEp) $ \case
               LocalEndPointValid w ->  do
                 atomically $ writeTMChan (_localEndPointOutputChan w) $ ConnectionClosed cid
                 let (Counter i m) = endPointConnections w
                 case cid `Map.lookup` m of
                   Nothing  -> return $ LocalEndPointValid w
                   Just con -> do
                     _ <- swapMVar (connectionState con) ZMQConnectionFailed
                     return $ LocalEndPointValid
                       w{endPointConnections=Counter i (Map.delete cid m)}                  
               c -> return c
       when (i > 0) $
         case eOurEp of
--           This mean we called transport close
--           Left (v, _) ->  atomically $ writeTMChan (_localEndPointOutputChan v)
--               $ ErrorEvent $ TransportError (EventConnectionLost (remoteEndPointAddress rep)) "disconnect event"
           Right ourEp -> withMVar (_localEndPointState ourEp) $ \case
              LocalEndPointValid v ->  atomically $ writeTMChan (_localEndPointOutputChan v) 
                 $ ErrorEvent $ TransportError (EventConnectionLost (remoteEndPointAddress rep)) "disconnect event"
              _ -> return ()
           _ -> return ()
       case eOurEp of
         Left (_, addr) -> do
           unless silent $ do
             ZMQ.send (_remoteEndPointChan v) [] $ encode' $ MessageEndPointClose addr True
             threadDelay 1000000
--             ZMQ.disconnect (_remoteEndPointChan v) (B8.unpack . endPointAddressToByteString $ remoteEndPointAddress rep)
         Right ourEp -> do
           unless silent $ do
             ZMQ.send (_remoteEndPointChan v) [] $ encode' $ MessageEndPointClose (_localEndPointAddress ourEp) True
--             ZMQ.disconnect (_remoteEndPointChan v) (B8.unpack . endPointAddressToByteString $ remoteEndPointAddress rep)
           modifyMVar_ (_localEndPointState ourEp) $ \case
             LocalEndPointClosed -> return LocalEndPointClosed
             LocalEndPointValid w -> do
               return $ LocalEndPointValid w{endPointRemotes = Map.delete (remoteEndPointAddress rep) (endPointRemotes w)}	
       ZMQ.close (_remoteEndPointChan v)

connectionCleanup :: RemoteEndPoint -> ConnectionId -> IO ()
connectionCleanup rep cid = modifyMVar_ (remoteEndPointState rep) $ \case
   RemoteEndPointValid v -> return $
      RemoteEndPointValid v{_remoteEndPointIncommingConnections = Set.delete cid (_remoteEndPointIncommingConnections v)}
   c -> return c
         
encode' :: Binary a => a -> ByteString
encode' = B.concat . BL.toChunks . encode

decode' :: Binary a => ByteString -> a
decode' s = decode . BL.fromChunks $ [s]


afterP :: a -> IO (IO a)
afterP = return . return

trySome :: IO a -> IO (Either SomeException a)
trySome = try
