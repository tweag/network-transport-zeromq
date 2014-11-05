-- |
-- Copyright: (C) 2014 EURL Tweag
--
{-# LANGUAGE CPP #-}
module Network.Transport.ZMQ.Internal.Types
  ( ZMQParameters(..)
  , SecurityMechanism(..)
  , defaultZMQParameters
    -- * Internal types
  , TransportInternals(..)
  , TransportState(..)
  , mkTransportState
    -- ** ValidTransportState
  , ValidTransportState
  , transportContext
  , transportEndPoints
  , transportEndPointAt
  , transportAuth
  , transportSockets
    -- ** RemoteEndPoint
  , RemoteEndPoint(..)
  , RemoteEndPointState(..)
  , ValidRemoteEndPoint(..)
  , ClosingRemoteEndPoint(..)
  , remoteEndPointPendingConnections
    -- ** LocalEndPoint
  , LocalEndPoint(..)
  , LocalEndPointState(..)
  , ValidLocalEndPoint(..)
  , localEndPointConnections
  , localEndPointConnectionAt
  , localEndPointRemotes
  , localEndPointRemoteAt
  , localEndPointMulticastGroups

    -- ** ZeroMQ connection
  , ZMQConnection(..)
  , ZMQConnectionState(..)
  , ValidZMQConnection(..)
    -- ** ZeroMQ multicast
  , ZMQMulticastGroup(..)
  , MulticastGroupState(..)
  , ValidMulticastGroup(..)
    -- * ZeroMQ specific types
  , Hints(..)
  , defaultHints
    -- * Internal data structures
  , Counter(..)
  , counterNextId
  , counterValues
  , counterValueAt
  , nextElement
  , nextElement'
  , nextElementM
  , nextElementM'
  ) where

import Control.Category ((>>>))
import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM.TMChan
import Data.Word
import Data.ByteString
import Data.IORef
import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap
import Data.Map.Strict (Map)
import qualified Data.Map.Strict  as Map
import           Data.Set
     ( Set
     )
import           System.ZMQ4
      ( Socket
      , Push
      )

import Network.Transport

import qualified System.ZMQ4 as ZMQ
import Data.Accessor (Accessor, accessor)
import qualified Data.Accessor.Container as DAC (mapMaybe)


-- | Parameters for ZeroMQ connection.
data ZMQParameters = ZMQParameters
  { zmqHighWaterMark     :: Word64 -- uint64_t
  , zmqSecurityMechanism :: Maybe SecurityMechanism
  }

defaultZMQParameters :: ZMQParameters
defaultZMQParameters = ZMQParameters
    { zmqHighWaterMark     = 0
    , zmqSecurityMechanism = Nothing
    }

-- | A ZeroMQ "security mechanism".
data SecurityMechanism
  = SecurityPlain
  { plainPassword :: ByteString
  , plainUsername :: ByteString
  } -- ^ Clear-text authentication, using a (username, password) pair.

type TransportAddress = ByteString

-- | Transport data type.
data TransportInternals = TransportInternals
  { transportAddress :: !TransportAddress
  -- ^ Transport address (used as identifier).
  , transportState  :: !(MVar TransportState)
  -- ^ Internal state.
  , transportParameters :: !ZMQParameters
  -- ^ Parameters that were used to create the transport.
  }

-- | Transport state.
data TransportState
  = TransportValid !ValidTransportState         -- ^ Transport is in active state.
  | TransportClosed                             -- ^ Transport is closed.

-- | Transport state.
data ValidTransportState = ValidTransportState
  { _transportContext   :: !ZMQ.Context
  , _transportEndPoints :: !(Map EndPointAddress LocalEndPoint)
  , _transportAuth      :: !(Maybe (Async ()))
  , _transportSockets   :: !(IORef (IntMap (IO ())))
  -- ^ A set of cleanup actions.
  }

data LocalEndPoint = LocalEndPoint
  { localEndPointAddress :: !EndPointAddress
  , localEndPointState   :: !(MVar LocalEndPointState)
  , localEndPointPort    :: !Int
  }

data LocalEndPointState
  = LocalEndPointValid !ValidLocalEndPoint
  | LocalEndPointClosed

data ValidLocalEndPoint = ValidLocalEndPoint
  {  localEndPointChan        :: !(TMChan Event)
    -- ^ channel for n-t - user communication
  , _localEndPointConnections :: !(Counter ConnectionId ZMQConnection)
    -- ^ list of incomming connections
  , _localEndPointRemotes     :: !(Map EndPointAddress RemoteEndPoint)
    -- ^ list of remote end points
  ,  localEndPointThread      :: !(Async ())
    -- ^ thread id
  ,  localEndPointOpened      :: !(IORef Bool)
    -- ^ is remote endpoint opened
  , _localEndPointMulticastGroups :: !(Map MulticastAddress ZMQMulticastGroup)
    -- ^ list of multicast nodes
  }

data ZMQConnection = ZMQConnection
  { connectionLocalEndPoint  :: !LocalEndPoint
  , connectionRemoteEndPoint :: !RemoteEndPoint
  , connectionReliability    :: !Reliability
  , connectionState          :: !(MVar ZMQConnectionState)
  , connectionReady          :: !(MVar ())
  }

data ZMQConnectionState
  = ZMQConnectionInit
  | ZMQConnectionValid !ValidZMQConnection
  | ZMQConnectionClosed
  | ZMQConnectionFailed

data ValidZMQConnection = ValidZMQConnection
  { _connectionSocket         :: !(Maybe (ZMQ.Socket ZMQ.Push))
  , _connectionId             :: !Word64
  }

data RemoteEndPoint = RemoteEndPoint
  { remoteEndPointAddress :: !EndPointAddress
  , remoteEndPointState   :: !(MVar RemoteEndPointState)
  , remoteEndPointOpened  :: !(IORef Bool)
  }

data RemoteEndPointState
  = RemoteEndPointValid ValidRemoteEndPoint
  | RemoteEndPointClosed
  | RemoteEndPointFailed
  | RemoteEndPointPending (IORef [RemoteEndPointState -> IO RemoteEndPointState])
  | RemoteEndPointClosing ClosingRemoteEndPoint

data ValidRemoteEndPoint = ValidRemoteEndPoint
  {  remoteEndPointSocket               :: !(Socket Push)
  , _remoteEndPointPendingConnections   :: !(Counter ConnectionId ZMQConnection)
  , _remoteEndPointIncommingConnections :: !(Set ConnectionId)
  , _remoteEndPointOutgoingCount        :: !Int
  }

data ClosingRemoteEndPoint = ClosingRemoteEndPoint
  { _remoteEndPointClosingSocket :: !(Socket Push)
  , _remoteEndPointDone :: !(MVar ())
  }

data ZMQMulticastGroup = ZMQMulticastGroup
  { multicastGroupState         :: MVar MulticastGroupState
  , multicastGroupClose         :: IO ()
  }

data MulticastGroupState
  = MulticastGroupValid ValidMulticastGroup
  | MulticastGroupClosed

data ValidMulticastGroup = ValidMulticastGroup
  { _multicastGroupSubscribed :: IORef Bool
  }

data Counter a b = Counter
  { _counterNext   :: !a
  , _counterValue  :: !(Map a b)
  }

-- | A list of Hints provided for connection
data Hints = Hints
  { hintsPort :: Maybe Int                   -- ^ The port to bind.
  , hintsControlPort :: Maybe Int            -- ^ The port that is used to receive multicast messages.
  }

defaultHints :: Hints
defaultHints = Hints Nothing Nothing

nextElement :: (Enum a, Ord a)
            => (b -> IO Bool)
            -> b
            -> Counter a b
            -> IO (Counter a b, (a, b))
nextElement t e c = nextElement' t (const e) c

nextElement' :: (Enum a, Ord a)
             => (b -> IO Bool)
             -> (a -> b)
             -> Counter a b
             -> IO (Counter a b, (a,b))
nextElement' t e c = nextElementM t (return . e) c

nextElementM :: (Enum a, Ord a)
             => (b -> IO Bool)
             -> (a -> IO b)
             -> Counter a b
             -> IO (Counter a b, (a,b))
nextElementM t me (Counter n m) =
    case n' `Map.lookup` m of
      Nothing -> mv >>= \v' -> return (Counter n' (Map.insert n' v' m), (n', v'))
      Just v  -> t v >>= \case
        True -> mv >>= \v' -> return (Counter n' (Map.insert n' v' m), (n', v'))
        False -> nextElementM t me (Counter n' m)
  where
    n' = succ n
    mv = me n'

nextElementM' :: (Enum a, Ord a)
              => (b -> IO Bool)
              -> (a -> IO (b,c))
              -> Counter a b
              -> IO (Counter a b, (a,c))
nextElementM' t me (Counter n m) =
    case n' `Map.lookup` m of
      Nothing -> mv >>= \(v',r) -> return (Counter n' (Map.insert n' v' m), (n', r))
      Just v  -> t v >>= \case
        True -> mv >>= \(v',r) -> return (Counter n' (Map.insert n' v' m), (n', r))
        False -> nextElementM' t me (Counter n' m)
  where
    n' = succ n
    mv = me n'


-------------------------------------------------------------------------------
-- Accessors definitions
-------------------------------------------------------------------------------

transportContext :: Accessor ValidTransportState ZMQ.Context
transportContext = accessor _transportContext (\e t -> t{_transportContext = e})

transportEndPoints :: Accessor ValidTransportState (Map EndPointAddress LocalEndPoint)
transportEndPoints = accessor _transportEndPoints (\e t -> t{_transportEndPoints = e})

transportEndPointAt :: EndPointAddress -> Accessor ValidTransportState (Maybe LocalEndPoint)
transportEndPointAt addr = transportEndPoints >>> DAC.mapMaybe addr

transportAuth :: Accessor ValidTransportState (Maybe (Async ()))
transportAuth = accessor _transportAuth (\e t -> t{_transportAuth = e})

transportSockets :: Accessor ValidTransportState (IORef (IntMap (IO ())))
transportSockets = accessor _transportSockets (\e t -> t{_transportSockets = e})

localEndPointConnections :: Accessor ValidLocalEndPoint (Counter ConnectionId ZMQConnection)
localEndPointConnections = accessor _localEndPointConnections (\e t -> t{_localEndPointConnections = e})

localEndPointConnectionAt :: ConnectionId -> Accessor ValidLocalEndPoint (Maybe ZMQConnection)
localEndPointConnectionAt idx = localEndPointConnections >>> counterValueAt idx

localEndPointRemotes :: Accessor ValidLocalEndPoint (Map EndPointAddress RemoteEndPoint)
localEndPointRemotes = accessor _localEndPointRemotes (\e t -> t{_localEndPointRemotes = e})

localEndPointRemoteAt :: EndPointAddress -> Accessor ValidLocalEndPoint (Maybe RemoteEndPoint)
localEndPointRemoteAt addr = localEndPointRemotes >>> DAC.mapMaybe addr 

localEndPointMulticastGroups :: Accessor ValidLocalEndPoint (Map MulticastAddress ZMQMulticastGroup)
localEndPointMulticastGroups = accessor _localEndPointMulticastGroups (\e t -> t{_localEndPointMulticastGroups = e})

remoteEndPointPendingConnections :: Accessor ValidRemoteEndPoint (Counter ConnectionId ZMQConnection)
remoteEndPointPendingConnections = accessor _remoteEndPointPendingConnections (\e t -> t{_remoteEndPointPendingConnections = e})

counterNextId :: Accessor (Counter a b) a
counterNextId = accessor _counterNext (\e t -> t{_counterNext = e})

counterValues :: Accessor (Counter a b) (Map a b)
counterValues = accessor _counterValue (\e t -> t{_counterValue = e})

counterValueAt :: (Ord a) => a -> Accessor (Counter a b) (Maybe b)
counterValueAt idx = counterValues >>> DAC.mapMaybe idx

--------------------------------------------------------------------------------
-- Smart constructors
--------------------------------------------------------------------------------
mkTransportState :: ZMQ.Context -> Maybe (Async ()) -> IO TransportState

mkTransportState ctx auth
  = TransportValid <$>
      (ValidTransportState
         <$> pure ctx
         <*> pure (Map.empty)
         <*> pure auth
         <*> newIORef IntMap.empty)
