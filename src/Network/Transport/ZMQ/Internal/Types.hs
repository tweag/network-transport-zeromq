-- |
-- Copyright: (C) 2014 EURL Tweag
--

module Network.Transport.ZMQ.Internal.Types
  ( ZMQParameters(..)
  , ZMQAuthType(..)
  , defaultZMQParameters
    -- * Internal types
  , ZMQTransport(..)
  , TransportState(..)
  , ValidTransportState(..)
    -- ** RemoteEndPoint
  , RemoteEndPoint(..)
  , RemoteEndPointState(..)
  , ValidRemoteEndPoint(..)
  , ClosingRemoteEndPoint(..)
    -- ** LocalEndPoint
  , LocalEndPoint(..)
  , LocalEndPointState(..)
  , ValidLocalEndPoint(..)
    -- ** ZeroMQ connection
  , ZMQConnection(..)
  , ZMQConnectionState(..)
  , ValidZMQConnection(..)
    -- ** ZeroMQ multicast
  , ZMQMulticastGroup(..)
  , MulticastGroupState(..)
  , ValidMulticastGroup(..)
    -- * Internal data structures
  , Counter(..)
  , nextElement
  , nextElement'
  , nextElementM
  , nextElementM'
  ) where

import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM.TMChan
import Data.Word
import Data.ByteString
import Data.IORef
import Data.IntMap (IntMap)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict  as M
import           Data.Set
     ( Set
     )
import           System.ZMQ4
      ( Socket
      , Push
      )

import Network.Transport

import qualified System.ZMQ4 as ZMQ


-- | Parameters for ZeroMQ connection.
data ZMQParameters = ZMQParameters
  { highWaterMark     :: Word64 -- uint64_t
  , authorizationType :: ZMQAuthType
  , minPort           :: Int
  , maxPort           :: Int
  , maxTries          :: Int
  }

defaultZMQParameters :: ZMQParameters
defaultZMQParameters = ZMQParameters
    { highWaterMark     = 0
    , authorizationType = ZMQNoAuth
    , minPort           = 40000
    , maxPort           = 60000
    , maxTries          = 10000
    }

data ZMQAuthType
  = ZMQNoAuth
  | ZMQAuthPlain
    { zmqAuthPlainPassword :: ByteString
    , zmqAutnPlainUserName :: ByteString
    }

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
  , _transportEndPoints :: !(Map EndPointAddress LocalEndPoint)
  , _transportAuth      :: !(Maybe (Async ()))
  , _transportSockets   :: !(IORef (IntMap (IO ())))
  -- ^ A set of cleanup actions.
  }

data LocalEndPoint = LocalEndPoint
  { localEndPointAddress :: !EndPointAddress
  , localEndPointState   :: !(MVar LocalEndPointState)
  , localEndPointPort    :: !(Int)
  }

data LocalEndPointState
  = LocalEndPointValid !ValidLocalEndPoint
  | LocalEndPointClosed

data ValidLocalEndPoint = ValidLocalEndPoint
  { _localEndPointChan        :: !(TMChan Event)
    -- ^ channel for n-t - user communication
  , _localEndPointConnections :: !(Counter ConnectionId ZMQConnection)
    -- ^ list of incomming connections
  , _localEndPointRemotes     :: !(Map EndPointAddress RemoteEndPoint)
    -- ^ list of remote end points
  , _localEndPointThread      :: !(Async ())
    -- ^ thread id
  , _localEndPointOpened      :: !(IORef Bool)
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
  { _remoteEndPointChan                 :: !(Socket Push)
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
  { counterNext   :: !a
  , counterValue  :: !(Map a b)
  }

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
    case n' `M.lookup` m of
      Nothing -> mv >>= \v' -> return (Counter n' (M.insert n' v' m), (n', v'))
      Just v  -> t v >>= \case
        True -> mv >>= \v' -> return (Counter n' (M.insert n' v' m), (n', v'))
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
    case n' `M.lookup` m of
      Nothing -> mv >>= \(v',r) -> return (Counter n' (M.insert n' v' m), (n', r))
      Just v  -> t v >>= \case
        True -> mv >>= \(v',r) -> return (Counter n' (M.insert n' v' m), (n', r))
        False -> nextElementM' t me (Counter n' m)
  where
    n' = succ n
    mv = me n'
