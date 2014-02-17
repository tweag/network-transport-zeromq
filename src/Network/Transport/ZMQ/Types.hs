{-# LANGUAGE LambdaCase #-}
module Network.Transport.ZMQ.Types
    ( ZMQParameters(..)
    , ZMQAuthType(..)
    , defaultZMQParameters
      -- * Internal types
      -- ** RemoteEndPoint
    , RemoteEndPoint(..)
    , RemoteEndPointState(..)
    , ValidRemoteEndPoint(..)
      -- ** LocalEndPoint
    , LocalEndPoint(..)
    , LocalEndPointState(..)
    , ValidLocalEndPointState(..)
      -- ** ZeroMQ connection
    , ZMQConnection(..)
    , ZMQConnectionState(..)
    , ValidZMQConnection(..)
      -- ** Events
    , LocalEndPointEvent(..)
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
import Control.Concurrent.Chan
import Data.Word
import Data.ByteString
import Data.IORef
import Data.List.NonEmpty
import Data.Map.Strict (Map)
import qualified Data.Map.Strict  as M

import Network.Transport
-- | Parameters for ZeroMQ connection
data ZMQParameters = ZMQParameters
      { highWaterMark :: Word64 -- uint64_t
      , lingerPeriod  :: Int    -- int
      , authorizationType :: ZMQAuthType
      , minPort       :: Int
      , maxPort       :: Int
      , maxTries      :: Int
      }
-- High Watermark

defaultZMQParameters :: ZMQParameters
defaultZMQParameters = ZMQParameters
      { highWaterMark = 0
      , lingerPeriod  = 0
      , authorizationType = ZMQNoAuth
      , minPort       = 2000
      , maxPort       = 60000
      , maxTries      = 1000
      }

data ZMQAuthType
        = ZMQNoAuth
        | ZMQAuthPlain
            { zmqAuthPlainPassword :: ByteString
            , zmqAutnPlainUserName :: ByteString
            }

data LocalEndPoint = LocalEndPoint
      { _localEndPointAddress :: !EndPointAddress
      , _localEndPointState :: MVar LocalEndPointState
      }

data LocalEndPointState
      = LocalEndPointValid !ValidLocalEndPointState
      | LocalEndPointClosed
      | LocalEndPointFailed
      | LocalEndPointInit

data ValidLocalEndPointState = ValidLocalEndPointState
      { _localEndPointInputChan :: !(Chan LocalEndPointEvent)
      , _localEndPointOutputChan :: !(TMChan Event)
      , _localEndPointThread :: Async ()
      }

{-
data ValidLocalEndPointState = ValidLocalEndPointState
      { _localEndPointChan :: !(TMChan Event)
      , _localEndPointRemoteEndPoint :: !(Map EndPointAddress RemoteEndPoint)
      -- ^ we need it to close connections when host is dead, really we
      -- need to keep list of remote end points only
      }
-}

data ZMQConnection = ZMQConnection
      { connectionRemoteEndPoint :: !RemoteEndPoint
      , connectionReliability    :: !Reliability
--      , connectionLocalEndPoint  :: !LocalEndPoint
      , connectionState :: !(MVar ZMQConnectionState)
      , connectionReady :: !(MVar ())
      }

data ZMQConnectionState
      = ZMQConnectionInit
      | ZMQConnectionValid !ValidZMQConnection
      | ZMQConnectionClosed

data ValidZMQConnection = ValidZMQConnection !Word64

data LocalEndPointEvent
        = LocalEndPointConnectionOpen LocalEndPoint EndPointAddress Reliability
            (MVar (Either (TransportError ConnectErrorCode) Connection))
        | LocalEndPointConnectionClose ZMQConnection
        | LocalEndPointClose

data RemoteEndPoint = RemoteEndPoint
      { remoteEndPointAddress :: !EndPointAddress
      , remoteEndPointThread  :: !(Async ())
      , remoteEndPointState   :: !(MVar RemoteEndPointState)
      }

data RemoteEndPointState
      = RemoteEndPointValid ValidRemoteEndPoint
      | RemoteEndPointClosed
      | RemoteEndPointPending (IORef [ValidRemoteEndPoint -> IO ()])

data ValidRemoteEndPoint = ValidRemoteEndPoint
      { _remoteEndPointChan :: !(Chan (NonEmpty ByteString))
      , _remoteEndPointPendingConnections :: !(Counter ConnectionId ZMQConnection)
      }
-- Counter wrapper

data Counter a b = Counter { counterNext   :: !a
                           , counterValue :: !(Map a b)
                           }

nextElement :: (Enum a, Ord a) => (b -> IO Bool) -> b -> Counter a b -> IO (Counter a b, (a, b))
nextElement t e c = nextElement' t (const e) c

nextElement' :: (Enum a, Ord a) => (b -> IO Bool) -> (a -> b) -> Counter a b -> IO (Counter a b, (a,b))
nextElement' t e c = nextElementM t (return . e) c

nextElementM :: (Enum a, Ord a) => (b -> IO Bool) -> (a -> IO b) -> Counter a b -> IO (Counter a b, (a,b))
nextElementM t me (Counter n m) =
    case n' `M.lookup` m of
      Nothing -> mv >>= \v' -> return (Counter n' (M.insert n' v' m), (n', v'))
      Just v  -> t v >>= \case
        True -> mv >>= \v' -> return (Counter n' (M.insert n' v' m), (n', v'))
        False -> nextElementM t me (Counter n' m)
  where
    n' = succ n
    mv = me n'

nextElementM' :: (Enum a, Ord a) => (b -> IO Bool) -> (a -> IO (b,c)) -> Counter a b -> IO (Counter a b, (a,c))
nextElementM' t me (Counter n m) =
    case n' `M.lookup` m of
      Nothing -> mv >>= \(v',r) -> return (Counter n' (M.insert n' v' m), (n', r))
      Just v  -> t v >>= \case
        True -> mv >>= \(v',r) -> return (Counter n' (M.insert n' v' m), (n', r))
        False -> nextElementM' t me (Counter n' m)
  where
    n' = succ n
    mv = me n'
