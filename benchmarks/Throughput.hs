{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE LambdaCase #-}
import           Control.Applicative
import           Control.Concurrent
      ( forkOS
      , threadDelay
      )
import           Control.Concurrent.MVar
import           Control.Distributed.Process
import           Control.Distributed.Process.Node
import           Control.Monad
      ( void
      , forM_
      , replicateM_
      )
import           Criterion.Types
import           Criterion.Measurement as M
import           Data.Binary
import           Data.ByteString.Char8 ( pack )
import qualified Data.ByteString.Lazy as BSL
import           Data.Typeable
import           Text.Printf

import           System.Environment

import           Network.Transport.ZMQ (TransportAddress(TCP), createTransport, defaultZMQParameters)

data SizedList a = SizedList { size :: Int , _elems :: [a] }
  deriving (Typeable)

instance Binary a => Binary (SizedList a) where
  put (SizedList sz xs) = put sz >> mapM_ put xs
  get = do
    sz <- get
    xs <- getMany sz
    return (SizedList sz xs)

-- Copied from Data.Binary
getMany :: Binary a => Int -> Get [a]
getMany = go []
 where
    go xs 0 = return $! reverse xs
    go xs i = do x <- get
                 x `seq` go (x:xs) (i-1)
{-# INLINE getMany #-}

nats :: Int -> SizedList Int
nats = \n -> SizedList n (aux n)
  where
    aux 0 = []
    aux n = n : aux (n - 1)

counter :: Process ()
counter = go 0
  where
    go :: Int -> Process ()
    go !n =
      receiveWait
        [ match $ \xs   -> go (n + size (xs :: SizedList Int))
        , match $ \them -> send them n >> go 0
        ]

count :: (Int, Int) -> ProcessId -> Process ()
count (packets, sz) them = do
  us <- getSelfPid
  replicateM_ packets $ send them (nats sz)
  send them us
  _ <- expect :: Process Int
  return ()

initialServer :: Process ()
initialServer = do
  us <- getSelfPid
  liftIO $ BSL.writeFile "counter.pid" (encode us)
  counter

initialClient :: (Int, Int) -> Process ()
initialClient n = do
  them <- liftIO $ decode <$> BSL.readFile "counter.pid"
  count n them

main :: IO ()
main = getArgs >>= \case 
  [] -> defaultBenchmark
  [role, host] -> do 
      transport <- createTransport defaultZMQParameters (TCP $ pack host)
      node <- newLocalNode transport initRemoteTable
      case role of
        "SERVER" -> runProcess node initialServer
        "CLIENT" -> fmap read getLine >>= runProcess node . initialClient
        _        -> error "wrong role"
  _ -> error "either call benchmark with [SERVER|CLIENT] host or without arguments"



defaultBenchmark :: IO ()
defaultBenchmark = do
  -- server
  void . forkOS $ do
    transport <- createTransport defaultZMQParameters (TCP "127.0.0.1")
    node <- newLocalNode transport initRemoteTable
    runProcess node $ initialServer
  
  threadDelay 1000000
  e <- newEmptyMVar
  void . forkOS $ do
    putStrLn "packet size  time\n---          ---\n"
    forM_ [1,10,100,200,600,800,1000,2000,4000] $ \i -> do
        transport <- createTransport defaultZMQParameters (TCP "127.0.0.1")
        node <- newLocalNode transport initRemoteTable
        d <- snd <$> M.measure (nfIO $ runProcess node $ initialClient (1000,i)) 1
        printf "%-8i %10.4f\n" i d
    putMVar e ()
  takeMVar e
