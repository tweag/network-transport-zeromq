{-# LANGUAGE OverloadedStrings, LambdaCase #-}
module Latency where

import Control.Applicative
import Control.Monad (void, forM_, forever, replicateM_)
import Control.Concurrent (forkOS, threadDelay)
import Control.Concurrent.MVar
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Criterion.Measurement
import Data.Binary (encode, decode)
import Data.ByteString.Char8 (pack)
import qualified Data.ByteString.Lazy as BSL
import Network.Transport.ZMQ (createTransport, defaultZMQParameters)
import System.Environment
import Text.Printf

pingServer :: Process ()
pingServer = forever $ do
  them <- expect
  send them ()

pingClient :: Int -> ProcessId -> Process ()
pingClient n them = do
  us <- getSelfPid
  replicateM_ n $ send them us >> (expect :: Process ())

initialServer :: Process ()
initialServer = do
  us <- getSelfPid
  liftIO $ BSL.writeFile "pingServer.pid" (encode us)
  pingServer

initialClient :: Int -> Process ()
initialClient n = do
  them <- liftIO $ decode <$> BSL.readFile "pingServer.pid"
  pingClient n them

main :: IO ()
main = getArgs >>= \case
  [] -> defaultBenchmark
  [role, host] -> do
    Right transport <- createTransport defaultZMQParameters (pack host)
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
    Right transport <- createTransport defaultZMQParameters "127.0.0.1"
    node <- newLocalNode transport initRemoteTable
    runProcess node $ initialServer
  
  threadDelay 1000000
  e <- newEmptyMVar
  void . forkOS $ do
    putStrLn "pings        time\n---          ---\n"
    forM_ [100,200,600,800,1000,2000,5000,8000,10000] $ \i -> do
        Right transport <- createTransport defaultZMQParameters "127.0.0.1"
        node <- newLocalNode transport initRemoteTable
        d <- time_ (runProcess node $ initialClient i)
        printf "%-8i %10.4f\n" i d
    putMVar e ()
  takeMVar e
