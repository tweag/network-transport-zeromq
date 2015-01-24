{-# LANGUAGE LambdaCase, OverloadedStrings #-}
module Main where

-- | Like Latency, but creating lots of channels

import System.Environment
import Control.Applicative
import Control.Monad (void, forM_, forever, replicateM_)
import Control.Concurrent.MVar
import Control.Concurrent (forkOS, threadDelay)
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Criterion.Types
import Criterion.Measurement as M
import Data.Binary (encode, decode)
import Data.ByteString.Char8 (pack)
import Network.Transport.ZMQ (createTransport, defaultZMQParameters)
import qualified Data.ByteString.Lazy as BSL
import Text.Printf

pingServer :: Process ()
pingServer = forever $ do
  them <- expect
  sendChan them ()
  -- TODO: should this be automatic?
  reconnectPort them

pingClient :: Int -> ProcessId -> Process ()
pingClient n them = do
  replicateM_ n $ do
    (sc, rc) <- newChan :: Process (SendPort (), ReceivePort ())
    send them sc
    receiveChan rc

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
main = do
    initializeTime
    getArgs >>= \case
      [] -> defaultBench 
      [role, host] -> do
         transport <- createTransport defaultZMQParameters (pack host)
         node <- newLocalNode transport initRemoteTable
         case role of
           "SERVER" -> runProcess node initialServer
           "CLIENT" -> fmap read getLine >>= runProcess node .  initialClient
           _       -> error "Role should be either SERVER or CLIENT"
      _ -> error "either call benchmark with [SERVER|CLIENT] host or without arguments"
  where
    defaultBench = do
      void . forkOS $ do
        transport <- createTransport defaultZMQParameters "127.0.0.1"
        node <- newLocalNode transport initRemoteTable
        runProcess node $ initialServer
      threadDelay 1000000
      e <- newEmptyMVar
      void . forkOS $ do
        putStrLn "pings        time\n---          ---\n"
        forM_ [100,200,600,800,1000,2000,5000,8000,10000] $ \i -> do
            transport <- createTransport defaultZMQParameters "127.0.0.1"
            node <- newLocalNode transport initRemoteTable
            d <- snd <$> M.measure (nfIO $ runProcess node $ initialClient i) 1
            printf "%-8i %10.4f\n" i d
        putMVar e ()
      takeMVar e
