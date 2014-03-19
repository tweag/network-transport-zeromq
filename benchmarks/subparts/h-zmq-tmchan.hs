{-# LANGUAGE CPP, BangPatterns #-}

module Main where

import Control.Monad

import Data.Int
import System.Environment (getArgs, withArgs)
import Data.Time (getCurrentTime, diffUTCTime, NominalDiffTime)
import System.IO (withFile, IOMode(..), hPutStrLn, Handle, stderr)
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Debug.Trace
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack, unpack)
import qualified Data.ByteString as BS
import qualified Network.Socket.ByteString as NBS
import Data.Time (getCurrentTime, diffUTCTime, NominalDiffTime)
import qualified Data.ByteString as BS
import Data.Time (getCurrentTime, diffUTCTime, NominalDiffTime)
import System.ZMQ4 (Socket, Push(..), Pull(..),socket, connect, bind, context)
import qualified System.ZMQ4 as ZMQ
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TMChan (TMChan, newTMChan, readTMChan, writeTMChan)

main = do
  [pingsStr]  <- getArgs
  clientDone  <- newEmptyMVar

  ctx <- context
  -- Start the server
  forkIO $ do
    putStrLn "server: creating TCP connection"
    push <- socket ctx Push
    connect push "tcp://127.0.0.1:8080"
    putStrLn "server: awaiting client connection"
    pull <- socket ctx Pull
    bind pull "tcp://*:8081"
    putStrLn "server: listening for pings"

    -- Set up multiplexing channel
    multiplexChannel <- atomically $ newTMChan

    -- Wait for incoming connections (pings from the client)
    forkIO $ socketToChan pull multiplexChannel

    -- Reply to the client
    forever $ atomically (readTMChan multiplexChannel) >>= \(Just x) -> send push x

  -- Start the client
  forkIO $ do
    let pings = read pingsStr
    push <- socket ctx Push
    connect push "tcp://127.0.0.1:8081"
    putStrLn "server: awaiting client connection"
    pull <- socket ctx Pull
    bind pull "tcp://*:8080"
    ping pull push pings
    putMVar clientDone ()

  -- Wait for the client to finish
  takeMVar clientDone

socketToChan :: Socket Pull -> TMChan ByteString -> IO ()
socketToChan sock chan = go
  where
    go = do bs <- recv sock
            when (BS.length bs > 0) $ do
               atomically $ writeTMChan chan bs
               go

pingMessage :: ByteString
pingMessage = pack "ping123"

ping :: Socket Pull -> Socket Push -> Int -> IO ()
ping pull push pings = go pings
  where
    go :: Int -> IO ()
    go 0 = do
      putStrLn $ "client did " ++ show pings ++ " pings"
    go !i = do
      before <- getCurrentTime
      send push pingMessage
      bs <- recv pull
      after <- getCurrentTime
      -- putStrLn $ "client received " ++ unpack bs
      let latency = (1e6 :: Double) * realToFrac (diffUTCTime after before)
      hPutStrLn stderr $ show i ++ " " ++ show latency
      go (i - 1)

-- | Receive a package
recv :: Socket Pull -> IO ByteString
recv = ZMQ.receive

-- | Send a package
send :: Socket Push -> ByteString -> IO ()
send sock bs = ZMQ.send sock [] bs
