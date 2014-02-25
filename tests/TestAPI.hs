{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Concurrent 
import Control.Exception
import Control.Monad ( (<=<), replicateM )

import Network.Transport
import Network.Transport.ZMQ
import System.Exit

main :: IO ()
main = finish <=< trySome $ do
    Right transport <- createTransport defaultZMQParameters "127.0.0.1"
    Right ep1 <- newEndPoint transport
    Right ep2 <- newEndPoint transport
    Right c1  <- connect ep1 (address ep2) ReliableOrdered defaultConnectHints
    Right c2  <- connect ep2 (address ep1) ReliableOrdered defaultConnectHints
    Right _   <- send c1 ["123"]
    Right _   <- send c2 ["321"]
    close c1
    close c2
    m1 <- replicateM 3 $ receive ep1
    m2 <- replicateM 3 $ receive ep2
    print m1
    print m2
    Right ep1 <- newEndPoint transport
    Right ep2 <- newEndPoint transport
    closeEndPoint ep2
    Right c1  <- connect ep1 (address ep2) ReliableOrdered defaultConnectHints
    x <- send c1 ["123"]
    print x
    return ()
  where
    finish Left{} = exitWith $ ExitFailure 1
    finish Right{} = exitWith ExitSuccess

trySome :: IO a -> IO (Either SomeException a)
trySome = try

forkTry :: IO () -> IO ThreadId
forkTry = forkIO
