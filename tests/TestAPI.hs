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
    Right (zmq, transport) <- createTransportEx defaultZMQParameters "127.0.0.1"
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
    -- Right ep1 <- newEndPoint transport
    -- Right ep2 <- newEndPoint transport
    breakConnection zmq (address ep1) (address ep2)
    -- closeEndPoint ep2
    -- Left _  <- connect ep1 (address ep2) ReliableOrdered defaultConnectHints
    print <=< receive $ ep1
    print <=< receive $ ep2
    return ()
    --
    Right tr2 <- createTransport
                   defaultZMQParameters {authorizationType=ZMQAuthPlain "user" "password"}
                   "127.0.0.1"
    Right ep3 <- newEndPoint tr2
    Right ep4 <- newEndPoint tr2
    Right c3  <- connect ep3 (address ep4) ReliableOrdered defaultConnectHints
    Right _   <- send c3 ["4456"]
    print <=< replicateM 2 $ receive ep4
    Right c4  <- connect ep3 (address ep4) ReliableOrdered defaultConnectHints
    Right _   <- send c4 ["5567"]
    print <=< replicateM 2 $ receive ep4
  where
    finish (Left e) = print e >> exitWith (ExitFailure 1)
    finish Right{} = exitWith ExitSuccess

trySome :: IO a -> IO (Either SomeException a)
trySome = try

forkTry :: IO () -> IO ThreadId
forkTry = forkIO
