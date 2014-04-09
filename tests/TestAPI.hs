{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Concurrent 
import Control.Exception
import Control.Monad ( (<=<), replicateM )

import Data.IORef

import Network.Transport
import Network.Transport.ZMQ
import System.Exit

main :: IO ()
main = finish <=< trySome $ do
    putStr "simple: "
    Right (zmq, transport) <- createTransportEx defaultZMQParameters "127.0.0.1"
    Right ep1 <- newEndPoint transport
    Right ep2 <- newEndPoint transport
    Right c1  <- connect ep1 (address ep2) ReliableOrdered defaultConnectHints
    Right c2  <- connect ep2 (address ep1) ReliableOrdered defaultConnectHints
    Right _   <- send c1 ["123"]
    Right _   <- send c2 ["321"]
    close c1
    close c2
    [ConnectionOpened 1 ReliableOrdered _, Received 1 ["321"], ConnectionClosed 1] <- replicateM 3 $ receive ep1
    [ConnectionOpened 1 ReliableOrdered _, Received 1 ["123"], ConnectionClosed 1] <- replicateM 3 $ receive ep2
    putStrLn "OK"

    putStr "connection break: "
    Right ep3 <- newEndPoint transport

    Right c21  <- connect ep1 (address ep2) ReliableOrdered defaultConnectHints
    Right c22  <- connect ep2 (address ep1) ReliableOrdered defaultConnectHints
    Right c23  <- connect ep3 (address ep1) ReliableOrdered defaultConnectHints

    ConnectionOpened 2 ReliableOrdered _ <- receive ep2
    ConnectionOpened 2 ReliableOrdered _ <- receive ep1
    ConnectionOpened 3 ReliableOrdered _ <- receive ep1

    breakConnection zmq (address ep1) (address ep2)

    ErrorEvent (TransportError (EventConnectionLost _ ) _) <- receive $ ep1
    ErrorEvent (TransportError (EventConnectionLost _ ) _) <- receive $ ep2

    Left (TransportError SendFailed _) <- send c21 ["test"]
    Left (TransportError SendFailed _) <- send c22 ["test"]

    Left (TransportError SendFailed _) <- send c23 ["test"]
    ErrorEvent (TransportError (EventConnectionLost _) _ ) <- receive ep1
    Right c24 <- connect ep3 (address ep1) ReliableOrdered defaultConnectHints
    Right ()  <- send c24 ["final"]
    ConnectionOpened 4 ReliableOrdered _ <- receive ep1
    Received 4 ["final"] <- receive ep1
    putStrLn "OK"
    multicast transport
    putStr "Register cleanup test:"
    Right (zmq1, transport1) <- createTransportEx defaultZMQParameters "127.0.0.1"
    x <- newIORef 0
    Just _ <- registerCleanupAction zmq1 (modifyIORef x (+1))
    Just u <- registerCleanupAction zmq1 (modifyIORef x (+1))
    applyCleanupAction zmq u
    closeTransport transport1
    2 <- readIORef x
    putStrLn "OK"
  where
      multicast transport = do 
        Right ep1 <- newEndPoint transport
        Right ep2 <- newEndPoint transport
        putStr "multicast: "
        Right g1 <- newMulticastGroup ep1
        multicastSubscribe g1
        threadDelay 1000000
        multicastSend g1 ["test"]
        ReceivedMulticast _ ["test"] <- receive ep1
        Right g2 <- resolveMulticastGroup ep2 (multicastAddress g1)
        multicastSubscribe g2
        threadDelay 100000
        multicastSend g2 ["test-2"]
        ReceivedMulticast _ ["test-2"] <- receive ep2
        ReceivedMulticast _ ["test-2"] <- receive ep1
        putStrLn "OK"
        
        putStr "Auth:"
        Right tr2 <- createTransport
                       defaultZMQParameters {authMethod=Just $ AuthPlain "user" "password"}
                       "127.0.0.1"
        Right ep3 <- newEndPoint tr2
        Right ep4 <- newEndPoint tr2
        Right c3  <- connect ep3 (address ep4) ReliableOrdered defaultConnectHints
        Right _   <- send c3 ["4456"]
        [ConnectionOpened 1 ReliableOrdered _, Received 1 ["4456"]] <- replicateM 2 $ receive ep4
        Right c4  <- connect ep3 (address ep4) ReliableOrdered defaultConnectHints
        Right _   <- send c4 ["5567"]
        [ConnectionOpened 2 ReliableOrdered _, Received 2 ["5567"]] <- replicateM 2 $ receive ep4
        putStrLn "OK"

        putStr "Connect to non existing host: "
        Left (TransportError ConnectFailed _) <- connect ep3 (EndPointAddress "tcp://128.0.0.1:7689") ReliableOrdered defaultConnectHints
        putStrLn "OK"
      finish (Left e) = print e >> exitWith (ExitFailure 1)
      finish Right{} = exitWith ExitSuccess


trySome :: IO a -> IO (Either SomeException a)
trySome = try

forkTry :: IO () -> IO ThreadId
forkTry = forkIO
