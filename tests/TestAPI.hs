{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Arrow
import Control.Concurrent
import Control.Monad ( replicateM )

import Data.IORef

import Network.Transport
import Network.Transport.ZMQ
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = defaultMain $
  testGroup "API tests" $
    map (uncurry testGroup . second (\x -> map ($ x) tests)) addrs
    ++
    [ testCase "test multicast" $ test_multicast (TCP "127.0.0.1")
    ]
  where
    addrs = [ ("TCP", TCP "127.0.0.1")
            , ("IPC", IPC "/tmp/" "zeromqXXX.ipc")
            ]
    tests :: [TransportAddress -> TestTree]
    tests =
      [ testCase "simple"  . test_simple
      , testCase "connection break" . test_connectionBreak
      , testCase "authentification" . test_auth
      , testCase "connect to non existent host" . test_nonexists
      , testCase "test cleanup actions" . test_cleanup
      ]

test_simple :: TransportAddress -> IO ()
test_simple addr = do
    transport <- createTransport defaultZMQParameters addr
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
    closeTransport transport

test_connectionBreak :: TransportAddress -> IO ()
test_connectionBreak addr = do
    (zmq, transport) <-
      createTransportExposeInternals defaultZMQParameters addr
    Right ep1 <- newEndPoint transport
    Right ep2 <- newEndPoint transport
    Right ep3 <- newEndPoint transport

    Right c21  <- connect ep1 (address ep2) ReliableOrdered defaultConnectHints
    Right c22  <- connect ep2 (address ep1) ReliableOrdered defaultConnectHints
    Right c23  <- connect ep3 (address ep1) ReliableOrdered defaultConnectHints

    ConnectionOpened 1 ReliableOrdered _ <- receive ep2
    ConnectionOpened 1 ReliableOrdered _ <- receive ep1
    ConnectionOpened 2 ReliableOrdered _ <- receive ep1

    breakConnection zmq (address ep1) (address ep2)

    ErrorEvent (TransportError (EventConnectionLost _ ) _) <- receive $ ep1
    ErrorEvent (TransportError (EventConnectionLost _ ) _) <- receive $ ep2

    Left (TransportError SendFailed _) <- send c21 ["test"]
    Left (TransportError SendFailed _) <- send c22 ["test"]

    Left (TransportError SendFailed _) <- send c23 ["test"]
    ErrorEvent (TransportError (EventConnectionLost _) _ ) <- receive ep1
    Right c24 <- connect ep3 (address ep1) ReliableOrdered defaultConnectHints
    Right ()  <- send c24 ["final"]
    ConnectionOpened 3 ReliableOrdered _ <- receive ep1
    Received 3 ["final"] <- receive ep1
    closeTransport transport

test_multicast :: TransportAddress -> IO ()
test_multicast addr = do
    transport <- createTransport defaultZMQParameters addr
    Right ep1 <- newEndPoint transport
    Right ep2 <- newEndPoint transport
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
    return ()

test_auth :: TransportAddress -> IO ()
test_auth addr = do
    tr2 <-
      createTransport defaultZMQParameters{ zmqSecurityMechanism =
                                              Just $ SecurityPlain "user" "password" }
                      addr
    Right ep3 <- newEndPoint tr2
    Right ep4 <- newEndPoint tr2
    Right c3  <- connect ep3 (address ep4) ReliableOrdered defaultConnectHints
    Right _   <- send c3 ["4456"]
    [ConnectionOpened 1 ReliableOrdered _, Received 1 ["4456"]] <- replicateM 2 $ receive ep4
    Right c4  <- connect ep3 (address ep4) ReliableOrdered defaultConnectHints
    Right _   <- send c4 ["5567"]
    [ConnectionOpened 2 ReliableOrdered _, Received 2 ["5567"]] <- replicateM 2 $ receive ep4
    return ()

test_nonexists :: TransportAddress -> IO ()
test_nonexists addr = do
    tr <- createTransport defaultZMQParameters addr
    Right ep <- newEndPoint tr
    Left (TransportError ConnectFailed _) <- connect ep (EndPointAddress "tcp://129.0.0.1:7684") ReliableOrdered defaultConnectHints
    closeTransport tr

test_cleanup :: TransportAddress -> IO ()
test_cleanup addr = do
    (zmq, transport) <-
      createTransportExposeInternals defaultZMQParameters addr
    x <- newIORef (0::Int)
    Just _ <- registerCleanupAction zmq (modifyIORef x (+1))
    Just u <- registerCleanupAction zmq (modifyIORef x (+1))
    applyCleanupAction zmq u
    closeTransport transport
    2 <- readIORef x
    return ()

test_prior :: IO ()
test_prior = do
    (zmqa, _) <- createTransportExposeInternals defaultZMQParameters "127.0.0.1"
    Right epa <- apiNewEndPoint defaultHints{hintsPort=Just 8888} zmqa

    (_, transportb) <-
          createTransportExposeInternals defaultZMQParameters "127.0.0.1"
    Right epb <- newEndPoint transportb
    Right _   <- connect epb (EndPointAddress "tcp://127.0.0.1:8888") ReliableOrdered defaultConnectHints
    (ConnectionOpened 1 ReliableOrdered _) <- receive epa
    return ()
   
