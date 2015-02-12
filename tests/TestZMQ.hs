{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Applicative ((<$>))

import Network.Transport
import Network.Transport.ZMQ
import Network.Transport.Tests
import Network.Transport.Tests.Auxiliary (runTests)

main :: IO ()
main = do
  testTransport' (Right <$> createTransport defaultZMQParameters (TCP "127.0.0.1"))
  testTransport' (Right <$> createTransport defaultZMQParameters (IPC "/tmp/" "zeromqXXX.ipc"))

testTransport' :: IO (Either String Transport) -> IO ()
testTransport' newTransport = do
  Right transport <- newTransport
  runTests
    [ ("PingPong",              testPingPong transport numPings)
    , ("EndPoints",             testEndPoints transport numPings)
    , ("Connections",           testConnections transport numPings)
    , ("CloseOneConnection",    testCloseOneConnection transport numPings)
    , ("CloseOneDirection",     testCloseOneDirection transport numPings)
    , ("CloseReopen",           testCloseReopen transport numPings)
    , ("ParallelConnects",      testParallelConnects transport 10)
    , ("SendAfterClose",        testSendAfterClose transport 100)
    , ("Crossing",              testCrossing transport 10)
    , ("CloseTwice",            testCloseTwice transport 100)
    , ("ConnectToSelf",         testConnectToSelf transport numPings)
    , ("ConnectToSelfTwice",    testConnectToSelfTwice transport numPings)
    , ("CloseSelf",             testCloseSelf newTransport)
    , ("CloseEndPoint",         testCloseEndPoint transport numPings)
    , ("CloseTransport",        testCloseTransport newTransport)
    , ("ExceptionOnReceive",    testExceptionOnReceive newTransport)
    , ("SendException",         testSendException newTransport)
    , ("Kill",                  testKill newTransport 80)
                                -- testKill test have a timeconstraint so n-t-zmq
                                -- fails to work with required speed, we need to
                                -- reduce a number of tests here
    ]
  where
    numPings = 500 :: Int
