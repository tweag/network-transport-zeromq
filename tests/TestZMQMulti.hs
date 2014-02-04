{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Applicative ((<$>))

import Network.Transport
import Network.Transport.ZeroMQ
import Network.Transport.Tests.MultiTransport
import Network.Transport.Tests.Auxiliary (runTests)

main :: IO ()
main = testTransport' 

testTransport' :: IO ()
testTransport' = do
  Right transport1 <- newTransport "127.0.0.1" "8082"
  Right transport2 <- newTransport "127.0.0.1" "8083"
  runTests
    [ ("PingPong",              testPingPong transport1 transport2 numPings)
    , ("EndPoints",             testEndPoints transport1 transport2 numPings)
    , ("Connections",           testConnections transport1 transport2 numPings)
    , ("CloseOneConnection",    testCloseOneConnection transport1 transport2 numPings)
    , ("CloseOneDirection",     testCloseOneDirection transport1 transport2 numPings)
    , ("CloseReopen",           testCloseReopen transport1 transport2 numPings)
--    , ("ParallelConnects",      testParallelConnects transport numPings)
--    , ("SendAfterClose",        testSendAfterClose transport 100)
--    , ("Crossing",              testCrossing transport 10)
--    , ("CloseTwice",            testCloseTwice transport 100)
--    , ("ConnectToSelf",         testConnectToSelf transport numPings)
--    , ("ConnectToSelfTwice",    testConnectToSelfTwice transport numPings)
--    , ("CloseSelf",             testCloseSelf newTransport)
--    , ("CloseEndPoint",         testCloseEndPoint transport numPings)
--    , ("CloseTransport",        testCloseTransport newTransport)
--    , ("ConnectClosedEndPoint", testConnectClosedEndPoint transport)
--    , ("ExceptionOnReceive",    testExceptionOnReceive newTransport)
--    , ("SendException",         testSendException newTransport)
--    , ("Kill",                  testKill newTransport 1000)
    ]
  where
    numPings = 1000 :: Int
    newTransport h p = (either (Left . show) (Right) <$> createTransport defaultZeroMQParameters h p)


