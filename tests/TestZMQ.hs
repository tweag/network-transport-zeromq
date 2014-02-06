{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Applicative ((<$>))

import Network.Transport
import Network.Transport.ZeroMQ
import Network.Transport.Tests
import Network.Transport.Tests.Auxiliary (runTests)

main :: IO ()
main = testTransport' (either (Left . show) (Right) <$> createTransport defaultZeroMQParameters "127.0.0.1" "3541")

testTransport' :: IO (Either String Transport) -> IO ()
testTransport' newTransport = do
  Right transport <- newTransport
  runTests
    [ ("PingPong",              testPingPong transport numPings)
    , ("EndPoints",             testEndPoints transport numPings)
    , ("Connections",           testConnections transport numPings)
    , ("CloseOneConnection",    testCloseOneConnection transport numPings)
    , ("CloseOneDirection",     testCloseOneDirection transport numPings)
--    , ("CloseReopen",           testCloseReopen transport numPings)
--    , ("ParallelConnects",      testParallelConnects transport numPings)
--    , ("SendAfterClose",        testSendAfterClose transport 100)
--    , ("Crossing",              testCrossing transport 10)
--    , ("CloseTwice",            testCloseTwice transport 100)
    , ("ConnectToSelf",         testConnectToSelf transport numPings)
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
    numPings = 10000 :: Int

