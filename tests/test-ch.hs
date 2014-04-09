{-# LANGUAGE OverloadedStrings #-}
-- |
-- Copyright: (c) 2014, EURL Tweag
-- License: BSD3
--
-- Runner for given CH test suite. The test suite to run is given by the
-- expansion of the @TEST_SUITE_MODULE@ macro.

module Main where

import TEST_SUITE_MODULE (tests)

import Network.Transport.Test (TestTransport(..))
import Network.Transport.ZMQ
  ( createTransportEx
  , defaultZMQParameters
  , breakConnection
  )
import Test.Framework (defaultMain)

main :: IO ()
main = do
    Right (zmqt, transport) <- createTransportEx defaultZMQParameters "127.0.0.1"
    defaultMain =<< tests TestTransport
      { testTransport = transport
      , testBreakConnection = breakConnection zmqt
      }
