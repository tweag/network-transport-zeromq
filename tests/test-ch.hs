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
  )
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TMChan
import qualified Data.Map as Map
import qualified Data.ByteString.Char8 as B
import Network.Transport
import Network.Transport.ZMQ.Types
import Test.Framework (defaultMain)

main :: IO ()
main = do
    Right (zmqt, transport) <- createTransportEx defaultZMQParameters "127.0.0.1"
    defaultMain =<< tests TestTransport
      { testTransport = transport
      , testBreakConnection = breakConnection zmqt
      }

breakConnection transport from to = do
  withMVar (_transportState transport) $ \x -> case x of
    TransportValid v -> 
      let b = c   `Map.lookup` _transportEndPoints v
          c = read $ B.unpack . snd $ B.spanEnd (/= '.') $ endPointAddressToByteString to
      in case b of
          Nothing -> return ()
          Just ep -> withMVar (localEndPointState ep) $ \y -> case y of
            LocalEndPointValid w -> do
              atomically $ writeTMChan (_localEndPointChan w)
                         $ ErrorEvent $ TransportError (EventConnectionLost from) "FakeConnectionLost"      
    _ -> return ()
