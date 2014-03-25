{-# LANGUAGE OverloadedStrings #-}
module System.ZMQ4.Utils
  ( bindFromRangeRandom
  , bindFromRangeRandomM
  , authManager
  , closeZeroLinger
  )
  where

import Control.Monad ( forever )
import Control.Monad.Catch
import Control.Concurrent.Async
import Data.ByteString
import Data.List.NonEmpty
import System.Random ( randomRIO )
import Text.Printf


import           System.ZMQ4.Monadic
      ( ZMQ 
      )
import qualified System.ZMQ4.Monadic as M
import           System.ZMQ4
      ( Context
      , errno
      )
import qualified System.ZMQ4         as P

-- | Bind socket to the random port in a given range.
bindFromRangeRandomM :: M.Socket z t
                     -> String -- ^ Address
                     -> Int    -- ^ Min port
                     -> Int    -- ^ Max port
                     -> Int    -- ^ Max tries
                     -> ZMQ z Int
bindFromRangeRandomM sock addr mI mA tr = go tr
    where
      go 0 = error "!" -- XXX: throw correct error
      go x = do
        port   <- M.liftIO $ randomRIO (mI,mA)
        result <- try $ M.bind sock (printf "%s:%i" addr port)
        case result of
            Left e
              | errno e == -1 -> go (x - 1)
              | otherwise -> throwM e
            Right () -> return port

bindFromRangeRandom :: P.Socket t
                    -> String -- ^ Address
                    -> Int    -- ^ Min port
                    -> Int    -- ^ Max port
                    -> Int    -- ^ Max tries
                    -> IO Int
bindFromRangeRandom sock addr mI mA tr = go tr
    where
      go 0 = error "!" -- XXX: throw correct error
      go x = do
        port   <- randomRIO (mI,mA)
        result <- try $ P.bind sock (printf "%s:%i" addr port)
        case result of
            Left e
              | errno e == -1 -> go (x - 1)
              | otherwise -> throwM e
            Right () -> return port

-- | One possible password authentification
authManager :: P.Context -> ByteString -> ByteString -> IO (Async ())
authManager ctx user pass = do
    req <- P.socket ctx P.Rep
    P.bind req "inproc://zeromq.zap.01"
    async $ forever $ do
      ("1.0":requestId:domain:ipAddress:identity:mech:xs) <- P.receiveMulti req
      case mech of
        "PLAIN" -> case xs of
           (pass':user':_)
             | user == user' && pass == pass' -> do
                P.sendMulti req $ "1.0" :| [requestId, "200", "OK", "", ""]
             | otherwise -> P.sendMulti req $ "1.0" :| [requestId, "400", "Credentials are not implemented", "", ""]
           _ -> P.sendMulti req $ "1.0" :| [requestId, "500", "Method not implemented", "", ""]
        _ -> P.sendMulti req $ "1.0" :| [requestId, "500", "Method not implemented", "", ""]


-- | Close socket immideately.
closeZeroLinger :: P.Socket a -> IO ()
closeZeroLinger sock = do
  P.setLinger (P.restrict 0) sock
  P.close sock
