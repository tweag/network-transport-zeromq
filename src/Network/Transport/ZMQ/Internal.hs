-- |
-- Copyright: (C) 2014 EURL Tweag
--

{-# LANGUAGE OverloadedStrings #-}
module Network.Transport.ZMQ.Internal
  ( bindRandomPort
  , authManager
  , closeZeroLinger
  )
  where

import           Control.Monad ( forever )
import           Control.Concurrent.Async
import           Data.ByteString ( ByteString )
import           Data.List.NonEmpty ( NonEmpty(..) )

import           System.ZMQ4

-- | Bind socket to the random port.
bindRandomPort :: Socket t
               -> String -- ^ Address
               -> IO Int
bindRandomPort sock addr = do
    bind sock $ "tcp://"++addr++":0"
    fmap (read . last . split (/=':')) $ lastEndpoint sock

-- | One possible password authentification
authManager :: Context -> ByteString -> ByteString -> IO (Async ())
authManager ctx user pass = do
    req <- socket ctx Rep
    bind req "inproc://zeromq.zap.01"
    async $ forever $ do
      ("1.0":requestId:_domain:_ipAddress:_identity:mech:xs) <- receiveMulti req
      case mech of
        "PLAIN" -> case xs of
           (pass':user':_)
             | user == user' && pass == pass' -> do
                sendMulti req $ "1.0" :| [requestId, "200", "OK", "", ""]
             | otherwise -> sendMulti req $ "1.0" :| [requestId, "400", "Credentials are not implemented", "", ""]
           _ -> sendMulti req $ "1.0" :| [requestId, "500", "Method not implemented", "", ""]
        _ -> sendMulti req $ "1.0" :| [requestId, "500", "Method not implemented", "", ""]


-- | Close socket immideately.
closeZeroLinger :: Socket a -> IO ()
closeZeroLinger sock = do
  setLinger (restrict (0::Int)) sock
  close sock

split :: (Char -> Bool) -> String -> [String]
split f = go
  where
    go [] = []
    go s  = case span f s of
                (p,[]) -> [p]
                (p,_:ss) -> p:go ss
