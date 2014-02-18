module System.ZMQ4.Utils
  ( bindFromRangeRandom
  , bindFromRangeRandomM
  )
  where

import Control.Monad.Catch
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
