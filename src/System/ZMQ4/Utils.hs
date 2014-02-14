module System.ZMQ4.Utils
  ( bindFromRangeRandom
  )
  where

import Control.Monad.Catch
import System.Random ( randomRIO )
import Text.Printf

import System.ZMQ4.Monadic

-- | Bind socket to the random port in a given range.
bindFromRangeRandom :: Socket z t
                    -> String -- ^ Address
                    -> Int    -- ^ Min port
                    -> Int    -- ^ Max port
                    -> Int    -- ^ Max tries
                    -> ZMQ z Int
bindFromRangeRandom sock addr mI mA tr = go tr
    where
      go 0 = error "!" -- XXX: throw correct error
      go x = do
        port   <- liftIO $ randomRIO (mI,mA)
        result <- try $ bind sock (printf "%s:%i" addr port)
        case result of
            Left e
              | errno e == -1 -> go (x - 1)
              | otherwise -> throwM e
            Right () -> return port
