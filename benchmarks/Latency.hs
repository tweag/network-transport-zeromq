import System.Environment
import Control.Monad
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Network.Transport.ZMQ (createTransport, defaultZMQParameters)
import Data.Binary (encode, decode)
import Data.ByteString.Char8 (pack)
import qualified Data.ByteString.Lazy as BSL

pingServer :: Process ()
pingServer = forever $ do
  them <- expect
  send them ()

pingClient :: Int -> ProcessId -> Process ()
pingClient n them = do
  us <- getSelfPid
  replicateM_ n $ send them us >> (expect :: Process ())
  liftIO . putStrLn $ "Did " ++ show n ++ " pings"

initialProcess :: String -> Process ()
initialProcess "SERVER" = do
  us <- getSelfPid
  liftIO $ BSL.writeFile "pingServer.pid" (encode us)
  pingServer
initialProcess "CLIENT" = do
  n <- liftIO $ getLine
  them <- liftIO $ decode <$> BSL.readFile "pingServer.pid"
  pingClient (read n) them

main :: IO ()
main = do
  [role, host] <- getArgs
  Right transport <- createTransport defaultZMQParameters (pack host)
  node <- newLocalNode transport initRemoteTable
  runProcess node $ initialProcess role
