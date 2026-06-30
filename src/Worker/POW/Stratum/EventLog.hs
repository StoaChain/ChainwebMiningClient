{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Worker.POW.Stratum.EventLog
-- License: MIT
--
-- The non-blocking, append-only NDJSON event-log writer for the StoaChain 2.0.0
-- accounting sidecar. The stratum share-accept site hands each 'PoolEvent' to a
-- 'PoolEventSink'; the sink enqueues it on a bounded queue and a background
-- writer thread appends it to the shared-volume file the sidecar tails.
--
-- Money-path / availability invariants:
--
--   * NON-BLOCKING: 'emit' uses a bounded 'TBQueue' with @tryWriteTBQueue@ — it
--     NEVER blocks the mining/stratum path. If the queue is full (writer slow /
--     consumer absent) the event is DROPPED and a counter is bumped; mining is
--     never stalled by accounting (the prod-failure constraint).
--   * DROP IS OBSERVABLE: the writer periodically logs the dropped count (the
--     operator-visible signal) so silent share loss cannot accumulate unnoticed.
--   * WRITE FAILURE IS NON-FATAL: an 'IOException' on append/rotate is logged and
--     the writer re-opens + continues; a bad volume never crashes the engine.
--   * BOUNDED ON DISK: the log rotates at a size threshold (rename to @<path>.1@,
--     overwriting the previous backup, then a fresh file) so it cannot grow
--     unbounded. The rename yields a NEW inode, which the sidecar's offset store
--     classifies as @rotated-or-recreated@ and resets to the new file's start.
--
-- Disabled mode: when no path is configured, 'withEventLog' supplies 'noopSink'
-- and starts no thread — the engine behaves byte-identically to the un-patched
-- solo binary (the emit is the only added side effect, and it is a no-op).
--
module Worker.POW.Stratum.EventLog
( PoolEventSink
, noopSink
, withEventLog
) where

import Control.Concurrent.Async (withAsync, link)
import Control.Concurrent.STM
import Control.Concurrent.STM.TBQueue (tryWriteTBQueue)
import Control.Exception (SomeException, try)
import Control.Monad (when)
import Data.Int (Int64)
import Data.IORef
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text as T
import Numeric.Natural (Natural)
import qualified System.IO as IO
import System.Directory (doesFileExist, getFileSize, renameFile)

import qualified System.LogLevel as L

import Logger
import Utils (sshow)
import Worker.POW.Stratum.PoolEvent

-- | A sink the stratum accept site calls for each pool event. Total + non-blocking.
type PoolEventSink = PoolEvent -> IO ()

-- | A sink that drops everything — used when the event log is disabled.
noopSink :: PoolEventSink
noopSink _ = return ()

-- | Bounded in-memory queue capacity. At a few accepted shares/sec across the
-- fleet this is never reached in practice; if it is (a stalled writer), events
-- are dropped (counted + logged) rather than blocking mining.
queueCapacity :: Natural
queueCapacity = 65536

-- | Rotate the active log once it reaches this many bytes. One @.1@ backup is
-- kept (overwritten on the next rotation), so on-disk size is bounded at ~2x
-- this. Large enough that the always-caught-up sidecar has consumed the file
-- long before a rotation drops it.
rotateBytes :: Int64
rotateBytes = 256 * 1024 * 1024

-- | How often (in events written) to flush the dropped-count operator signal.
dropLogEvery :: Int
dropLogEvery = 1000

-- | Run @inner@ with a live event-log sink. When @mpath@ is 'Nothing' the sink is
-- 'noopSink' and no writer thread is started. Otherwise a bounded queue + a
-- background writer thread are created; the writer is linked so its (unexpected)
-- death is surfaced, and it is torn down when @inner@ returns.
withEventLog :: Logger -> Maybe FilePath -> (PoolEventSink -> IO a) -> IO a
withEventLog _ Nothing inner = inner noopSink
withEventLog logger (Just path) inner = withLogTag logger "EventLog" $ \elog -> do
    q <- newTBQueueIO queueCapacity
    dropped <- newIORef (0 :: Int)
    let sink ev = do
            let line = encodePoolEventLine ev
            ok <- atomically $ tryWriteTBQueue q line
            when (not ok) $ modifyIORef' dropped (+ 1)
    writeLog elog L.Info $ "pool event log enabled: " <> T.pack path
    withAsync (writerLoop elog path q dropped) $ \a -> do
        link a
        inner sink

-- | The background writer: drains the queue, appends each line, rotates by size,
-- and periodically reports drops. Never throws out of the loop.
writerLoop :: Logger -> FilePath -> TBQueue LB.ByteString -> IORef Int -> IO ()
writerLoop logger path q dropped = do
    h0 <- openAppend path
    sz0 <- currentSize path
    countRef <- newIORef (0 :: Int)
    loop h0 sz0 countRef
  where
    loop h written countRef = do
        line <- atomically $ readTBQueue q
        try (LB.hPut h line >> IO.hFlush h) >>= \case
            Left (e :: SomeException) -> do
                writeLog logger L.Warn $ "pool event-log write failed (re-opening): " <> sshow e
                _ <- try (IO.hClose h) :: IO (Either SomeException ())
                h' <- reopen
                loop h' 0 countRef
            Right () -> do
                reportDrops countRef
                let written' = written + LB.length line
                if written' >= rotateBytes
                    then do
                        h' <- rotate h
                        loop h' 0 countRef
                    else loop h written' countRef

    -- Surface the dropped-event count to the operator every dropLogEvery writes.
    reportDrops countRef = do
        n <- atomicModifyIORef' countRef (\x -> (x + 1, x + 1))
        when (n `mod` dropLogEvery == 0) $ do
            d <- readIORef dropped
            when (d > 0) $
                writeLog logger L.Warn $
                    "pool event-log dropped " <> sshow d <> " event(s) (queue full / consumer slow)"

    -- Close, rename the active file to <path>.1 (overwriting the prior backup),
    -- and open a fresh active file (new inode → sidecar resets to its start).
    rotate h = do
        _ <- try (IO.hClose h) :: IO (Either SomeException ())
        _ <- try (renameFile path (path <> ".1")) :: IO (Either SomeException ())
        writeLog logger L.Info $ "rotated pool event log at " <> sshow rotateBytes <> " bytes"
        openAppend path

    reopen = openAppend path

    openAppend :: FilePath -> IO IO.Handle
    openAppend p = do
        h <- IO.openFile p IO.AppendMode
        IO.hSetBuffering h (IO.BlockBuffering Nothing)
        return h

    currentSize :: FilePath -> IO Int64
    currentSize p = doesFileExist p >>= \case
        True -> fromInteger <$> getFileSize p
        False -> return 0
