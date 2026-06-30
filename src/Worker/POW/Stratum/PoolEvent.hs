{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Worker.POW.Stratum.PoolEvent
-- License: MIT
--
-- The pool-event NDJSON contract emitted by the stratum server's share-accept
-- site for the StoaChain 2.0.0 accounting sidecar. This is the engine side of
-- the contract whose authoritative TS view lives at @pool2/event-schema.ts@ and
-- @pool2/EVENT-SCHEMA.md@ in the AncientHoldings hub repo.
--
-- TWO SEPARATE variants (D1): a @share@ event for EVERY accepted submit, and a
-- SEPARATE @block@ event when that same submit also clears the block target. The
-- finder's share is recorded exactly once (from the @share@ event); the @block@
-- event does NOT also record a share.
--
-- The @block@ event carries the engine's OWN nonce + chainId only (D2) — never a
-- node-canonical hash and never a height. The sidecar resolves the canonical
-- @{height, hash}@ downstream by scanning the node's recent canonical headers for
-- the matching nonce. Emitting a @blockHash@ here is a contract violation; we
-- never do.
--
module Worker.POW.Stratum.PoolEvent
( PoolEvent(..)
, shareEvent
, blockEvent
, encodePoolEventLine
) where

import qualified Data.Aeson as A
import Data.Aeson ((.=))
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text as T

-- | The schema version the sidecar gates on. Bump on ANY change to the line shape
-- (add/remove/retype a field on either variant) and update @pool2/event-schema.ts@
-- + @pool2/EVENT-SCHEMA.md@ in the same change.
poolEventSchemaVersion :: Int
poolEventSchemaVersion = 1

-- | A pool event line. All fields are already-rendered 'T.Text' (the accept site
-- extracts the primitives) so this module needs none of the engine's newtypes.
data PoolEvent
    = ShareEvent
        { _peUsername :: !T.Text
            -- ^ the @mining.authorize@ credential (the first-dot-split segment) =
            -- @pool_workers.stratum_credential@
        , _peWorker :: !T.Text
            -- ^ the optional worker tag (the post-dot component, dot stripped);
            -- MAY be empty (advisory / stat-only)
        , _peChainId :: !T.Text
            -- ^ share-advisory chainId (the sidecar drops it from recordShare)
        , _peTarget :: !T.Text
            -- ^ the session/job target as a non-negative integer decimal string;
            -- the sidecar converts it to a difficulty weight
        , _peTimestamp :: !T.Text
            -- ^ ISO-8601 timestamp
        }
    | BlockEvent
        { _peUsername :: !T.Text
        , _peWorker :: !T.Text
        , _peChainId :: !T.Text
            -- ^ block-AUTHORITATIVE chainId (the chain the hub confirms against)
        , _peTarget :: !T.Text
        , _peOwnNonce :: !T.Text
            -- ^ the engine's OWN nonce (Word64 decimal) — the rival discriminator
            -- the sidecar matches against the node's canonical header nonce. NOT a
            -- block identity.
        , _peTimestamp :: !T.Text
        }

-- | Build a @share@ event from already-rendered fields.
shareEvent
    :: T.Text -- ^ username (credential)
    -> T.Text -- ^ worker tag (may be empty)
    -> T.Text -- ^ chainId
    -> T.Text -- ^ target (decimal)
    -> T.Text -- ^ timestamp (ISO-8601)
    -> PoolEvent
shareEvent = ShareEvent

-- | Build a @block@ event from already-rendered fields.
blockEvent
    :: T.Text -- ^ username (credential)
    -> T.Text -- ^ worker tag (may be empty)
    -> T.Text -- ^ chainId
    -> T.Text -- ^ target (decimal)
    -> T.Text -- ^ ownNonce (decimal)
    -> T.Text -- ^ timestamp (ISO-8601)
    -> PoolEvent
blockEvent = BlockEvent

instance A.ToJSON PoolEvent where
    toJSON (ShareEvent u w c t ts) = A.object
        [ "schemaVersion" .= poolEventSchemaVersion
        , "kind" .= ("share" :: T.Text)
        , "username" .= u
        , "worker" .= w
        , "chainId" .= c
        , "target" .= t
        , "timestamp" .= ts
        ]
    toJSON (BlockEvent u w c t n ts) = A.object
        [ "schemaVersion" .= poolEventSchemaVersion
        , "kind" .= ("block" :: T.Text)
        , "username" .= u
        , "worker" .= w
        , "chainId" .= c
        , "target" .= t
        , "ownNonce" .= n
        , "timestamp" .= ts
        ]

-- | A single NDJSON line for the event: compact JSON (aeson never embeds a
-- newline) followed by a trailing @\\n@.
encodePoolEventLine :: PoolEvent -> LB.ByteString
encodePoolEventLine ev = A.encode ev <> "\n"
