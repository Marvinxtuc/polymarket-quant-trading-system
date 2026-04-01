-- BLOCK-002: augment order_intents with idempotency metadata and indices.

ALTER TABLE order_intents ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT '';
ALTER TABLE order_intents ADD COLUMN strategy_name TEXT;
ALTER TABLE order_intents ADD COLUMN signal_source TEXT;
ALTER TABLE order_intents ADD COLUMN signal_fingerprint TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_intents_idempotency
    ON order_intents(idempotency_key)
    WHERE idempotency_key <> '';

CREATE INDEX IF NOT EXISTS idx_order_intents_intent_status
    ON order_intents(status, idempotency_key);
