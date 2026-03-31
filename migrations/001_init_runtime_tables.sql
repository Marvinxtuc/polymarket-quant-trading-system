CREATE TABLE IF NOT EXISTS runtime_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS control_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS reconciliation_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS positions_snapshot (
    token_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS order_intents (
    intent_id TEXT PRIMARY KEY,
    strategy_order_uuid TEXT,
    broker_order_id TEXT,
    token_id TEXT NOT NULL,
    condition_id TEXT,
    side TEXT NOT NULL,
    status TEXT NOT NULL,
    recovered_source TEXT,
    recovery_reason TEXT,
    payload TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_intents_status ON order_intents(status);
CREATE INDEX IF NOT EXISTS idx_order_intents_broker_order_id ON order_intents(broker_order_id);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    strategy_order_uuid TEXT PRIMARY KEY,
    wallet TEXT,
    condition_id TEXT,
    token_id TEXT,
    side TEXT,
    notional REAL,
    created_ts INTEGER NOT NULL
);
