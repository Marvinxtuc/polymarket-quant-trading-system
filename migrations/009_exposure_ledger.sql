-- BLOCK-009: global exposure ledger + risk breaker persistence.

CREATE TABLE IF NOT EXISTS exposure_ledger (
    scope_type TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    updated_ts INTEGER NOT NULL,
    PRIMARY KEY(scope_type, scope_key)
);

CREATE INDEX IF NOT EXISTS idx_exposure_ledger_scope_type
    ON exposure_ledger(scope_type);

CREATE TABLE IF NOT EXISTS risk_breaker_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);
