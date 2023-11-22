CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    source TEXT,
    tx_type TEXT,
    data JSON,
    created_at TIMESTAMP,
    imported_at TIMESTAMP DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now', 'utc')),
    UNIQUE(source_id, source, tx_type)
);

CREATE INDEX idx_transactions_source ON transactions (source);
CREATE INDEX idx_transactions_source_ID ON transactions (source_id);
