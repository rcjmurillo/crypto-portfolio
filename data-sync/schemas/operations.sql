CREATE TABLE IF NOT EXISTS operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    source TEXT,
    op_type TEXT,
    data JSON,
    created_at TIMESTAMP,
    imported_at TIMESTAMP DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now', 'utc')),
    UNIQUE(source_id, source, op_type)
);

CREATE INDEX idx_operations_source ON operations (source);
CREATE INDEX idx_operations_source_ID ON operations (source_id);
