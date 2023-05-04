CREATE TABLE operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_source INTEGER,
    source TEXT,
    record_type TEXT,
    data JSON,
    created_at TIMESTAMP,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_operations_source ON operations (source);