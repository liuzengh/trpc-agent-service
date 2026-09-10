CREATE TABLE platform.worker_heartbeat (
    worker_id TEXT PRIMARY KEY CHECK (worker_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('READY', 'NOT_READY')),
    concurrency INTEGER NOT NULL CHECK (concurrency > 0),
    started_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    last_error TEXT NOT NULL DEFAULT ''
);

CREATE INDEX worker_heartbeat_seen_idx
    ON platform.worker_heartbeat (last_seen_at DESC);
