package storage

import (
	"context"
	"fmt"
	"time"
)

var _ IdleSessionArchiver = (*PostgresStateStore)(nil)

func (s *PostgresStateStore) ArchiveIdleSessions(ctx context.Context, before time.Time, limit int) (int64, error) {
	if before.IsZero() || limit <= 0 {
		return 0, fmt.Errorf("archive cutoff and positive limit are required")
	}
	var archived int64
	err := s.database.QueryRowContext(ctx, `
WITH candidates AS (
    SELECT tenant_id, session_key FROM sessions
    WHERE status='active' AND updated_at < $1
    ORDER BY updated_at FOR UPDATE SKIP LOCKED LIMIT $2
), archived AS (
    UPDATE sessions AS session
    SET status='archived', archived_at=NOW(), updated_at=NOW()
    FROM candidates
    WHERE session.tenant_id=candidates.tenant_id
      AND session.session_key=candidates.session_key
    RETURNING session.tenant_id, session.session_key
), conversations AS (
    UPDATE channel_conversations AS conversation
    SET ended_at=NOW(), updated_at=NOW()
    FROM archived
    WHERE conversation.tenant_id=archived.tenant_id
      AND conversation.session_key=archived.session_key
      AND conversation.ended_at IS NULL
)
SELECT COUNT(*) FROM archived`, before.UTC(), limit).Scan(&archived)
	if err != nil {
		return 0, fmt.Errorf("archive idle sessions: %w", err)
	}
	return archived, nil
}
