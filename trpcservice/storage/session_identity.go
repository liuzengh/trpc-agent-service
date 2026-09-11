package storage

import (
	"context"
	"time"
)

// IdleSessionArchiver closes inactive conversations in bounded batches.
type IdleSessionArchiver interface {
	ArchiveIdleSessions(context.Context, time.Time, int) (int64, error)
}
