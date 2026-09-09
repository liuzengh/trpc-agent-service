package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// Archiver implements monthly archival: session_event and
// audit_log rows older than the retention window are moved to the same-shaped
// archive tables, keeping the hot tables small.
//
// Copy-then-delete runs per batch inside one transaction, so a crash
// mid-batch leaves the rows in both tables (idempotent re-run) and never
// loses them. Batches are small and paced to avoid long locks on hot tables.
type Archiver struct {
	pool *pgxpool.Pool

	// Retention is how long rows stay in the hot tables (about one
	// month online). Interval is how often the sweep runs; BatchSize caps one
	// transaction; BatchPause paces consecutive batches.
	Retention  time.Duration
	Interval   time.Duration
	BatchSize  int
	BatchPause time.Duration
}

// NewArchiver creates an Archiver; zero fields fall back to the defaults
// (30 days retention, daily sweep, 1000-row batches, 100ms pause).
func NewArchiver(pool *pgxpool.Pool, retention, interval time.Duration) *Archiver {
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	return &Archiver{
		pool: pool, Retention: retention, Interval: interval,
		BatchSize: 1000, BatchPause: 100 * time.Millisecond,
	}
}

// archiveTargets maps each hot table to its archive table; both share column
// order (same shape, archive tables drop the FKs).
var archiveTargets = []struct{ src, dst string }{
	{"session_event", "session_event_archive"},
	{"audit_log", "audit_log_archive"},
}

// Run sweeps on every interval tick until ctx is canceled. It sweeps once at
// startup too, so a long-down process catches up immediately.
func (a *Archiver) Run(ctx context.Context) {
	a.sweep(ctx)
	ticker := time.NewTicker(a.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sweep(ctx)
		}
	}
}

func (a *Archiver) sweep(ctx context.Context) {
	events, audits, err := a.ArchiveOnce(ctx)
	if err != nil {
		plog.Errorf("archive sweep failed: %v", err)
		return
	}
	if events+audits > 0 {
		plog.Infof("archive sweep: %d session events, %d audit rows archived", events, audits)
	}
}

// ArchiveOnce archives all rows older than the retention window, returning
// per-table counts. Exported for tests and manual runs.
func (a *Archiver) ArchiveOnce(ctx context.Context) (events, audits int64, err error) {
	cutoff := time.Now().Add(-a.Retention)
	counts := make([]int64, 0, len(archiveTargets))
	for _, t := range archiveTargets {
		n, err := a.archiveTable(ctx, t.src, t.dst, cutoff)
		if err != nil {
			return 0, 0, err
		}
		counts = append(counts, n)
	}
	return counts[0], counts[1], nil
}

// archiveTable moves one batch at a time: SELECT the oldest ids, copy them
// into the archive (idempotent on re-run), delete them from the source — all
// in one transaction per batch, with rate-limited batched deletes.
func (a *Archiver) archiveTable(ctx context.Context, src, dst string, cutoff time.Time) (int64, error) {
	var total int64
	for {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n, err := a.archiveBatch(ctx, src, dst, cutoff)
		if err != nil {
			return total, fmt.Errorf("archive %s: %w", src, err)
		}
		total += n
		if n < int64(a.BatchSize) {
			return total, nil
		}
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case <-time.After(a.BatchPause):
		}
	}
}

func (a *Archiver) archiveBatch(ctx context.Context, src, dst string, cutoff time.Time) (int64, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		fmt.Sprintf(`SELECT id FROM %s WHERE created_at < $1 ORDER BY created_at LIMIT $2`, src),
		cutoff, a.BatchSize)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s SELECT * FROM %s WHERE id = ANY($1) ON CONFLICT (id) DO NOTHING`, dst, src),
		ids); err != nil {
		return 0, fmt.Errorf("copy to archive: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE id = ANY($1)`, src), ids); err != nil {
		return 0, fmt.Errorf("delete archived: %w", err)
	}
	return int64(len(ids)), tx.Commit(ctx)
}
