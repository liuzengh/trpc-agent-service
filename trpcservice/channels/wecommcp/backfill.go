package wecommcp

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type BackfillStore interface {
	NextGap(context.Context, PollKey) (Gap, bool, error)
	AdvanceGap(context.Context, controlplane.ChannelBinding, PollKey, Gap, time.Time, string) error
}

func (s *PostgresStore) NextGap(ctx context.Context, key PollKey) (Gap, bool, error) {
	var g Gap
	err := s.db.QueryRowContext(ctx, `SELECT checkpoint_version,config_hash,cursor_at,recent_from FROM channel_poll_gap WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3 AND status='pending' ORDER BY previous_through LIMIT 1`, key.TenantID, key.BindingID, key.ChatHash).Scan(&g.Version, &g.ConfigHash, &g.Cursor, &g.RecentFrom)
	if errors.Is(err, sql.ErrNoRows) {
		return g, false, nil
	}
	return g, err == nil, err
}

func (s *PostgresStore) AdvanceGap(ctx context.Context, b controlplane.ChannelBinding, key PollKey, g Gap, to time.Time, problem string) error {
	if to.Before(g.Cursor) || to.After(g.RecentFrom) {
		return ErrStateConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var version int64
	var status string
	if err = tx.QueryRowContext(ctx, `SELECT version,status FROM channel_binding WHERE tenant_id=$1 AND channel_binding_id=$2 FOR SHARE`, key.TenantID, key.BindingID).Scan(&version, &status); err != nil || version != b.Version || status != controlplane.StatusActive {
		return ErrStateConflict
	}
	var cpHash string
	var floor sql.NullTime
	if err = tx.QueryRowContext(ctx, `SELECT config_hash,floor_at FROM channel_poll_checkpoint WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3 FOR SHARE`, key.TenantID, key.BindingID, key.ChatHash).Scan(&cpHash, &floor); err != nil {
		return err
	}
	if problem == "" && (cpHash != g.ConfigHash || (floor.Valid && g.Cursor.Before(floor.Time))) {
		return ErrStateConflict
	}
	nextStatus := "pending"
	if !to.Before(g.RecentFrom) {
		nextStatus = "completed"
	}
	if problem != "" {
		nextStatus = "blocked"
	}
	r, err := tx.ExecContext(ctx, `UPDATE channel_poll_gap SET cursor_at=$6,status=$7,last_error=$8 WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3 AND checkpoint_version=$4 AND cursor_at=$5 AND status='pending'`, key.TenantID, key.BindingID, key.ChatHash, g.Version, g.Cursor, to, nextStatus, problem)
	if err = changed(r, err); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MemoryStore) NextGap(ctx context.Context, key PollKey) (Gap, bool, error) {
	if err := ctx.Err(); err != nil {
		return Gap{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.gaps {
		if g.key == key && g.value.Status == "pending" {
			return g.value, true, nil
		}
	}
	return Gap{}, false, nil
}

func (s *MemoryStore) AdvanceGap(ctx context.Context, _ controlplane.ChannelBinding, key PollKey, g Gap, to time.Time, problem string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := s.checkpoints[key]
	if to.Before(g.Cursor) || to.After(g.RecentFrom) || (problem == "" && (cp.ConfigHash != g.ConfigHash || g.Cursor.Before(cp.Floor))) {
		return ErrStateConflict
	}
	for i := range s.gaps {
		row := &s.gaps[i]
		if row.key != key || row.value.Version != g.Version {
			continue
		}
		if row.value.Status != "pending" || !row.value.Cursor.Equal(g.Cursor) {
			return ErrStateConflict
		}
		row.value.Cursor = to
		if !to.Before(g.RecentFrom) {
			row.value.Status = "completed"
		}
		if problem != "" {
			row.value.Status = "blocked"
			row.value.LastError = problem
		}
		return nil
	}
	return ErrStateConflict
}
