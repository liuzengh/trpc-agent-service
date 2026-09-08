package wecommcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

var ErrInvalidRecovery = errors.New("invalid checkpoint recovery")

func recoveryCheckpoint(b controlplane.ChannelBinding, chat string, cp Checkpoint, version int64, action string, from, timeNow time.Time, ack bool) (Checkpoint, error) {
	cfg, err := ParseBinding(b)
	if err != nil || b.Status != controlplane.StatusDisabled || b.Version <= 0 || cp.Version != version {
		return Checkpoint{}, ErrStateConflict
	}
	matched := false
	for _, id := range cfg.AllowedChatIDs {
		if endpointHash(id) == chat {
			matched = true
		}
	}
	if !matched || from.IsZero() || from.Nanosecond() != 0 || from.Before(cfg.Start()) || from.Before(timeNow.Add(-7*24*time.Hour+time.Minute)) || from.After(timeNow) {
		return Checkpoint{}, fmt.Errorf("%w: scope or timestamp", ErrInvalidRecovery)
	}
	hash := ConfigFingerprint(b, cfg)
	switch action {
	case "rewind":
		if from.After(cp.Through) || cp.ConfigHash != hash {
			return Checkpoint{}, fmt.Errorf("%w: rewind cannot skip forward or adopt config", ErrInvalidRecovery)
		}
	case "resume":
		if !ack {
			return Checkpoint{}, fmt.Errorf("%w: resume requires acknowledge_gap=true", ErrInvalidRecovery)
		}
	default:
		return Checkpoint{}, fmt.Errorf("%w: action must be rewind or resume", ErrInvalidRecovery)
	}
	return Checkpoint{ConfigHash: hash, Through: from, Floor: from, Version: cp.Version + 1}, nil
}

func (s *MemoryStore) ListCheckpoints(ctx context.Context, tenant, binding string) ([]CheckpointView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []CheckpointView{}
	for key, cp := range s.checkpoints {
		if key.TenantID == tenant && key.BindingID == binding {
			out = append(out, CheckpointView{key.ChatHash, cp})
		}
	}
	slices.SortFunc(out, func(a, b CheckpointView) int {
		if a.ChatHash < b.ChatHash {
			return -1
		}
		if a.ChatHash > b.ChatHash {
			return 1
		}
		return 0
	})
	return out, nil
}
func (s *MemoryStore) RecoverCheckpoint(ctx context.Context, b controlplane.ChannelBinding, chat string, version int64, action string, from time.Time, ack bool) (Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := PollKey{b.TenantID, b.ID, chat}
	cp, ok := s.checkpoints[key]
	if !ok {
		return cp, ErrStateConflict
	}
	next, err := recoveryCheckpoint(b, chat, cp, version, action, from, time.Now(), ack)
	if err != nil {
		return cp, err
	}
	s.checkpoints[key] = next
	return next, nil
}

func (s *PostgresStore) ListCheckpoints(ctx context.Context, tenant, binding string) ([]CheckpointView, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT chat_hash,config_hash,through_at,version,floor_at FROM channel_poll_checkpoint WHERE tenant_id=$1 AND channel_binding_id=$2 ORDER BY chat_hash`, tenant, binding)
	if err != nil {
		return nil, errors.New("cannot list channel checkpoints")
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(rows)
	out := []CheckpointView{}
	for rows.Next() {
		var c CheckpointView
		var floor sql.NullTime
		if rows.Scan(&c.ChatHash, &c.ConfigHash, &c.Through, &c.Version, &floor) != nil {
			return nil, errors.New("cannot decode channel checkpoint")
		}
		if floor.Valid {
			c.Floor = floor.Time
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *PostgresStore) RecoverCheckpoint(ctx context.Context, b controlplane.ChannelBinding, chat string, version int64, action string, from time.Time, ack bool) (Checkpoint, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Checkpoint{}, errors.New("cannot begin checkpoint recovery")
	}
	defer func() { _ = tx.Rollback() }()
	// Hold a shared row lock until commit so concurrent enable/reconfiguration
	// cannot race recovery. Caller-supplied binding metadata is not trusted here.
	var current controlplane.ChannelBinding
	var cfg []byte
	err = tx.QueryRowContext(ctx, `SELECT tenant_id,channel_binding_id,app_id,account_id,channel_type,secret_ref,config,status,version FROM channel_binding WHERE tenant_id=$1 AND channel_binding_id=$2 FOR SHARE`, b.TenantID, b.ID).Scan(&current.TenantID, &current.ID, &current.AppID, &current.AccountID, &current.ChannelType, &current.SecretRef, &cfg, &current.Status, &current.Version)
	if err != nil || current.Version != b.Version {
		return Checkpoint{}, ErrStateConflict
	}
	current.Config = json.RawMessage(cfg)
	var cp Checkpoint
	var floor sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT config_hash,through_at,version,floor_at FROM channel_poll_checkpoint WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3 FOR UPDATE`, b.TenantID, b.ID, chat).Scan(&cp.ConfigHash, &cp.Through, &cp.Version, &floor)
	if err != nil {
		return Checkpoint{}, ErrStateConflict
	}
	if floor.Valid {
		cp.Floor = floor.Time
	}
	next, err := recoveryCheckpoint(current, chat, cp, version, action, from, time.Now(), ack)
	if err != nil {
		return Checkpoint{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE channel_poll_checkpoint SET config_hash=$4,through_at=$5,floor_at=$5,version=version+1,updated_at=now() WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3`, b.TenantID, b.ID, chat, next.ConfigHash, next.Through); err != nil {
		return Checkpoint{}, errors.New("checkpoint recovery write failed")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO channel_checkpoint_recovery(tenant_id,channel_binding_id,chat_hash,version,action,previous_through,resume_from,previous_config_hash,config_hash,acknowledge_gap,trace_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, b.TenantID, b.ID, chat, next.Version, action, cp.Through, next.Through, cp.ConfigHash, next.ConfigHash, ack, audit.TraceID(ctx)); err != nil {
		return Checkpoint{}, errors.New("checkpoint recovery audit failed")
	}
	if err := tx.Commit(); err != nil {
		return Checkpoint{}, errors.New("checkpoint recovery outcome unavailable; inspect current version before retry")
	}
	return next, nil
}
