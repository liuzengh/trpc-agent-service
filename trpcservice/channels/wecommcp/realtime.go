package wecommcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type Gap struct {
	PreviousThrough  time.Time `json:"previous_through"`
	RecentFrom       time.Time `json:"recent_from"`
	ProcessedThrough time.Time `json:"processed_through"`
	RecordedAt       time.Time `json:"recorded_at"`
	Reason           string    `json:"reason"`
}
type RealtimeStore interface {
	AdvanceRecent(context.Context, controlplane.ChannelBinding, PollKey, Checkpoint, time.Time, time.Time) error
	ListGaps(context.Context, string, string, int) ([]Gap, error)
}

func validRecentAdvance(b controlplane.ChannelBinding, key PollKey, cp Checkpoint, from, to time.Time) bool {
	cfg, err := ParseBinding(b)
	if err != nil || b.Status != controlplane.StatusActive || key.TenantID != b.TenantID || key.BindingID != b.ID || !to.After(from) || to.Sub(from) > 2*time.Minute || from.Before(cfg.Start()) || to.Before(cp.Through) || cp.ConfigHash != ConfigFingerprint(b, cfg) {
		return false
	}
	p, err := channels.ParseMessagePolicy(b.Config)
	if err != nil || p.Mode != channels.RealtimeMessages {
		return false
	}
	for _, chat := range cfg.AllowedChatIDs {
		h := sha256.Sum256([]byte(chat))
		if hex.EncodeToString(h[:]) == key.ChatHash {
			return true
		}
	}
	return false
}
func (s *PostgresStore) AdvanceRecent(ctx context.Context, b controlplane.ChannelBinding, key PollKey, cp Checkpoint, from, to time.Time) error {
	if !validRecentAdvance(b, key, cp, from, to) {
		return ErrStateConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var version int64
	var status string
	var raw []byte
	if err = tx.QueryRowContext(ctx, `SELECT version,status,config FROM channel_binding WHERE tenant_id=$1 AND channel_binding_id=$2 FOR SHARE`, key.TenantID, key.BindingID).Scan(&version, &status, &raw); err != nil || version != b.Version || status != controlplane.StatusActive {
		return ErrStateConflict
	}
	current := b
	current.Config = json.RawMessage(raw)
	if !validRecentAdvance(current, key, cp, from, to) {
		return ErrStateConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE channel_poll_checkpoint SET through_at=$6,version=version+1,updated_at=now() WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3 AND config_hash=$4 AND version=$5 AND through_at<=$6`, key.TenantID, key.BindingID, key.ChatHash, cp.ConfigHash, cp.Version, to)
	if err = changed(result, err); err != nil {
		return err
	}
	if from.After(cp.Through) {
		_, err = tx.ExecContext(ctx, `INSERT INTO channel_poll_gap(tenant_id,channel_binding_id,chat_hash,checkpoint_version,previous_through,recent_from,processed_through,reason,trace_id) VALUES($1,$2,$3,$4,$5,$6,$7,'realtime_window',$8)`, key.TenantID, key.BindingID, key.ChatHash, cp.Version+1, cp.Through, from, to, audit.TraceID(ctx))
		if err != nil {
			return errors.New("cannot persist realtime gap audit")
		}
	}
	return tx.Commit()
}
func (s *MemoryStore) AdvanceRecent(ctx context.Context, b controlplane.ChannelBinding, key PollKey, cp Checkpoint, from, to time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validRecentAdvance(b, key, cp, from, to) {
		return ErrStateConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.checkpoints[key]
	if !ok || current.Version != cp.Version || current.ConfigHash != cp.ConfigHash || to.Before(current.Through) {
		return ErrStateConflict
	}
	if from.After(current.Through) {
		s.gaps = append(s.gaps, scopedGap{key, Gap{PreviousThrough: current.Through, RecentFrom: from, ProcessedThrough: to, RecordedAt: time.Now().UTC(), Reason: "realtime_window"}})
	}
	current.Through = to
	current.Version++
	s.checkpoints[key] = current
	return nil
}

type scopedGap struct {
	key   PollKey
	value Gap
}

func (s *MemoryStore) ListGaps(ctx context.Context, tenant, binding string, limit int) ([]Gap, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, ErrStateConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := []Gap{}
	for i := len(s.gaps) - 1; i >= 0 && len(items) < limit; i-- {
		g := s.gaps[i]
		if g.key.TenantID == tenant && g.key.BindingID == binding {
			items = append(items, g.value)
		}
	}
	return items, nil
}
func (s *PostgresStore) ListGaps(ctx context.Context, tenant, binding string, limit int) ([]Gap, error) {
	if limit < 1 || limit > 100 {
		return nil, ErrStateConflict
	}
	rows, err := s.db.QueryContext(ctx, `SELECT previous_through,recent_from,processed_through,recorded_at,reason FROM channel_poll_gap WHERE tenant_id=$1 AND channel_binding_id=$2 ORDER BY recorded_at DESC LIMIT $3`, tenant, binding, limit)
	if err != nil {
		return nil, errors.New("realtime gap audit unavailable")
	}
	defer func() { _ = rows.Close() }()
	items := []Gap{}
	for rows.Next() {
		var g Gap
		if err = rows.Scan(&g.PreviousThrough, &g.RecentFrom, &g.ProcessedThrough, &g.RecordedAt, &g.Reason); err != nil {
			return nil, err
		}
		items = append(items, g)
	}
	return items, rows.Err()
}
