package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

type Disposition struct {
	MessageID  string    `json:"message_id"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurred_at"`
	RecordedAt time.Time `json:"recorded_at"`
}
type DispositionStore interface {
	RecordDisposition(context.Context, controlplane.ChannelBinding, channels.InboundEnvelope, string) error
	ListDispositions(context.Context, string, string, int) ([]Disposition, error)
}
type RunAdmission interface {
	StartRun(context.Context, workqueue.AgentTask, string) (bool, error)
	ExpireUnstarted(context.Context, workqueue.AgentTask) (bool, error)
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func (j *PostgresJournal) RecordDisposition(ctx context.Context, b controlplane.ChannelBinding, m channels.InboundEnvelope, reason string) error {
	_, err := j.db.ExecContext(ctx, `INSERT INTO channel_message_disposition(tenant_id,channel_binding_id,message_id,reason,occurred_at) SELECT $1,$2,$3,$4,$5 WHERE NOT EXISTS(SELECT 1 FROM inbound_message WHERE channel_binding_id=$2 AND external_message_id=$3) ON CONFLICT DO NOTHING`, b.TenantID, b.ID, m.ExternalMessageID, reason, m.OccurredAt)
	if err != nil {
		return errors.New("message disposition persistence unavailable")
	}
	return nil
}
func (j *PostgresJournal) ListDispositions(ctx context.Context, tenant, binding string, limit int) ([]Disposition, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("invalid disposition limit")
	}
	rows, err := j.db.QueryContext(ctx, `SELECT message_id,reason,occurred_at,recorded_at FROM channel_message_disposition WHERE tenant_id=$1 AND channel_binding_id=$2 ORDER BY recorded_at DESC LIMIT $3`, tenant, binding, limit)
	if err != nil {
		return nil, errors.New("message dispositions unavailable")
	}
	defer func() { _ = rows.Close() }()
	items := []Disposition{}
	for rows.Next() {
		var v Disposition
		if err = rows.Scan(&v.MessageID, &v.Reason, &v.OccurredAt, &v.RecordedAt); err != nil {
			return nil, errors.New("message disposition invalid")
		}
		items = append(items, v)
	}
	return items, rows.Err()
}

type memoryDisposition struct {
	tenant, binding string
	value           Disposition
}

func (j *MemoryJournal) RecordDisposition(ctx context.Context, b controlplane.ChannelBinding, m channels.InboundEnvelope, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	key := b.ID + "\x00" + m.ExternalMessageID
	if _, exists := j.inbound[key]; exists {
		return nil
	}
	if j.dispositions == nil {
		j.dispositions = map[string]memoryDisposition{}
	}
	if _, exists := j.dispositions[key]; !exists {
		j.dispositions[key] = memoryDisposition{b.TenantID, b.ID, Disposition{MessageID: m.ExternalMessageID, Reason: reason, OccurredAt: m.OccurredAt, RecordedAt: time.Now().UTC()}}
	}
	return nil
}
func (j *MemoryJournal) ListDispositions(ctx context.Context, tenant, binding string, limit int) ([]Disposition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, errors.New("invalid disposition limit")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	items := []Disposition{}
	for _, entry := range j.dispositions {
		if entry.tenant == tenant && entry.binding == binding {
			items = append(items, entry.value)
		}
	}
	sort.Slice(items, func(i, k int) bool { return items[i].RecordedAt.After(items[k].RecordedAt) })
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (j *PostgresJournal) StartRun(ctx context.Context, task workqueue.AgentTask, workerID string) (bool, error) {
	return j.admitRun(ctx, task, workerID, true)
}
func (j *PostgresJournal) ExpireUnstarted(ctx context.Context, task workqueue.AgentTask) (bool, error) {
	return j.admitRun(ctx, task, "", false)
}
func (j *PostgresJournal) admitRun(ctx context.Context, task workqueue.AgentTask, workerID string, start bool) (bool, error) {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var status, mode, channel string
	var started, expiry sql.NullTime
	var created time.Time
	var config []byte
	err = tx.QueryRowContext(ctx, `SELECT r.status,r.started_at,COALESCE(r.message_mode,''),r.message_expires_at,r.created_at,COALESCE(b.channel_type,''),COALESCE(b.config,'{}'::jsonb) FROM agent_run r LEFT JOIN conversation c ON c.conversation_id=r.conversation_id AND c.tenant_id=r.tenant_id LEFT JOIN channel_binding b ON b.channel_binding_id=c.channel_binding_id AND b.tenant_id=r.tenant_id WHERE r.request_id=$1 AND r.tenant_id=$2 AND r.app_id=$3 FOR UPDATE OF r`, task.RequestID, task.Scope.TenantID, task.Scope.AppID).Scan(&status, &started, &mode, &expiry, &created, &channel, &config)
	if err != nil {
		return false, errors.New("run admission unavailable")
	}
	if status == "expired" {
		return true, tx.Commit()
	}
	if status == "queued" && !started.Valid && mode == "" && channels.RealtimeChannel(channel) {
		policy, e := channels.ParseMessagePolicy(json.RawMessage(config))
		if e != nil {
			return false, e
		}
		mode = policy.Mode
		if mode == channels.RealtimeMessages {
			expiry = sql.NullTime{Time: created.Add(policy.MaxAge()), Valid: true}
		}
	}
	if status == "queued" && !started.Valid && mode == channels.RealtimeMessages && expiry.Valid && !time.Now().Before(expiry.Time) {
		if _, err = tx.ExecContext(ctx, `UPDATE agent_run SET status='expired',error_type='message_expired',completed_at=now() WHERE request_id=$1`, task.RequestID); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE inbound_message SET status='expired',processed_at=now() WHERE tenant_id=$1 AND request_id=$2`, task.Scope.TenantID, task.RequestID); err != nil {
			return false, err
		}
		return true, tx.Commit()
	}
	if start {
		if status == "dead" {
			return false, ErrRunTerminal
		}
		if status != "completed" {
			if _, err = tx.ExecContext(ctx, `UPDATE agent_run SET status='running',worker_id=$2,started_at=COALESCE(started_at,now()),error_type=NULL,error_message=NULL WHERE request_id=$1`, task.RequestID, workerID); err != nil {
				return false, err
			}
		}
	}
	return false, tx.Commit()
}
func (j *MemoryJournal) StartRun(ctx context.Context, task workqueue.AgentTask, workerID string) (bool, error) {
	return j.admitRun(ctx, task, workerID, true)
}
func (j *MemoryJournal) ExpireUnstarted(ctx context.Context, task workqueue.AgentTask) (bool, error) {
	return j.admitRun(ctx, task, "", false)
}
func (j *MemoryJournal) admitRun(ctx context.Context, task workqueue.AgentTask, workerID string, start bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	r := j.runs[task.RequestID]
	if r == nil || r.tenantID != task.Scope.TenantID || r.appID != task.Scope.AppID {
		return false, errors.New("run admission unavailable")
	}
	if r.status == "expired" {
		return true, nil
	}
	deadline := r.lifetime.ExpiresAt
	mode := r.lifetime.Mode
	if mode == "" && channels.RealtimeChannel(r.channel) {
		mode = channels.RealtimeMessages
		deadline = r.createdAt.Add(2 * time.Minute)
	}
	if r.status == "queued" && r.startedAt.IsZero() && mode == channels.RealtimeMessages && !deadline.IsZero() && !time.Now().Before(deadline) {
		r.status = "expired"
		r.errType = "message_expired"
		r.completedAt = time.Now().UTC()
		return true, nil
	}
	if start {
		if r.status == "dead" {
			return false, ErrRunTerminal
		}
		if r.status != "completed" {
			r.status = "running"
			r.workerID = workerID
			if r.startedAt.IsZero() {
				r.startedAt = time.Now().UTC()
			}
		}
	}
	return false, nil
}
