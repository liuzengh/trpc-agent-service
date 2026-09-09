package gateway

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"
)

type RunView struct {
	AppName          string    `json:"app_name,omitempty"`
	Kind             string    `json:"kind"`
	RequestID        string    `json:"request_id"`
	TenantID         string    `json:"tenant_id"`
	AppID            string    `json:"app_id"`
	RevisionID       string    `json:"revision_id"`
	AgentName        string    `json:"agent_name"`
	Channel          string    `json:"channel"`
	BindingID        string    `json:"binding_id,omitempty"`
	LatencyMS        *int64    `json:"latency_ms,omitempty"`
	Status           string    `json:"status"`
	ErrorType        string    `json:"error_type,omitempty"`
	TraceID          string    `json:"trace_id,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	Cost             float64   `json:"cost"`
	Reply            string    `json:"reply,omitempty"`
	Delivery         any       `json:"delivery,omitempty"`
}
type RunFilter struct {
	TenantID, AppID, Status, BeforeID string
	Channel                           string
	BindingID                         string
	AfterTime                         time.Time
	BeforeTime                        time.Time
	Limit                             int
}
type RunReader interface {
	ListRuns(context.Context, RunFilter) ([]RunView, error)
	ReadRun(context.Context, string, string, bool) (RunView, error)
}

var ErrRunMissing = errors.New("run not found")

const runViewSelect = `SELECT r.request_id,r.tenant_id,r.app_id,r.revision_id,COALESCE(r.agent_name,''),COALESCE(b.channel_type,''),r.status,COALESCE(r.error_type,''),COALESCE(r.trace_id,''),r.created_at,r.prompt_tokens,r.completion_tokens,r.cost,COALESCE(c.channel_binding_id,''),CASE WHEN r.started_at IS NOT NULL AND r.completed_at IS NOT NULL THEN GREATEST(0,floor(EXTRACT(EPOCH FROM (r.completed_at-r.started_at))*1000))::bigint END FROM agent_run r LEFT JOIN conversation c ON c.conversation_id=r.conversation_id AND c.tenant_id=r.tenant_id LEFT JOIN channel_binding b ON b.channel_binding_id=c.channel_binding_id AND b.tenant_id=r.tenant_id`

func scanRunView(row interface{ Scan(...any) error }) (RunView, error) {
	v := RunView{Kind: "im"}
	err := row.Scan(&v.RequestID, &v.TenantID, &v.AppID, &v.RevisionID, &v.AgentName, &v.Channel, &v.Status, &v.ErrorType, &v.TraceID, &v.CreatedAt, &v.PromptTokens, &v.CompletionTokens, &v.Cost, &v.BindingID, &v.LatencyMS)
	return v, err
}
func (j *PostgresJournal) ListRuns(ctx context.Context, f RunFilter) ([]RunView, error) {
	if f.TenantID == "" || f.Limit < 1 || f.Limit > 100 {
		return nil, errors.New("invalid run filter")
	}
	var before any
	var after any
	if !f.AfterTime.IsZero() {
		after = f.AfterTime
	}
	if !f.BeforeTime.IsZero() {
		before = f.BeforeTime
	}
	rows, err := j.db.QueryContext(ctx, runViewSelect+` WHERE r.tenant_id=$1 AND ($2='' OR r.app_id=$2) AND ($3='' OR r.status=$3 OR ($3='failed' AND r.status='dead')) AND ($4::timestamptz IS NULL OR (r.created_at,r.request_id)<($4,$5)) AND ($7='' OR b.channel_type=$7) AND ($8::timestamptz IS NULL OR r.created_at>=$8) AND ($9='' OR c.channel_binding_id=$9) ORDER BY r.created_at DESC,r.request_id DESC LIMIT $6`, f.TenantID, f.AppID, f.Status, before, f.BeforeID, f.Limit, f.Channel, after, f.BindingID)
	if err != nil {
		return nil, errors.New("run query unavailable")
	}
	defer func() { _ = rows.Close() }()
	out := []RunView{}
	for rows.Next() {
		v, err := scanRunView(rows)
		if err != nil {
			return nil, errors.New("run query unavailable")
		}
		out = append(out, v)
	}
	if rows.Err() != nil {
		return nil, errors.New("run query unavailable")
	}
	return out, nil
}
func (j *PostgresJournal) ReadRun(ctx context.Context, tenant, id string, includeReply bool) (RunView, error) {
	v, err := scanRunView(j.db.QueryRowContext(ctx, runViewSelect+` WHERE r.tenant_id=$1 AND r.request_id=$2`, tenant, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrRunMissing
	}
	if err != nil {
		return v, errors.New("run query unavailable")
	}
	var delivery struct {
		Status    string `json:"status"`
		Attempts  int    `json:"attempts"`
		ErrorType string `json:"error_type,omitempty"`
	}
	err = j.db.QueryRowContext(ctx, `SELECT status,attempt_count,COALESCE(last_error_type,'') FROM outbound_message WHERE tenant_id=$1 AND request_id=$2`, tenant, id).Scan(&delivery.Status, &delivery.Attempts, &delivery.ErrorType)
	if err == nil {
		v.Delivery = delivery
	} else if !errors.Is(err, sql.ErrNoRows) {
		return v, errors.New("delivery query unavailable")
	}
	if includeReply {
		var text sql.NullString
		err = j.db.QueryRowContext(ctx, `SELECT payload->>'text' FROM outbound_message WHERE tenant_id=$1 AND request_id=$2`, tenant, id).Scan(&text)
		if err == nil {
			v.Reply = text.String
		} else if !errors.Is(err, sql.ErrNoRows) {
			return v, errors.New("reply query unavailable")
		}
	}
	return v, nil
}
func memoryRunView(id string, r *memoryRun) RunView {
	v := RunView{Kind: "im", RequestID: id, TenantID: r.tenantID, AppID: r.appID, RevisionID: r.revisionID, Channel: r.channel, BindingID: r.bindingID, AgentName: r.result.AgentName, Status: r.status, ErrorType: r.errType, TraceID: r.result.TraceID, CreatedAt: r.createdAt, PromptTokens: r.result.PromptTokens, CompletionTokens: r.result.CompletionTokens, Cost: r.result.Cost}
	if !r.startedAt.IsZero() && !r.completedAt.IsZero() {
		ms := max(int64(0), r.completedAt.Sub(r.startedAt).Milliseconds())
		v.LatencyMS = &ms
	}
	return v
}
func (j *MemoryJournal) ListRuns(_ context.Context, f RunFilter) ([]RunView, error) {
	if f.TenantID == "" || f.Limit < 1 || f.Limit > 100 {
		return nil, errors.New("invalid run filter")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	out := []RunView{}
	for id, r := range j.runs {
		if f.BindingID != "" && r.bindingID != f.BindingID {
			continue
		}
		if f.Channel != "" && r.channel != f.Channel || !f.AfterTime.IsZero() && r.createdAt.Before(f.AfterTime) {
			continue
		}
		if r.tenantID != f.TenantID || (f.AppID != "" && r.appID != f.AppID) || (f.Status != "" && r.status != f.Status && (f.Status != "failed" || r.status != "dead")) {
			continue
		}
		v := memoryRunView(id, r)
		if !f.BeforeTime.IsZero() && (v.CreatedAt.After(f.BeforeTime) || v.CreatedAt.Equal(f.BeforeTime) && v.RequestID >= f.BeforeID) {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, k int) bool {
		if out[i].CreatedAt.Equal(out[k].CreatedAt) {
			return out[i].RequestID > out[k].RequestID
		}
		return out[i].CreatedAt.After(out[k].CreatedAt)
	})
	if len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}
func (j *MemoryJournal) ReadRun(_ context.Context, tenant, id string, includeReply bool) (RunView, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r, ok := j.runs[id]
	if !ok || r.tenantID != tenant {
		return RunView{}, ErrRunMissing
	}
	v := memoryRunView(id, r)
	if includeReply {
		v.Reply = r.result.Reply
	}
	for _, out := range j.outbound {
		if out.item.TenantID == tenant && out.item.RequestID == id {
			v.Delivery = map[string]any{"status": out.status, "attempts": out.item.AttemptCount}
			break
		}
	}
	return v, nil
}
