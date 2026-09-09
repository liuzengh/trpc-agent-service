package console

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
)

const DebugBinding = "console-debug"

type Snapshot struct {
	SourceChecksum string                     `json:"source_checksum"`
	Config         controlplane.AgentRevision `json:"config"`
	DraftVersion   int64                      `json:"draft_version"`
	DisabledTools  []string                   `json:"disabled_tools"`
}
type Session struct {
	SnapshotID  string `json:"snapshot_id"`
	UserID      string `json:"user_id"`
	ActiveRunID string `json:"active_run_id"`
	Turns       int    `json:"turns"`
	Identity    string `json:"identity"`
}
type Run struct {
	SessionID        string                        `json:"session_id"`
	SnapshotID       string                        `json:"snapshot_id"`
	UserID           string                        `json:"user_id"`
	Input            string                        `json:"input"`
	MessageID        string                        `json:"message_id"`
	InputHash        string                        `json:"input_hash"`
	Reply            string                        `json:"reply,omitempty"`
	ErrorType        string                        `json:"error_type,omitempty"`
	TraceID          string                        `json:"trace_id,omitempty"`
	TraceParent      string                        `json:"trace_parent,omitempty"`
	WorkerID         string                        `json:"worker_id,omitempty"`
	LeaseUntil       time.Time                     `json:"lease_until,omitempty"`
	StartedAt        time.Time                     `json:"started_at,omitempty"`
	LatencyMS        *int64                        `json:"latency_ms,omitempty"`
	ContinuationOf   string                        `json:"continuation_of,omitempty"`
	ApprovedCalls    []governance.ApprovedToolCall `json:"approved_calls,omitempty"`
	PendingApprovals []ApprovalView                `json:"pending_approvals,omitempty"`
	PromptTokens     int                           `json:"prompt_tokens"`
	CompletionTokens int                           `json:"completion_tokens"`
	Cost             float64                       `json:"cost"`
}
type ApprovalView struct {
	ID            string    `json:"approval_id"`
	Tool          string    `json:"tool_name"`
	Status        string    `json:"status"`
	ArgumentsHash string    `json:"arguments_hash"`
	ExpiresAt     time.Time `json:"expires_at"`
}
type DebugScope struct{ TenantID, AppID, SnapshotID, SessionID, UserID, OwnerID string }
type debugContextKey struct{}

func WithDebugScope(ctx context.Context, scope DebugScope) context.Context {
	return context.WithValue(runtimecontext.WithDebugExecution(ctx), debugContextKey{}, scope)
}
func debugScope(ctx context.Context) (DebugScope, bool) {
	scope, ok := ctx.Value(debugContextKey{}).(DebugScope)
	return scope, ok
}
func Hash(value string) string { b := sha256.Sum256([]byte(value)); return hex.EncodeToString(b[:]) }

// RuntimeRepository only exposes snapshots within an Engine-created context.
// Normal IM routing and publication continue to see the base control plane.
type RuntimeRepository struct {
	controlplane.Repository
	Store *Store
}

func (r *RuntimeRepository) GetRevision(ctx context.Context, tenant, id string) (controlplane.AgentRevision, error) {
	scope, ok := debugScope(ctx)
	if !ok {
		return r.Repository.GetRevision(ctx, tenant, id)
	}
	if scope.TenantID != tenant || scope.SnapshotID != id {
		return controlplane.AgentRevision{}, controlplane.ErrNotFound
	}
	stored, err := r.Store.Get(ctx, "snapshot", tenant, id)
	if err != nil {
		return controlplane.AgentRevision{}, err
	}
	if stored.OwnerID != scope.OwnerID || stored.AppID != scope.AppID {
		return controlplane.AgentRevision{}, controlplane.ErrNotFound
	}
	var snapshot Snapshot
	if json.Unmarshal(stored.Data, &snapshot) != nil || snapshot.Config.ID != id || snapshot.Config.TenantID != tenant || snapshot.Config.AppID != scope.AppID || snapshot.Config.Checksum != controlplane.RevisionChecksum(snapshot.Config) {
		return controlplane.AgentRevision{}, ErrUnavailable
	}
	return snapshot.Config, nil
}

// Debug journals use the existing interfaces and callback/approval policies,
// but never create operational IM bindings, messages or publishable revisions.
type ToolJournal struct{ Store *Store }

func (j *ToolJournal) Start(ctx context.Context, e toolexec.Execution) (toolexec.StartResult, error) {
	if e.TenantID == "" || e.RequestID == "" || e.ToolCallID == "" || e.ToolName == "" || e.RevisionID == "" || e.ArgumentsHash == "" {
		return toolexec.StartResult{}, toolexec.ErrConflict
	}
	id := toolexec.StableID(e.RequestID, e.ToolCallID)
	if e.ID != "" && e.ID != id {
		return toolexec.StartResult{}, toolexec.ErrConflict
	}
	e.ID = id
	e.Status = toolexec.StatusRunning
	e.StartedAt = time.Now().UTC()
	raw, _ := json.Marshal(e)
	_, err := j.Store.Create(ctx, Record{Kind: "tool", TenantID: e.TenantID, AppID: e.RequestID, ID: id, OwnerID: e.RequestID, Status: e.Status, Data: raw, ExpiresAt: time.Now().Add(7 * 24 * time.Hour)})
	if errors.Is(err, ErrConflict) {
		old, getErr := j.Get(ctx, e.TenantID, id)
		if getErr != nil {
			return toolexec.StartResult{}, getErr
		}
		if old.RequestID != e.RequestID || old.ToolCallID != e.ToolCallID || old.ToolName != e.ToolName || old.ArgumentsHash != e.ArgumentsHash || old.RevisionID != e.RevisionID {
			return toolexec.StartResult{}, toolexec.ErrConflict
		}
		return toolexec.StartResult{Execution: old, Existing: true}, nil
	}
	return toolexec.StartResult{Execution: e}, err
}
func (j *ToolJournal) Complete(ctx context.Context, id, status, resultHash, errorType string) error {
	scope, ok := debugScope(ctx)
	if !ok {
		return toolexec.ErrNotFound
	}
	record, err := j.Store.Get(ctx, "tool", scope.TenantID, id)
	if err != nil {
		return err
	}
	var e toolexec.Execution
	if json.Unmarshal(record.Data, &e) != nil {
		return ErrUnavailable
	}
	if status != toolexec.StatusSucceeded && status != toolexec.StatusFailed && status != toolexec.StatusUnknown {
		return toolexec.ErrConflict
	}
	if e.Status != toolexec.StatusRunning {
		if e.Status == status && e.ResultHash == resultHash && e.ErrorType == errorType {
			return nil
		}
		return toolexec.ErrConflict
	}
	e.Status = status
	e.ResultHash = resultHash
	e.ErrorType = errorType
	e.CompletedAt = time.Now().UTC()
	record.Status = status
	record.Data, _ = json.Marshal(e)
	_, err = j.Store.Update(ctx, record, record.Version)
	return err
}
func (j *ToolJournal) Get(ctx context.Context, tenant, id string) (toolexec.Execution, error) {
	record, err := j.Store.Get(ctx, "tool", tenant, id)
	if errors.Is(err, ErrNotFound) {
		err = toolexec.ErrNotFound
	}
	if err != nil {
		return toolexec.Execution{}, err
	}
	var e toolexec.Execution
	err = json.Unmarshal(record.Data, &e)
	return e, err
}
func (j *ToolJournal) ListByRequest(ctx context.Context, tenant, request string) ([]toolexec.Execution, error) {
	records, err := j.Store.List(ctx, Filter{Kind: "tool", TenantID: tenant, AppID: request, Limit: 100})
	if err != nil {
		return nil, err
	}
	out := []toolexec.Execution{}
	for _, record := range records {
		var e toolexec.Execution
		if json.Unmarshal(record.Data, &e) != nil {
			return nil, ErrUnavailable
		}
		out = append(out, e)
	}
	return out, nil
}
func (*ToolJournal) LinkOperation(context.Context, string, string, string) error {
	return toolexec.ErrConflict
}
func (*ToolJournal) ResolveOperation(context.Context, string, string, string, string, string) error {
	return toolexec.ErrConflict
}
func (j *ToolJournal) Ready(ctx context.Context) error { return j.Store.Ready(ctx) }
func (*ToolJournal) Close() error                      { return nil }

type Approvals struct{ Store *Store }

func (a *Approvals) Request(ctx context.Context, in approval.Request) (approval.Record, error) {
	if in.TenantID == "" || in.AppID == "" || in.RequestID == "" || in.ToolCallID == "" || in.ToolName == "" || in.ArgumentsHash == "" || in.UserID == "" || in.SessionID == "" || in.RevisionID == "" || in.ChannelBindingID != DebugBinding {
		return approval.Record{}, approval.ErrForbidden
	}
	if in.ExpiresAt.IsZero() {
		in.ExpiresAt = time.Now().UTC().Add(15 * time.Minute)
	}
	r := approval.Record{ApprovalID: approval.StableID(in.TenantID, in.RequestID, in.ToolCallID), TenantID: in.TenantID, AppID: in.AppID, RevisionID: in.RevisionID, ChannelBindingID: in.ChannelBindingID, RequestID: in.RequestID, MessageID: in.MessageID, UserID: in.UserID, SessionID: in.SessionID, ToolCallID: in.ToolCallID, ToolName: in.ToolName, ArgumentsHash: in.ArgumentsHash, ResumeText: in.ResumeText, ReplyTarget: in.ReplyTarget, Status: approval.StatusPending, ExpiresAt: in.ExpiresAt, CreatedAt: time.Now().UTC()}
	raw, _ := json.Marshal(r)
	_, err := a.Store.Create(ctx, Record{Kind: "approval", TenantID: r.TenantID, AppID: r.RequestID, ID: r.ApprovalID, OwnerID: r.UserID, Status: r.Status, Data: raw, ExpiresAt: time.Now().Add(7 * 24 * time.Hour)})
	if errors.Is(err, ErrConflict) {
		old, getErr := a.Get(ctx, r.TenantID, r.ApprovalID)
		if getErr != nil {
			return r, getErr
		}
		if old.ArgumentsHash != r.ArgumentsHash || old.ToolName != r.ToolName || old.UserID != r.UserID || old.SessionID != r.SessionID || old.RevisionID != r.RevisionID {
			return r, approval.ErrConflict
		}
		return old, nil
	}
	return r, err
}
func (a *Approvals) Get(ctx context.Context, tenant, id string) (approval.Record, error) {
	record, err := a.Store.Get(ctx, "approval", tenant, id)
	if errors.Is(err, ErrNotFound) {
		err = approval.ErrNotFound
	}
	if err != nil {
		return approval.Record{}, err
	}
	var result approval.Record
	err = json.Unmarshal(record.Data, &result)
	return result, err
}
func (a *Approvals) pending(ctx context.Context, f Filter) ([]approval.Record, error) {
	records, err := a.Store.List(ctx, f)
	if err != nil {
		return nil, err
	}
	out := []approval.Record{}
	for _, record := range records {
		var value approval.Record
		if json.Unmarshal(record.Data, &value) != nil {
			return nil, ErrUnavailable
		}
		if value.Status == approval.StatusPending && value.ExpiresAt.After(time.Now()) {
			out = append(out, value)
		}
	}
	return out, nil
}
func (a *Approvals) ListPendingByRequest(ctx context.Context, tenant, request string) ([]approval.Record, error) {
	return a.pending(ctx, Filter{Kind: "approval", TenantID: tenant, AppID: request, Status: approval.StatusPending, Limit: 100})
}
func (a *Approvals) ListPendingBySession(ctx context.Context, tenant, binding, user, session string) ([]approval.Record, error) {
	if binding != DebugBinding {
		return nil, approval.ErrForbidden
	}
	items, err := a.pending(ctx, Filter{Kind: "approval", TenantID: tenant, OwnerID: user, Status: approval.StatusPending, Limit: 100})
	if err != nil {
		return nil, err
	}
	out := []approval.Record{}
	for _, r := range items {
		if r.SessionID == session {
			out = append(out, r)
		}
	}
	return out, nil
}
func (a *Approvals) Decide(ctx context.Context, in approval.Decision) (approval.Record, error) {
	if in.Status != approval.StatusApproved && in.Status != approval.StatusDenied || in.ExternalMessageID == "" {
		return approval.Record{}, approval.ErrConflict
	}
	record, err := a.Store.Get(ctx, "approval", in.TenantID, in.ApprovalID)
	if err != nil {
		return approval.Record{}, err
	}
	var value approval.Record
	if json.Unmarshal(record.Data, &value) != nil {
		return value, ErrUnavailable
	}
	if value.ChannelBindingID != in.ChannelBindingID || value.UserID != in.UserID || value.SessionID != in.SessionID || value.TenantID != in.TenantID {
		return value, approval.ErrForbidden
	}
	if value.Status == approval.StatusPending && !value.ExpiresAt.After(time.Now()) {
		return value, approval.ErrExpired
	}
	if value.Status != approval.StatusPending && value.Status != in.Status {
		return value, approval.ErrConflict
	}
	marker := Record{Kind: "decision", TenantID: in.TenantID, AppID: in.ChannelBindingID, OwnerID: in.UserID, ID: Hash(in.ChannelBindingID + "\x00" + in.ExternalMessageID), Status: in.Status, ExpiresAt: record.ExpiresAt}
	marker.Data, _ = json.Marshal(map[string]string{"approval_id": in.ApprovalID, "status": in.Status})
	if _, err := a.Store.Create(ctx, marker); errors.Is(err, ErrConflict) {
		old, e := a.Store.Get(ctx, "decision", in.TenantID, marker.ID)
		var existing map[string]string
		if e != nil || json.Unmarshal(old.Data, &existing) != nil || existing["approval_id"] != in.ApprovalID || existing["status"] != in.Status {
			return value, approval.ErrConflict
		}
	} else if err != nil {
		return value, err
	}
	if value.Status == in.Status {
		return value, nil
	}
	value.Status = in.Status
	value.DecisionMessageID = in.ExternalMessageID
	value.DecisionReason = "console user decision"
	value.DecidedAt = time.Now().UTC()
	record.Status = in.Status
	record.Data, _ = json.Marshal(value)
	_, err = a.Store.Update(ctx, record, record.Version)
	return value, err
}
func (a *Approvals) MarkResumed(ctx context.Context, id string) error {
	scope, ok := debugScope(ctx)
	if !ok {
		return approval.ErrForbidden
	}
	record, err := a.Store.Get(ctx, "approval", scope.TenantID, id)
	if err != nil {
		return err
	}
	var value approval.Record
	_ = json.Unmarshal(record.Data, &value)
	if value.Status != approval.StatusApproved {
		return approval.ErrForbidden
	}
	value.ResumedAt = time.Now().UTC()
	record.Data, _ = json.Marshal(value)
	_, err = a.Store.Update(ctx, record, record.Version)
	return err
}
func (a *Approvals) Ready(ctx context.Context) error { return a.Store.Ready(ctx) }
func (*Approvals) Close() error                      { return nil }

type RoutedApprovals struct {
	approval.Repository
	Debug *Approvals
}

func (r RoutedApprovals) Request(ctx context.Context, in approval.Request) (approval.Record, error) {
	if _, ok := debugScope(ctx); ok {
		return r.Debug.Request(ctx, in)
	}
	return r.Repository.Request(ctx, in)
}

type RoutedJournal struct {
	toolexec.Journal
	Debug *ToolJournal
}

func (r RoutedJournal) Start(ctx context.Context, e toolexec.Execution) (toolexec.StartResult, error) {
	if _, ok := debugScope(ctx); ok {
		return r.Debug.Start(ctx, e)
	}
	return r.Journal.Start(ctx, e)
}
func (r RoutedJournal) Complete(ctx context.Context, id, status, result, errType string) error {
	if _, ok := debugScope(ctx); ok {
		return r.Debug.Complete(ctx, id, status, result, errType)
	}
	return r.Journal.Complete(ctx, id, status, result, errType)
}
func (r RoutedJournal) Get(ctx context.Context, tenant, id string) (toolexec.Execution, error) {
	if _, ok := debugScope(ctx); ok {
		return r.Debug.Get(ctx, tenant, id)
	}
	return r.Journal.Get(ctx, tenant, id)
}
func (r RoutedJournal) LinkOperation(ctx context.Context, tenant, id, op string) error {
	if _, ok := debugScope(ctx); ok {
		return toolexec.ErrConflict
	}
	return r.Journal.LinkOperation(ctx, tenant, id, op)
}
func (r RoutedJournal) ResolveOperation(ctx context.Context, tenant, op, status, result, errType string) error {
	if _, ok := debugScope(ctx); ok {
		return toolexec.ErrConflict
	}
	return r.Journal.ResolveOperation(ctx, tenant, op, status, result, errType)
}

type Runtime interface {
	ChatWithScope(context.Context, agentruntime.ChatInput) (agentruntime.ChatResult, error)
}
