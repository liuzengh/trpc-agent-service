package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrExecutionTraceNotFound = errors.New("execution trace not found")

type ExecutionTraceUsage struct {
	PromptTokens        int `json:"prompt_tokens"`
	CompletionTokens    int `json:"completion_tokens"`
	TotalTokens         int `json:"total_tokens"`
	CachedTokens        int `json:"cached_tokens,omitempty"`
	CacheCreationTokens int `json:"cache_creation_tokens,omitempty"`
	CacheReadTokens     int `json:"cache_read_tokens,omitempty"`
	ReasoningTokens     int `json:"reasoning_tokens,omitempty"`
}

type ExecutionTraceStep struct {
	StepID             string               `json:"step_id"`
	InvocationID       string               `json:"invocation_id,omitempty"`
	ParentInvocationID string               `json:"parent_invocation_id,omitempty"`
	AgentName          string               `json:"agent_name,omitempty"`
	Branch             string               `json:"branch,omitempty"`
	NodeID             string               `json:"node_id,omitempty"`
	NodeType           string               `json:"node_type,omitempty"`
	StartedAt          time.Time            `json:"started_at"`
	EndedAt            time.Time            `json:"ended_at"`
	PredecessorStepIDs []string             `json:"predecessor_step_ids,omitempty"`
	AppliedSurfaceIDs  []string             `json:"applied_surface_ids,omitempty"`
	Usage              *ExecutionTraceUsage `json:"usage,omitempty"`
	Failed             bool                 `json:"failed,omitempty"`
}

type AgentExecutionTrace struct {
	Status           string               `json:"status"`
	RootAgentName    string               `json:"root_agent_name,omitempty"`
	RootInvocationID string               `json:"root_invocation_id,omitempty"`
	SessionID        string               `json:"session_id,omitempty"`
	StartedAt        time.Time            `json:"started_at"`
	EndedAt          time.Time            `json:"ended_at"`
	Usage            *ExecutionTraceUsage `json:"usage,omitempty"`
	Steps            []ExecutionTraceStep `json:"steps"`
}

type ExecutionTraceRecord struct {
	TenantID  string              `json:"tenant_id"`
	AppCode   string              `json:"app_code"`
	Channel   string              `json:"channel"`
	BindingID string              `json:"binding_id"`
	MessageID string              `json:"message_id"`
	TraceID   string              `json:"trace_id"`
	Trace     AgentExecutionTrace `json:"trace"`
}

// ExecutionTraceRef identifies one persisted execution graph for a list lookup.
type ExecutionTraceRef struct {
	Channel   string
	BindingID string
	MessageID string
}

func validateExecutionTraceRecord(record ExecutionTraceRecord) error {
	if strings.TrimSpace(record.TenantID) == "" || strings.TrimSpace(record.AppCode) == "" || strings.TrimSpace(record.Channel) == "" || strings.TrimSpace(record.BindingID) == "" || strings.TrimSpace(record.MessageID) == "" || strings.TrimSpace(record.TraceID) == "" {
		return fmt.Errorf("execution trace tenant, application, channel, binding, message, and trace IDs are required")
	}
	switch record.Trace.Status {
	case "completed", "incomplete", "failed":
	default:
		return fmt.Errorf("execution trace status %q is invalid", record.Trace.Status)
	}
	return nil
}

func executionTraceKey(tenantID, channel, bindingID, messageID string) string {
	return tenantID + "\x00" + channel + "\x00" + bindingID + "\x00" + messageID
}

func cloneExecutionTraceUsage(usage *ExecutionTraceUsage) *ExecutionTraceUsage {
	if usage == nil {
		return nil
	}
	cloned := *usage
	return &cloned
}

func cloneExecutionTraceRecord(record ExecutionTraceRecord) ExecutionTraceRecord {
	cloned := record
	cloned.Trace.Usage = cloneExecutionTraceUsage(record.Trace.Usage)
	cloned.Trace.Steps = make([]ExecutionTraceStep, len(record.Trace.Steps))
	for index, step := range record.Trace.Steps {
		cloned.Trace.Steps[index] = step
		cloned.Trace.Steps[index].PredecessorStepIDs = append([]string(nil), step.PredecessorStepIDs...)
		cloned.Trace.Steps[index].AppliedSurfaceIDs = append([]string(nil), step.AppliedSurfaceIDs...)
		cloned.Trace.Steps[index].Usage = cloneExecutionTraceUsage(step.Usage)
	}
	return cloned
}

func (s *MemoryStateStore) RecordExecutionTrace(ctx context.Context, record ExecutionTraceRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateExecutionTraceRecord(record); err != nil {
		return err
	}
	s.mu.Lock()
	s.traces[executionTraceKey(record.TenantID, record.Channel, record.BindingID, record.MessageID)] = cloneExecutionTraceRecord(record)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStateStore) GetExecutionTrace(ctx context.Context, tenantID, channel, bindingID, messageID string) (ExecutionTraceRecord, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionTraceRecord{}, err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(messageID) == "" {
		return ExecutionTraceRecord{}, fmt.Errorf("execution trace tenant, channel, binding, and message IDs are required")
	}
	s.mu.RLock()
	record, ok := s.traces[executionTraceKey(tenantID, channel, bindingID, messageID)]
	s.mu.RUnlock()
	if !ok {
		return ExecutionTraceRecord{}, ErrExecutionTraceNotFound
	}
	return cloneExecutionTraceRecord(record), nil
}

func (s *MemoryStateStore) ListExecutionTraces(ctx context.Context, tenantID string, refs []ExecutionTraceRef) ([]ExecutionTraceRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("execution trace tenant ID is required")
	}
	if len(refs) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	records := make([]ExecutionTraceRecord, 0, len(refs))
	for _, ref := range refs {
		record, ok := s.traces[executionTraceKey(tenantID, ref.Channel, ref.BindingID, ref.MessageID)]
		if !ok {
			continue
		}
		records = append(records, cloneExecutionTraceRecord(record))
	}
	return records, nil
}

type executionTraceExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func writeExecutionTrace(ctx context.Context, execer executionTraceExecer, record ExecutionTraceRecord) error {
	if err := validateExecutionTraceRecord(record); err != nil {
		return err
	}
	projection, err := json.Marshal(record.Trace)
	if err != nil {
		return fmt.Errorf("encode execution trace projection: %w", err)
	}
	_, err = execer.ExecContext(ctx, `
INSERT INTO execution_traces (tenant_id, app_code, channel_type, binding_id, message_id, trace_id, projection, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,NOW())
ON CONFLICT (tenant_id, channel_type, binding_id, message_id) DO UPDATE
SET app_code=EXCLUDED.app_code, trace_id=EXCLUDED.trace_id, projection=EXCLUDED.projection, updated_at=NOW()`,
		record.TenantID, record.AppCode, record.Channel, record.BindingID, record.MessageID, record.TraceID, string(projection))
	if err != nil {
		return fmt.Errorf("upsert execution trace: %w", err)
	}
	return nil
}

func (s *PostgresStateStore) RecordExecutionTrace(ctx context.Context, record ExecutionTraceRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeExecutionTrace(ctx, s.database, record)
}

func (s *PostgresStateStore) ListExecutionTraces(ctx context.Context, tenantID string, refs []ExecutionTraceRef) ([]ExecutionTraceRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("execution trace tenant ID is required")
	}
	if len(refs) == 0 {
		return nil, nil
	}
	query := strings.Builder{}
	args := make([]any, 0, 1+len(refs)*3)
	query.WriteString(`
SELECT tenant_id, app_code, channel_type, binding_id, message_id, trace_id, projection
FROM execution_traces
WHERE tenant_id=$1 AND (channel_type, binding_id, message_id) IN (`)
	args = append(args, tenantID)
	for index, ref := range refs {
		if index > 0 {
			query.WriteString(",")
		}
		query.WriteString(fmt.Sprintf("($%d,$%d,$%d)", len(args)+1, len(args)+2, len(args)+3))
		args = append(args, ref.Channel, ref.BindingID, ref.MessageID)
	}
	query.WriteString(")")
	rows, err := s.database.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list execution traces: %w", err)
	}
	defer rows.Close()
	records := make([]ExecutionTraceRecord, 0, len(refs))
	for rows.Next() {
		var record ExecutionTraceRecord
		var projection []byte
		if err := rows.Scan(&record.TenantID, &record.AppCode, &record.Channel, &record.BindingID, &record.MessageID, &record.TraceID, &projection); err != nil {
			return nil, fmt.Errorf("scan execution traces: %w", err)
		}
		if err := json.Unmarshal(projection, &record.Trace); err != nil {
			return nil, fmt.Errorf("decode execution trace projection: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution traces: %w", err)
	}
	return records, nil
}

func (s *PostgresStateStore) GetExecutionTrace(ctx context.Context, tenantID, channel, bindingID, messageID string) (ExecutionTraceRecord, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(messageID) == "" {
		return ExecutionTraceRecord{}, fmt.Errorf("execution trace tenant, channel, binding, and message IDs are required")
	}
	var record ExecutionTraceRecord
	var projection []byte
	err := s.database.QueryRowContext(ctx, `
SELECT tenant_id, app_code, channel_type, binding_id, message_id, trace_id, projection
FROM execution_traces
WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND message_id=$4`, tenantID, channel, bindingID, messageID).Scan(
		&record.TenantID, &record.AppCode, &record.Channel, &record.BindingID, &record.MessageID, &record.TraceID, &projection)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionTraceRecord{}, ErrExecutionTraceNotFound
	}
	if err != nil {
		return ExecutionTraceRecord{}, fmt.Errorf("get execution trace: %w", err)
	}
	if err := json.Unmarshal(projection, &record.Trace); err != nil {
		return ExecutionTraceRecord{}, fmt.Errorf("decode execution trace projection: %w", err)
	}
	return record, nil
}
