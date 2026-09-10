// Package postgres implements AtomicSessionStore with one PostgreSQL
// commit_turn transaction function as the business-effect boundary.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type DB interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type Store struct {
	db        DB
	telemetry telemetry.Provider
}

func New(db DB) *Store { return NewWithTelemetry(db, nil) }

func NewWithTelemetry(db DB, provider telemetry.Provider) *Store {
	return &Store{db: db, telemetry: telemetry.OrNoop(provider)}
}

func (s *Store) OpenForRun(ctx context.Context, in sessionstore.OpenForRunRequest) (head sessionstore.SessionHead, resultErr error) {
	ctx, done := s.measure(ctx, telemetry.OperationSessionOpen)
	defer func() { done(resultErr) }()
	if in.TenantID == "" || in.AgentAppID == "" || in.SessionID == "" {
		return sessionstore.SessionHead{}, runtime.ErrTenantScope
	}
	if in.RequestID == "" || in.InputSeq < 1 || in.Fence < 1 {
		return sessionstore.SessionHead{}, runtime.ErrCommitConflict
	}
	var state []byte
	head.SessionKey = in.SessionKey
	err := s.db.QueryRowContext(ctx, `SELECT h.version,h.last_fence,h.last_session_seq,h.next_input_seq,h.state_json
FROM session_head h JOIN execution_record e ON e.tenant_id=h.tenant_id AND e.agent_app_id=h.agent_app_id AND e.session_id=h.session_id
WHERE h.tenant_id=$1 AND h.agent_app_id=$2 AND h.session_id=$3 AND e.request_id=$4 AND e.input_seq=$5`, in.TenantID, in.AgentAppID, in.SessionID, in.RequestID, in.InputSeq).
		Scan(&head.Version, &head.LastFence, &head.LastSessionSeq, &head.NextInputSeq, &state)
	if err != nil {
		return sessionstore.SessionHead{}, translate(err)
	}
	if err := json.Unmarshal(state, &head.State); err != nil {
		return sessionstore.SessionHead{}, runtime.ErrInvariantViolation
	}
	if in.InputSeq < head.NextInputSeq {
		return head, runtime.ErrAlreadyTerminal
	}
	if in.InputSeq > head.NextInputSeq {
		return head, runtime.ErrInputNotReady
	}
	if in.Fence < head.LastFence {
		return head, runtime.ErrStaleFence
	}
	return head, nil
}

type eventJSON struct {
	EventID, EventType, PayloadRef string
}
type summaryJSON struct {
	SummaryID      string `json:"summary_id"`
	BaseSessionSeq uint64 `json:"base_session_seq"`
	LastEventID    string `json:"last_event_id"`
	CutoffAt       string `json:"cutoff_at"`
	ContentRef     string `json:"content_ref"`
}
type outboxJSON struct {
	Kind, IdempotencyKey, PayloadRef, TraceParent string
	EventSeq                                      uint64
}

func (s *Store) CommitTurn(ctx context.Context, in sessionstore.CommitTurnRequest) (result sessionstore.CommitTurnResult, resultErr error) {
	ctx, done := s.measure(ctx, telemetry.OperationSessionCommit)
	defer func() { done(resultErr) }()
	if err := sessionstore.ValidateCommit(in); err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	digest, err := sessionstore.CommitDigest(in)
	if err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	events := make([]map[string]any, 0, len(in.Events))
	for _, event := range in.Events {
		wrapper, err := json.Marshal(map[string]any{"ref": event.PayloadRef, "payload": event.Payload})
		if err != nil {
			return sessionstore.CommitTurnResult{}, err
		}
		events = append(events, map[string]any{"event_id": event.EventID, "event_type": event.EventType, "payload_ref": string(wrapper), "event_seq": event.EventSeq})
	}
	outboxes := make([]map[string]any, 0, len(in.Outbox))
	for _, event := range in.Outbox {
		outboxes = append(outboxes, map[string]any{"kind": event.Kind, "idempotency_key": event.IdempotencyKey, "payload_ref": event.PayloadRef, "traceparent": event.TraceParent, "event_seq": event.EventSeq})
	}
	eventBytes, err := json.Marshal(events)
	if err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	stateDelta := in.StateDelta
	if stateDelta == nil {
		stateDelta = sessionstore.StateDelta{}
	}
	stateBytes, err := json.Marshal(stateDelta)
	if err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	summaryBytes := []byte("null")
	if in.SummaryCandidate != nil {
		summaryBytes, err = json.Marshal(summaryJSON{SummaryID: in.SummaryCandidate.SummaryID, BaseSessionSeq: in.SummaryCandidate.BaseSessionSeq, LastEventID: in.SummaryCandidate.LastEventID, CutoffAt: in.SummaryCandidate.CutoffAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), ContentRef: in.SummaryCandidate.ContentRef})
		if err != nil {
			return sessionstore.CommitTurnResult{}, err
		}
	}
	outboxBytes, err := json.Marshal(outboxes)
	if err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	var resultRef, replyCursor sql.NullString
	var alreadyTerminal bool
	err = s.db.QueryRowContext(ctx, `SELECT commit_id,outcome,input_seq,session_version,result_ref,reply_cursor,already_terminal
FROM commit_turn($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14::jsonb,$15,$16,$17::jsonb)`,
		in.TenantID, in.AgentAppID, in.SessionID, in.RequestID, in.CommitID, digest, in.Stage,
		in.InputSeq, in.Fence, in.ExpectedVersion, string(in.Outcome), eventBytes, stateBytes,
		summaryBytes, nullable(in.ResultRef), nullable(in.ReplyCursor), outboxBytes).
		Scan(&result.CommitID, &result.Outcome, &result.InputSeq, &result.SessionVersion, &resultRef, &replyCursor, &alreadyTerminal)
	if err != nil {
		return sessionstore.CommitTurnResult{}, translate(err)
	}
	result.ResultRef, result.ReplyCursor = resultRef.String, replyCursor.String
	if alreadyTerminal {
		return result, runtime.ErrAlreadyTerminal
	}
	return result, nil
}

func (s *Store) GetTerminalByInputSeq(ctx context.Context, key sessionstore.TerminalKey) (result sessionstore.CommitTurnResult, resultErr error) {
	ctx, done := s.measure(ctx, telemetry.OperationSessionTerminal)
	defer func() { done(resultErr) }()
	err := s.db.QueryRowContext(ctx, `SELECT commit_id,outcome,input_seq,session_version,COALESCE(result_ref,''),COALESCE(reply_cursor,'')
FROM session_commit WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 AND input_seq=$4
AND outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')`,
		key.TenantID, key.AgentAppID, key.SessionID, key.InputSeq).
		Scan(&result.CommitID, &result.Outcome, &result.InputSeq, &result.SessionVersion, &result.ResultRef, &result.ReplyCursor)
	if err != nil {
		return sessionstore.CommitTurnResult{}, translate(err)
	}
	return result, nil
}

func (s *Store) ReadLastFence(ctx context.Context, key sessionstore.SessionKey) (fence uint64, resultErr error) {
	ctx, done := s.measure(ctx, telemetry.OperationSessionReadFence)
	defer func() { done(resultErr) }()
	err := s.db.QueryRowContext(ctx, `SELECT last_fence FROM session_head WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`, key.TenantID, key.AgentAppID, key.SessionID).Scan(&fence)
	if err != nil {
		return 0, translate(err)
	}
	return fence, nil
}

func (s *Store) LoadSession(ctx context.Context, key sessionstore.SessionKey) (snapshot sessionstore.SessionSnapshot, resultErr error) {
	ctx, done := s.measure(ctx, telemetry.OperationSessionLoad)
	defer func() { done(resultErr) }()
	snapshot.Head.SessionKey = key
	var state []byte
	err := s.db.QueryRowContext(ctx, `SELECT version,last_fence,last_session_seq,next_input_seq,state_json FROM session_head WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`, key.TenantID, key.AgentAppID, key.SessionID).
		Scan(&snapshot.Head.Version, &snapshot.Head.LastFence, &snapshot.Head.LastSessionSeq, &snapshot.Head.NextInputSeq, &state)
	if err != nil {
		return sessionstore.SessionSnapshot{}, translate(err)
	}
	if err := json.Unmarshal(state, &snapshot.Head.State); err != nil {
		return sessionstore.SessionSnapshot{}, runtime.ErrInvariantViolation
	}
	rows, err := s.db.QueryContext(ctx, `SELECT event_payload FROM session_event WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 ORDER BY session_seq`, key.TenantID, key.AgentAppID, key.SessionID)
	if err != nil {
		return sessionstore.SessionSnapshot{}, translate(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return sessionstore.SessionSnapshot{}, err
		}
		snapshot.Events = append(snapshot.Events, append([]byte(nil), raw...))
	}
	return snapshot, rows.Err()
}

func (s *Store) measure(ctx context.Context, operation telemetry.Operation) (context.Context, func(error)) {
	started := time.Now()
	ctx, finish := telemetry.StartOperation(ctx, s.telemetry, "", operation)
	return ctx, func(err error) {
		outcome := telemetry.OutcomeSuccess
		if err != nil {
			outcome = telemetry.OutcomeError
		}
		s.telemetry.Histogram(telemetry.MetricSessionBackendDuration).Record(ctx, time.Since(started).Seconds(),
			telemetry.DestinationAttribute(telemetry.DestinationPostgreSQL), telemetry.OperationAttribute(operation), telemetry.OutcomeAttribute(outcome))
		finish(err)
	}
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			if strings.Contains(strings.ToLower(err.Error()), "stale fence") {
				return runtime.ErrStaleFence
			}
			return runtime.ErrVersionConflict
		case "23505":
			return runtime.ErrCommitConflict
		case "XX001":
			return runtime.ErrInvariantViolation
		case "55000":
			return runtime.ErrInputNotReady
		case "P0902":
			return runtime.ErrCancelRequested
		case "42501":
			return runtime.ErrTenantScope
		}
	}
	return err
}

var _ sessionstore.AtomicSessionStore = (*Store)(nil)
