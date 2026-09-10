package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const (
	executionEventPollInterval  = 200 * time.Millisecond
	executionStreamRetryInitial = 100 * time.Millisecond
	executionStreamRetryMax     = time.Second
	executionStreamRetryWindow  = 30 * time.Second
)

// ExecutionEventJournal persists runner events and exposes them as a durable,
// tenant-scoped stream for protocol adapters.
type ExecutionEventJournal struct {
	store        *Store
	replyBuilder func(context.Context, worker.Execution, int64, *event.Event) ([]channels.Reply, error)
}

// ExecutionEventJournalOption configures optional durable event projections.
type ExecutionEventJournalOption func(*ExecutionEventJournal) error

// WithReplyEventBuilder enables the IM Reply Projection inside the same
// PostgreSQL transaction as execution_event insertion.
func WithReplyEventBuilder(builder func(context.Context, worker.Execution, int64, *event.Event) ([]channels.Reply, error)) ExecutionEventJournalOption {
	return func(journal *ExecutionEventJournal) error {
		if builder == nil {
			return errors.New("reply event builder is required")
		}
		journal.replyBuilder = builder
		return nil
	}
}

// NewExecutionEventJournal creates a journal backed by Store's PostgreSQL
// database. The store remains owned by the caller.
func NewExecutionEventJournal(store *Store, options ...ExecutionEventJournalOption) (*ExecutionEventJournal, error) {
	if store == nil {
		return nil, errors.New("postgres store is required")
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	journal := &ExecutionEventJournal{store: store}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("execution event journal option is required")
		}
		if err := option(journal); err != nil {
			return nil, fmt.Errorf("configure execution event journal: %w", err)
		}
	}
	return journal, nil
}

// HandleRunnerEvent appends one real Runner event. It serializes concurrent
// appends and revalidates the active execution run token in the same
// transaction.
func (j *ExecutionEventJournal) HandleRunnerEvent(
	ctx context.Context,
	exec worker.Execution,
	evt *event.Event,
) error {
	if j == nil || j.store == nil {
		return errors.New("execution event journal is not initialized")
	}
	if err := j.store.validate(); err != nil {
		return err
	}
	if evt == nil {
		return errors.New("runner event is required")
	}
	if exec.RequestID == "" {
		return errors.New("execution request_id is required")
	}
	if err := exec.Tenant.Validate(); err != nil {
		return fmt.Errorf("execution tenant context: %w", err)
	}
	// Keep the framework's original event for internal execution, but only
	// externalize the sanitized copy to the durable journal and reply outbox.
	sanitized, _ := guardrail.SanitizeEvent(evt)
	evt = sanitized
	lease, ok := worker.JobLeaseFromContext(ctx)
	if !ok {
		return errors.New("execution event requires a current execution lease")
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal execution event: %w", err)
	}
	eventID := evt.ID
	if eventID == "" {
		digest := sha256.Sum256(payload)
		eventID = fmt.Sprintf("payload:%x", digest)
	}

	tx, err := j.store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin execution event append: %w", err)
	}
	defer func() {
		rollback(tx)
	}()
	var executionAttempt int
	if err := tx.QueryRow(
		ctx,
		`SELECT 1
FROM platform.execution
WHERE tenant_id = $1
  AND app_id = $2
  AND request_id = $3
  AND status = 'RUNNING'
  AND lease_owner = $4
  AND run_token = $5
  AND lease_until > clock_timestamp()
  AND session_principal_id = $6
  AND session_id = $7
  AND user_id = $8
  AND config_version = $9
FOR UPDATE`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
		lease.Owner,
		lease.Token,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		exec.Tenant.UserID,
		exec.Tenant.ConfigVersion,
	).Scan(&executionAttempt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lock execution for event append: %w", queue.ErrLeaseLost)
		}
		return fmt.Errorf("lock execution for event append: %w", err)
	}
	var existingSequence int64
	err = tx.QueryRow(ctx, `
SELECT event_seq
FROM platform.execution_event
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND event_id = $4`,
		exec.Tenant.TenantID, exec.Tenant.AppID, exec.RequestID, eventID,
	).Scan(&existingSequence)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit duplicate execution event: %w", err)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check duplicate execution event: %w", err)
	}
	var sequence int64
	if err := tx.QueryRow(
		ctx,
		`SELECT COALESCE(MAX(event_seq), 0) + 1
FROM platform.execution_event
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
	).Scan(&sequence); err != nil {
		return fmt.Errorf("next execution event sequence: %w", err)
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO platform.execution_event (
    tenant_id, app_id, request_id, event_seq, event_id, event_type, payload
) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
		sequence,
		eventID,
		executionEventType(evt),
		payload,
	); err != nil {
		return fmt.Errorf("insert execution event: %w", err)
	}
	if j.replyBuilder != nil {
		replyEvent, err := j.replyProjectionEvent(ctx, tx, exec, sequence, evt)
		if err != nil {
			return fmt.Errorf("resolve reply projection event: %w", err)
		}
		replies, err := j.replyBuilder(ctx, exec, sequence, replyEvent)
		if err != nil {
			return fmt.Errorf("build reply projection: %w", err)
		}
		if err := insertReplyOutboxTx(
			ctx,
			tx,
			exec.Tenant.TenantID,
			exec.Tenant.AppID,
			exec.Tenant.BindingID,
			exec.RequestID,
			replies,
		); err != nil {
			return fmt.Errorf("insert reply outbox: %w", err)
		}
	}
	if exec.TerminalStatus != "" {
		if err := exec.TerminalStatus.Validate(); err != nil {
			return fmt.Errorf("terminal execution status: %w", err)
		}
		lastError := ""
		if exec.TerminalStatus == queue.CompletionFailed {
			lastError = platformlog.SafeError(evt.Error)
		}
		result, err := tx.Exec(ctx, `
UPDATE platform.execution
SET status = $10,
    last_error = $11,
    lease_owner = NULL,
    run_token = NULL,
    lease_until = NULL,
    finished_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE tenant_id = $1
  AND app_id = $2
  AND request_id = $3
  AND status = 'RUNNING'
  AND lease_owner = $4
  AND run_token = $5
  AND lease_until > clock_timestamp()
  AND session_principal_id = $6
  AND session_id = $7
  AND user_id = $8
  AND config_version = $9`,
			exec.Tenant.TenantID,
			exec.Tenant.AppID,
			exec.RequestID,
			lease.Owner,
			lease.Token,
			exec.Tenant.SessionPrincipalID,
			exec.Tenant.SessionID,
			exec.Tenant.UserID,
			exec.Tenant.ConfigVersion,
			exec.TerminalStatus,
			lastError,
		)
		if err != nil {
			return fmt.Errorf("update terminal execution: %w", err)
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("update terminal execution: %w", queue.ErrLeaseLost)
		}
		if err := recordExecutionUsageTx(
			ctx, tx,
			exec.Tenant.TenantID,
			exec.Tenant.AppID,
			exec.RequestID,
			exec.Tenant.ConfigVersion,
			executionAttempt,
			exec.Usage,
			true,
		); err != nil {
			return fmt.Errorf("record terminal execution usage: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit execution event append: %w", err)
	}
	return nil
}

// replyProjectionEvent preserves the terminal runner event as the durable
// event while supplying its preceding final assistant response to the reply
// projector. The framework's runner.completion event intentionally contains
// completion metadata only; the user-visible choices are on the preceding
// chat.completion event.
func (j *ExecutionEventJournal) replyProjectionEvent(
	ctx context.Context,
	tx pgx.Tx,
	exec worker.Execution,
	sequence int64,
	completion *event.Event,
) (*event.Event, error) {
	if completion == nil || !completion.IsRunnerCompletion() ||
		completion.Error != nil || completion.IsTerminalError() ||
		hasAssistantReplyText(completion) {
		return completion, nil
	}
	rows, err := tx.Query(ctx, `
SELECT payload
FROM platform.execution_event
WHERE tenant_id = $1
  AND app_id = $2
  AND request_id = $3
  AND event_seq < $4
  AND event_type = $5
ORDER BY event_seq DESC`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
		sequence,
		model.ObjectTypeChatCompletion,
	)
	if err != nil {
		return nil, fmt.Errorf("query final assistant event: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan final assistant event: %w", err)
		}
		candidate := &event.Event{}
		if err := json.Unmarshal(payload, candidate); err != nil {
			return nil, fmt.Errorf("decode final assistant event: %w", err)
		}
		if candidate.RequestID != "" && candidate.RequestID != exec.RequestID {
			continue
		}
		if candidate.Response == nil || candidate.Object != model.ObjectTypeChatCompletion ||
			!candidate.Done || candidate.IsPartial || !hasAssistantReplyText(candidate) {
			continue
		}
		merged := *completion
		response := *completion.Response
		response.Choices = append([]model.Choice(nil), candidate.Response.Choices...)
		merged.Response = &response
		return &merged, nil
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate final assistant events: %w", err)
	}
	return completion, nil
}

func hasAssistantReplyText(evt *event.Event) bool {
	if evt == nil || evt.Response == nil {
		return false
	}
	for _, choice := range evt.Response.Choices {
		if choice.Message.Role != "" && choice.Message.Role != model.RoleAssistant {
			continue
		}
		if choice.Message.Content != "" {
			return true
		}
	}
	return false
}

// SubscribeExecutionEvents streams persisted events after afterSequence. The
// stream closes after a terminal execution completion or terminal-error event,
// or when the caller cancels ctx. A Runner completion that parks an execution
// in WAITING_APPROVAL is not terminal: keep the durable stream open so the
// original client can receive the approved continuation.
func (j *ExecutionEventJournal) SubscribeExecutionEvents(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
	afterSequence int64,
) (<-chan gateway.ExecutionEvent, error) {
	if j == nil || j.store == nil {
		return nil, errors.New("execution event journal is not initialized")
	}
	if err := j.store.validate(); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if requestID == "" {
		return nil, errors.New("request_id is required")
	}
	if afterSequence < 0 {
		return nil, errors.New("execution event sequence cannot be negative")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	exists, err := j.executionExists(ctx, scope, requestID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("execution: %w", ErrNotFound)
	}
	events := make(chan gateway.ExecutionEvent)
	go j.streamExecutionEvents(ctx, events, scope, requestID, afterSequence)
	return events, nil
}

func (j *ExecutionEventJournal) streamExecutionEvents(
	ctx context.Context,
	output chan<- gateway.ExecutionEvent,
	scope tenant.Scope,
	requestID string,
	afterSequence int64,
) {
	defer close(output)
	retry := executionStreamRetryState{}
	for {
		items, err := j.executionEventsAfter(ctx, scope, requestID, afterSequence)
		if err != nil {
			if j.retryExecutionStreamRead(ctx, &retry, err) {
				continue
			}
			sendExecutionStreamError(ctx, output, requestID, afterSequence, err)
			return
		}
		retry.reset()
		for _, item := range items {
			select {
			case <-ctx.Done():
				return
			case output <- item:
			}
			afterSequence = item.Sequence
			if item.Event.IsTerminalError() {
				return
			}
		}
		status, err := j.executionStatus(ctx, scope, requestID)
		if err != nil {
			if j.retryExecutionStreamRead(ctx, &retry, err) {
				continue
			}
			sendExecutionStreamError(ctx, output, requestID, afterSequence, err)
			return
		}
		retry.reset()
		switch status {
		case "FAILED", "UNCERTAIN", "CANCELED":
			terminal := event.NewErrorEvent(
				requestID,
				"platform",
				"execution_"+strings.ToLower(status),
				"execution did not complete",
			)
			select {
			case <-ctx.Done():
				return
			case output <- gateway.ExecutionEvent{Sequence: afterSequence + 1, Event: terminal}:
			}
			return
		case "SUCCEEDED":
			return
		}
		timer := time.NewTimer(executionEventPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

type executionStreamRetryState struct {
	startedAt time.Time
	delay     time.Duration
}

func (s *executionStreamRetryState) reset() {
	if s == nil {
		return
	}
	s.startedAt = time.Time{}
	s.delay = 0
}

func (j *ExecutionEventJournal) retryExecutionStreamRead(
	ctx context.Context,
	state *executionStreamRetryState,
	err error,
) bool {
	if ctx.Err() != nil || !isRetryableExecutionStreamError(err) || state == nil {
		return false
	}
	now := time.Now()
	if state.startedAt.IsZero() {
		state.startedAt = now
	}
	remaining := executionStreamRetryWindow - now.Sub(state.startedAt)
	if remaining <= 0 {
		return false
	}
	delay := state.delay
	if delay <= 0 {
		delay = executionStreamRetryInitial
	}
	if delay > executionStreamRetryMax {
		delay = executionStreamRetryMax
	}
	if delay > remaining {
		delay = remaining
	}
	if j != nil && j.store != nil && j.store.pool != nil {
		j.store.pool.Reset()
	}
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return false
	case <-timer.C:
	}
	if state.delay == 0 {
		state.delay = executionStreamRetryInitial * 2
	} else {
		state.delay *= 2
		if state.delay > executionStreamRetryMax {
			state.delay = executionStreamRetryMax
		}
	}
	return true
}

func isRetryableExecutionStreamError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if pgconn.SafeToRetry(err) {
		return true
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "08") ||
			pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	return errors.Is(err, io.EOF)
}

func sendExecutionStreamError(
	ctx context.Context,
	output chan<- gateway.ExecutionEvent,
	requestID string,
	afterSequence int64,
	_ error,
) {
	if ctx.Err() != nil {
		return
	}
	terminal := event.NewErrorEvent(
		requestID,
		"platform",
		"execution_stream_unavailable",
		"execution result is incomplete because durable events are temporarily unavailable",
	)
	select {
	case <-ctx.Done():
	case output <- gateway.ExecutionEvent{Sequence: afterSequence + 1, Event: terminal}:
	}
}

func (j *ExecutionEventJournal) executionExists(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
) (bool, error) {
	var exists bool
	if err := j.store.pool.QueryRow(
		ctx,
		`SELECT EXISTS(
    SELECT 1
    FROM platform.execution
    WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
)`,
		scope.TenantID,
		scope.AppID,
		requestID,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("check execution: %w", err)
	}
	return exists, nil
}

func (j *ExecutionEventJournal) executionStatus(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
) (string, error) {
	var status string
	if err := j.store.pool.QueryRow(
		ctx,
		`SELECT status
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		scope.TenantID,
		scope.AppID,
		requestID,
	).Scan(&status); err != nil {
		return "", fmt.Errorf("read execution status: %w", resolveError("execution", err))
	}
	return status, nil
}

func (j *ExecutionEventJournal) executionEventsAfter(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
	afterSequence int64,
) ([]gateway.ExecutionEvent, error) {
	rows, err := j.store.pool.Query(
		ctx,
		`SELECT event_seq, payload
FROM platform.execution_event
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND event_seq > $4
ORDER BY event_seq`,
		scope.TenantID,
		scope.AppID,
		requestID,
		afterSequence,
	)
	if err != nil {
		return nil, fmt.Errorf("query execution events: %w", err)
	}
	defer rows.Close()
	var events []gateway.ExecutionEvent
	for rows.Next() {
		var item gateway.ExecutionEvent
		var payload []byte
		if err := rows.Scan(&item.Sequence, &payload); err != nil {
			return nil, fmt.Errorf("scan execution event: %w", err)
		}
		item.Event = &event.Event{}
		if err := json.Unmarshal(payload, item.Event); err != nil {
			return nil, fmt.Errorf("unmarshal execution event: %w", err)
		}
		if err := item.Validate(); err != nil {
			return nil, fmt.Errorf("stored execution event: %w", err)
		}
		events = append(events, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution events: %w", err)
	}
	return events, nil
}

func executionEventType(evt *event.Event) string {
	if evt.Response != nil && evt.Object != "" {
		return evt.Object
	}
	return "runner_event"
}

var _ gateway.ExecutionEventSource = (*ExecutionEventJournal)(nil)
var _ worker.EventSink = (*ExecutionEventJournal)(nil)
