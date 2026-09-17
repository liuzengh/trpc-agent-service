package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type SharedEvent struct {
	TenantID, RequestID string
	Sequence            uint64
	Type                string
	Data                json.RawMessage
	Terminal            bool
	CreatedAt           time.Time
}

type EventPage struct {
	Events       []SharedEvent
	LastSequence uint64
	Terminal     bool
}

type SharedEventStore interface {
	Replay(context.Context, ExecutionKey, uint64, int) (EventPage, error)
}

const (
	defaultReplayLimit = 64
	maximumReplayLimit = 256
)

func normalizeReplayLimit(limit int) int {
	if limit <= 0 {
		return defaultReplayLimit
	}
	if limit > maximumReplayLimit {
		return maximumReplayLimit
	}
	return limit
}

// TerminalEventStore derives replayable terminal SSE events from the shared
// TaskStore and immutable ResultStore. Both PostgreSQL implementations satisfy
// these ports, so Gateway restart does not lose terminal replay state.
// Ephemeral progress fan-out can be layered on top later and remains non-authoritative.
type TerminalEventStore struct {
	Tasks   ExecutionReader
	Results messaging.ResultStore
}

func (s TerminalEventStore) Replay(ctx context.Context, key ExecutionKey, after uint64, limit int) (EventPage, error) {
	if s.Tasks == nil || s.Results == nil || key.TenantID == "" || key.RequestID == "" || limit < 1 {
		return EventPage{}, runtime.ErrInvariantViolation
	}
	status, err := s.Tasks.GetExecution(ctx, key)
	if err != nil {
		return EventPage{}, err
	}
	if status.Envelope.TenantID != key.TenantID || status.Envelope.RequestID != key.RequestID {
		return EventPage{}, runtime.ErrTenantScope
	}
	if !status.Outcome.Terminal() {
		if after != 0 {
			return EventPage{}, runtime.ErrVersionConflict
		}
		return EventPage{}, nil
	}
	events, err := s.terminalEvents(ctx, status)
	if err != nil {
		return EventPage{}, err
	}
	last := uint64(len(events))
	if after > last {
		return EventPage{}, runtime.ErrVersionConflict
	}
	page := EventPage{LastSequence: last}
	for _, event := range events {
		if event.Sequence > after && len(page.Events) < limit {
			page.Events = append(page.Events, event)
		}
	}
	page.Terminal = after+uint64(len(page.Events)) >= last
	return page, nil
}

func (s TerminalEventStore) terminalEvents(ctx context.Context, status ExecutionStatus) ([]SharedEvent, error) {
	key := ExecutionKey{TenantID: status.Envelope.TenantID, RequestID: status.Envelope.RequestID}
	createdAt := status.Envelope.CreatedAt
	if status.Outcome == runtime.OutcomeSucceeded {
		result, err := s.Results.GetResult(ctx, key.TenantID, key.RequestID)
		if err != nil {
			if errors.Is(err, runtime.ErrNotFound) {
				return nil, runtime.ErrBackendUnavailable
			}
			return nil, err
		}
		if result.ResultRef == "" || result.ResultRef != status.ResultRef || len(result.Content) == 0 {
			return nil, runtime.ErrVersionMismatch
		}
		message, err := json.Marshal(struct {
			ResultRef     string `json:"result_ref"`
			ContentDigest string `json:"content_digest"`
			Content       string `json:"content"`
		}{ResultRef: result.ResultRef, ContentDigest: result.ContentDigest, Content: string(result.Content)})
		if err != nil {
			return nil, err
		}
		if !result.CreatedAt.IsZero() {
			createdAt = result.CreatedAt
		}
		completed := terminalData(status)
		return []SharedEvent{
			{TenantID: key.TenantID, RequestID: key.RequestID, Sequence: 1, Type: "message.completed", Data: message, CreatedAt: createdAt},
			{TenantID: key.TenantID, RequestID: key.RequestID, Sequence: 2, Type: "run.completed", Data: completed, Terminal: true, CreatedAt: createdAt},
		}, nil
	}
	eventType := "run.completed"
	switch status.Outcome {
	case runtime.OutcomeDenied, runtime.OutcomeConfirmationDenied:
		eventType = "run.denied"
	case runtime.OutcomeFailed, runtime.OutcomeConfirmationTimeout:
		eventType = "run.failed"
	}
	return []SharedEvent{{TenantID: key.TenantID, RequestID: key.RequestID, Sequence: 1, Type: eventType,
		Data: terminalData(status), Terminal: true, CreatedAt: createdAt}}, nil
}

func terminalData(status ExecutionStatus) json.RawMessage {
	value, _ := json.Marshal(struct {
		RequestID string          `json:"request_id"`
		Outcome   runtime.Outcome `json:"outcome"`
		ResultRef string          `json:"result_ref,omitempty"`
	}{RequestID: status.Envelope.RequestID, Outcome: status.Outcome, ResultRef: status.ResultRef})
	return value
}
