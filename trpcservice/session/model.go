package session

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type SessionState string

const (
	StateActive    SessionState = "active"
	StatePaused    SessionState = "paused"
	StateCompleted SessionState = "completed"
	StateArchived  SessionState = "archived"
)

var (
	ErrInvalidArgument = errors.New("invalid session argument")
	ErrInvalidState    = errors.New("invalid session state")
	ErrInvalidEvent    = errors.New("invalid session event")
	ErrVersionConflict = errors.New("session version conflict")
)

type Session struct {
	TenantID       string       `json:"tenant_id"`
	ID             string       `json:"session_id"`
	AgentAppID     string       `json:"agent_app_id"`
	AgentVersion   int64        `json:"agent_version"`
	Channel        string       `json:"channel"`
	BindingID      string       `json:"binding_id"`
	ExternalChat   string       `json:"external_chat"`
	ExternalUser   string       `json:"external_user"`
	State          SessionState `json:"state"`
	StateVersion   int64        `json:"state_version"`
	SummaryVersion int64        `json:"summary_version"`
	LastEventSeq   int64        `json:"last_event_seq"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

func (s Session) Validate() error {
	for name, value := range map[string]string{
		"tenant_id": s.TenantID, "session_id": s.ID, "agent_app_id": s.AgentAppID,
		"channel": s.Channel, "binding_id": s.BindingID,
	} {
		if err := validateID(value); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrInvalidArgument, name, err)
		}
	}
	if s.AgentVersion < 1 || s.StateVersion < 1 || s.SummaryVersion < 0 || s.LastEventSeq < 0 {
		return fmt.Errorf("%w: versions must be non-negative and agent/state versions positive", ErrInvalidArgument)
	}
	if !validState(s.State) {
		return fmt.Errorf("%w: %q", ErrInvalidState, s.State)
	}
	if !s.CreatedAt.IsZero() && !s.UpdatedAt.IsZero() && s.UpdatedAt.Before(s.CreatedAt) {
		return fmt.Errorf("%w: updated_at precedes created_at", ErrInvalidArgument)
	}
	return nil
}

func (s Session) Transition(next SessionState) (Session, error) {
	if err := s.Validate(); err != nil {
		return Session{}, err
	}
	if !validTransition(s.State, next) {
		return Session{}, fmt.Errorf("%w: %s -> %s", ErrInvalidState, s.State, next)
	}
	s.State = next
	s.StateVersion++
	s.UpdatedAt = time.Now().UTC()
	return s, nil
}

func validState(state SessionState) bool {
	switch state {
	case StateActive, StatePaused, StateCompleted, StateArchived:
		return true
	default:
		return false
	}
}

func validTransition(from, to SessionState) bool {
	switch from {
	case StateActive:
		return to == StatePaused || to == StateCompleted
	case StatePaused:
		return to == StateActive || to == StateCompleted
	case StateCompleted:
		return to == StateArchived
	default:
		return false
	}
}

func validateID(value string) error {
	if value == "" || len(value) > 256 {
		return errors.New("must be non-empty and at most 256 bytes")
	}
	if strings.TrimSpace(value) != value || strings.ContainsAny(value, "|\\/\x00") {
		return errors.New("contains invalid whitespace or separator")
	}
	return nil
}
