package channels

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
)

// RecallRequest is a verified provider recall event. Provider adapters supply
// normalized identifiers and a payload digest; trusted scope comes from the
// resolved Binding and is never read from provider payload tenant fields.
type RecallRequest struct {
	TenantID          string
	AppID             string
	BindingID         string
	Channel           Channel
	ExternalEventID   string
	ExternalMessageID string
	PayloadHash       []byte
}

// Validate checks the scope, identifiers, and digest required for durable
// recall admission.
func (r RecallRequest) Validate() error {
	if r.TenantID == "" || r.AppID == "" || r.BindingID == "" {
		return errors.New("recall scope is required")
	}
	if err := r.Channel.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"external event id":   r.ExternalEventID,
		"external message id": r.ExternalMessageID,
	} {
		normalized, err := NormalizeExternalID(value)
		if err != nil {
			return fmt.Errorf("recall %s: %w", name, err)
		}
		if normalized != value {
			return fmt.Errorf("recall %s is not normalized", name)
		}
	}
	if len(r.PayloadHash) != sha256.Size {
		return errors.New("recall payload hash must be a sha256 digest")
	}
	return nil
}

// RecallResult describes the durable result of one recall event. A running
// execution is canceled by the worker after this method commits; PostgreSQL
// remains the authoritative state machine.
type RecallResult struct {
	RequestID       string
	ExecutionStatus string
	Replayed        bool
}

// RecallAdmitter durably applies a verified provider recall event.
type RecallAdmitter interface {
	AdmitRecall(context.Context, RecallRequest) (RecallResult, error)
}
