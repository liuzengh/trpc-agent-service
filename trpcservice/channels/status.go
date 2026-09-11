package channels

import (
	"context"
	"time"
)

const (
	ChannelStateReady      = "ready"
	ChannelStateConnecting = "connecting"
	ChannelStateConnected  = "connected"
	ChannelStateError      = "error"
	ChannelStateOffline    = "offline"
)

// BindingStatus is the non-secret runtime projection for one configured
// external bot account. Configuration versions own what should exist; this
// projection reports whether the transport is currently usable.
type BindingStatus struct {
	Channel       Channel   `json:"channel"`
	BindingID     string    `json:"binding_id"`
	State         string    `json:"state"`
	Owner         string    `json:"owner,omitempty"`
	LastChangedAt time.Time `json:"last_changed_at,omitempty"`
	LastError     string    `json:"last_error,omitempty"`
}

// BindingStatusLister exposes channel health to the management plane without
// exposing credentials or connector implementation details.
type BindingStatusLister interface {
	ListBindingStatuses(context.Context, string, string) ([]BindingStatus, error)
}
