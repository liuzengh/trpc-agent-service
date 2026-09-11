// Package wecom implements the WeCom intelligent-bot long-connection protocol.
// It owns no platform routing, tenancy, durable delivery, or connection lease.
package wecom

import (
	"context"
	"errors"
	"net/http"
	"time"
)

const DefaultURL = "wss://openws.work.weixin.qq.com"

// Config contains only protocol configuration. Zero limits receive finite defaults.
// MaxReconnects is the number of retries after the first connection; zero disables
// reconnect. A Client never retries an individual Final command.
type Config struct {
	BotID             string
	Secret            string
	URL               string
	DialTimeout       time.Duration
	AckTimeout        time.Duration
	WriteTimeout      time.Duration
	HeartbeatInterval time.Duration
	CloseTimeout      time.Duration
	ReconnectBackoff  time.Duration
	MaxReconnects     int
	EventBuffer       int
	StateBuffer       int
	MaxPending        int
	MaxRequestIDs     int
	ReadLimit         int64
}

type Option func(*options) error
type options struct{ httpClient *http.Client }

// WithHTTPClient supplies a caller-owned transport, for example a proxy or test
// transport. It is never invoked by NewClient.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) error {
		if client == nil {
			return ErrInvalidConfig
		}
		o.httpClient = client
		return nil
	}
}

type State string

const (
	StateIdle           State = "IDLE"
	StateConnecting     State = "CONNECTING"
	StateAuthenticating State = "AUTHENTICATING"
	StateReady          State = "READY"
	StateDisconnected   State = "DISCONNECTED"
	StateBackoff        State = "BACKOFF"
	StateReplaced       State = "REPLACED"
	StateStopped        State = "STOPPED"
	StateFailed         State = "FAILED"
	StateClosed         State = "CLOSED"
)

// StateSnapshot is ordered by Sequence, not by receive time. States is a bounded
// latest-wins notification stream; missed intermediate sequences are expected.
// State() is authoritative. Generation identifies one connection attempt.
type StateSnapshot struct {
	State      State
	Generation uint64
	Sequence   uint64
	Reason     ErrorCode
}

type EventKind string

const (
	EventText        EventKind = "text"
	EventNotice      EventKind = "event"
	EventUnsupported EventKind = "unsupported"
)

// Event is a curated protocol DTO, not an SDK JSON passthrough. RequestID and
// Generation must both be preserved for replies; Generation is not a lease epoch.
type Event struct {
	// BodyDigest fingerprints the complete decoded callback body, including unknown
	// fields, but not transport headers or local generation. It is not raw payload.
	BodyDigest string
	Kind       EventKind
	RequestID  string
	Generation uint64
	MessageID  string
	BotID      string
	SenderID   string
	ChatID     string
	ChatType   string
	Text       string
	EventType  string
}
type Handler func(context.Context, Event) error

// ReplyRequest supports a text Final only. P0 conservatively permits at most one
// attempted Final per request ID per generation, even after a provider rejection.
type ReplyRequest struct {
	RequestID  string
	Generation uint64
	StreamID   string
	Content    string
}

// CommandAck is a provider command acknowledgement, not user-read or durable
// platform delivery confirmation. ErrCode is numeric; raw provider text is omitted.
type CommandAck struct {
	RequestID  string
	Generation uint64
	ErrCode    int64
}
type Certainty string

const (
	NotSent  Certainty = "NOT_SENT"
	Accepted Certainty = "ACCEPTED"
	Rejected Certainty = "REJECTED"
	Unknown  Certainty = "UNKNOWN"
)

type ErrorCode string

const (
	CodeInvalid         ErrorCode = "invalid_request"
	CodeNotReady        ErrorCode = "not_ready"
	CodeClosed          ErrorCode = "closed"
	CodeCanceled        ErrorCode = "canceled"
	CodeStaleGeneration ErrorCode = "stale_generation"
	CodeInFlight        ErrorCode = "request_in_flight"
	CodeFinalAttempted  ErrorCode = "final_already_attempted"
	CodePoisoned        ErrorCode = "request_poisoned"
	CodeCapacity        ErrorCode = "capacity_exceeded"
	CodeWrite           ErrorCode = "write_failed"
	CodeAckTimeout      ErrorCode = "ack_timeout"
	CodeRejected        ErrorCode = "provider_rejected"
	CodeDisconnected    ErrorCode = "disconnected"
	CodeProtocol        ErrorCode = "invalid_protocol"
	CodeHandler         ErrorCode = "handler_failed"
	CodeReplaced        ErrorCode = "connection_replaced"
	CodeDrainTimeout    ErrorCode = "drain_timeout"
)

// CommandError exposes certainty separately from retry policy. All messages are
// fixed and exclude configuration, provider bodies, URLs, and underlying errors.
type CommandError struct {
	Certainty Certainty
	Code      ErrorCode
}

func (e *CommandError) Error() string { return "wecom: " + string(e.Certainty) + ": " + string(e.Code) }
func commandError(certainty Certainty, code ErrorCode) error {
	return &CommandError{Certainty: certainty, Code: code}
}

var (
	ErrInvalidConfig   = errors.New("wecom: invalid configuration")
	ErrAlreadyRun      = errors.New("wecom: Run already started")
	ErrClosed          = errors.New("wecom: closed")
	ErrCanceled        = errors.New("wecom: canceled")
	ErrDial            = errors.New("wecom: connection failed")
	ErrAuth            = errors.New("wecom: authentication failed")
	ErrDisconnected    = errors.New("wecom: disconnected")
	ErrReplaced        = errors.New("wecom: connection replaced")
	ErrProtocol        = errors.New("wecom: invalid protocol")
	ErrEventOverflow   = errors.New("wecom: event queue full")
	ErrRequestCapacity = errors.New("wecom: request identity capacity exceeded")
	ErrHandler         = errors.New("wecom: handler failed")
	ErrHeartbeat       = errors.New("wecom: heartbeat failed")
	ErrDrainTimeout    = errors.New("wecom: drain timeout")
)
