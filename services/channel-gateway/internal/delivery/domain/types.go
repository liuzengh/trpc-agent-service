// Package domain owns durable delivery facts, separately from Worker execution.
package domain

import (
	"errors"
	"time"
)

var (
	ErrInvalid      = errors.New("invalid delivery input")
	ErrConflict     = errors.New("delivery identity conflicts with previous content")
	ErrUnavailable  = errors.New("delivery temporarily unavailable")
	ErrNotFound     = errors.New("delivery not found")
	ErrClaimLost    = errors.New("delivery claim or attempt lost")
	ErrUnauthorized = errors.New("delivery execution authorization rejected")
	ErrExpired      = errors.New("delivery deadline expired")
	ErrUnsupported  = errors.New("delivery operation not supported")
	ErrCapacity     = errors.New("delivery capacity exceeded")
)

// Intent has no destination. Delivery obtains the immutable original destination
// from Admission and verifies a committed Final with the Execution owner.
type Attachment struct {
	Name      string `json:"name"`
	Version   int64  `json:"version"`
	MIMEType  string `json:"mime_type"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type Intent struct {
	Attachments                                     []Attachment `json:",omitempty"`
	ID, AdmissionID, RunID, AttemptID, CompletionID string
	ExecutionGeneration, Sequence                   int64
	Text                                            string
	Deadline                                        time.Time
}

type ReplyOrigin struct {
	InstanceID       string
	Epoch, Revision  int64
	SocketGeneration uint64
}
type Target struct {
	TenantID, Provider, AccountID, ManifestDigest            string
	ConversationID, ThreadID, SourceMessageID, SourceEventID string
	CallbackRequestID                                        string
	ReceivedAt                                               time.Time
	Origin                                                   *ReplyOrigin
}
type Prepared struct {
	Intent Intent
	Digest string
	Target Target
	Parts  []string
}
type Receipt struct {
	IntentID, RunID string
	PartCount       int
}
type OwnerFence struct {
	InstanceID      string
	Epoch, Revision int64
}
type State string

const (
	Pending  State = "PENDING"
	Claimed  State = "CLAIMED"
	Calling  State = "CALLING"
	Accepted State = "ACCEPTED"
	Rejected State = "REJECTED"
	NotSent  State = "NOT_SENT"
	Unknown  State = "UNKNOWN"
	Expired  State = "EXPIRED"
)

type Certainty string

const (
	CertaintyNotSent  Certainty = "NOT_SENT"
	CertaintyAccepted Certainty = "ACCEPTED"
	CertaintyRejected Certainty = "REJECTED"
	CertaintyUnknown  Certainty = "UNKNOWN"
)

type ErrorClass string

const (
	ErrorNone        ErrorClass = ""
	ErrorTemporary   ErrorClass = "temporary"
	ErrorPermanent   ErrorClass = "permanent"
	ErrorDeadline    ErrorClass = "deadline"
	ErrorStaleOrigin ErrorClass = "stale_origin"
	ErrorRateLimited ErrorClass = "rate_limited"
)

type Result struct {
	Certainty         Certainty
	ErrorClass        ErrorClass
	ProviderMessageID string
}
type Part struct {
	ID, IntentID  string
	Index         int
	Text          string
	State         State
	AttemptNumber int64
}
type Snapshot struct {
	Receipt Receipt
	Parts   []Part
}
type ClaimRequest struct {
	Provider, AccountID, InstanceID string
	Owner                           *OwnerFence
	Limit                           int
	Lease                           time.Duration
}
type Claim struct {
	// UseBinding is a local account/client binding fingerprint, not a wire capability.
	UseBinding        string
	Part              Part
	Intent            Intent
	Target            Target
	Token, InstanceID string
	ExpiresAt         time.Time
	Owner             *OwnerFence
}
type CallingRequest struct {
	Claim                    Claim
	RequestID, RequestDigest string
	Timeout                  time.Duration
}

// Attempt contains the exact authoritative payload loaded by the A2 transaction.
// EvidenceToken is an internal attempt-bound capability, never a wire field.
type Attempt struct {
	UseBinding                                   string
	ID, PartID, IntentID, ClaimToken, InstanceID string
	Number                                       int64
	Owner                                        *OwnerFence
	RequestID, RequestDigest, EvidenceToken      string
	CallingUntil                                 time.Time
	Intent                                       Intent
	Target                                       Target
	Text                                         string
}
type Observation struct {
	ID, AttemptID, EvidenceToken, ProviderRequestID, RequestDigest string
	Result                                                         Result
}
type StoredObservation struct {
	ID, AttemptID, ProviderRequestID, RequestDigest string
	Result                                          Result
	ObservedAt                                      time.Time
}
