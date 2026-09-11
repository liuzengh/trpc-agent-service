// Package domain defines Execution facts without transport, SQL or SDK types.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrInvalid  = errors.New("invalid execution input")
	ErrConflict = errors.New("execution identity conflict")
	ErrNotFound = errors.New("execution fact not found")
	ErrNotReady = errors.New("execution not ready")
	ErrFenced   = errors.New("execution grant is no longer current")
	ErrCapacity = errors.New("execution capacity reached")
)

type Status string

const (
	Queued    Status = "QUEUED"
	Running   Status = "RUNNING"
	RetryWait Status = "RETRY_WAIT"
	Succeeded Status = "SUCCEEDED"
	Failed    Status = "FAILED"
)

// Policy is explicit deployment configuration copied into each accepted Run.
// It contains recovery/capacity controls, not a cumulative model-token budget.
type Policy struct {
	Version                                 string
	MaxRunAge, MaxReplyAge, MaxFutureSkew   time.Duration
	LeaseTTL, RenewalInterval, RetryBackoff time.Duration
	MaxAttempts                             int
}

func (p Policy) Validate() error {
	if p.Version == "" || p.MaxRunAge <= 0 || p.MaxReplyAge <= 0 || p.MaxFutureSkew < 0 ||
		p.LeaseTTL <= 0 || p.RenewalInterval <= 0 || p.RenewalInterval >= p.LeaseTTL/2 ||
		p.RetryBackoff <= 0 || p.MaxAttempts <= 0 {
		return ErrInvalid
	}
	return nil
}

type Route struct {
	TenantID, Provider, AccountID, BindingID, DeploymentRevisionID string
	ManifestRef, ManifestDigest                                    string
	Generation                                                     int64
}
type Input struct {
	ConversationID, ThreadID, SenderID, Text, SourceDigest string
	ReceivedAt                                             time.Time
}
type Requested struct {
	EventID, EventDigest, RunDigest, RunID, AdmissionID string
	Route                                               Route
	Input                                               Input
	UsagePolicy                                         UsagePolicy
}

type UsagePolicy struct {
	Revision                                                  int64
	Enabled                                                   bool
	MaxConcurrentRuns                                         int
	TokenPeriodSeconds, TokenLimit, TokenReservationPerRun    int64
	InputMicrosPerMillionTokens, OutputMicrosPerMillionTokens int64
}

func (p UsagePolicy) Validate() error {
	if !p.Enabled {
		if p != (UsagePolicy{}) {
			return ErrInvalid
		}
		return nil
	}
	if p.Revision < 1 || p.Revision > 9007199254740991 || p.MaxConcurrentRuns < 1 || p.MaxConcurrentRuns > 100000 || p.TokenPeriodSeconds < 3600 || p.TokenPeriodSeconds > 31536000 || p.TokenLimit < 1 || p.TokenReservationPerRun < 1 || p.TokenReservationPerRun > p.TokenLimit || p.InputMicrosPerMillionTokens < 0 || p.InputMicrosPerMillionTokens > 1_000_000_000 || p.OutputMicrosPerMillionTokens < 0 || p.OutputMicrosPerMillionTokens > 1_000_000_000 {
		return ErrInvalid
	}
	return nil
}

func (r Requested) Validate() error {
	for _, s := range []string{r.EventID, r.RunID, r.AdmissionID, r.Route.TenantID, r.Route.AccountID,
		r.Route.BindingID, r.Route.DeploymentRevisionID, r.Route.ManifestRef, r.Input.ConversationID, r.Input.SenderID} {
		if strings.TrimSpace(s) == "" {
			return ErrInvalid
		}
	}
	if r.Route.Generation < 1 || (r.Route.Provider != "telegram" && r.Route.Provider != "wecom") ||
		!DigestValid(r.EventDigest) || !DigestValid(r.RunDigest) || !DigestValid(r.Route.ManifestDigest) ||
		r.Input.ReceivedAt.IsZero() || strings.TrimSpace(r.Input.Text) == "" || r.UsagePolicy.Validate() != nil {
		return ErrInvalid
	}
	return nil
}

// Scope partitions history by the observed social identity inside a conversation.
// Route refreshes do not reset history; published revision changes remain isolated.
func (r Requested) Scope() []string {
	return []string{r.Route.TenantID, r.Route.Provider, r.Route.AccountID, r.Input.ConversationID,
		r.Input.ThreadID, r.Route.BindingID, r.Route.DeploymentRevisionID, r.SocialIdentityID()}
}

// SocialIdentityID records an observed provider account, not a permission grant or
// a Control login. Provider identities are not automatically linked across Bots.
func (r Requested) SocialIdentityID() string {
	b, _ := json.Marshal([]string{r.Route.TenantID, r.Route.Provider, r.Route.AccountID, r.Input.SenderID})
	return StableID("soc", string(b))
}
func (r Requested) SessionID() string {
	b, _ := json.Marshal(r.Scope()) // []string is always JSON-encodable.
	return StableID("ses", string(b))
}
func StableID(prefix, value string) string {
	h := sha256.Sum256([]byte(value))
	return prefix + "_" + hex.EncodeToString(h[:])
}
func Digest(b []byte) string { h := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(h[:]) }
func DigestValid(s string) bool {
	if len(s) != 71 || !strings.HasPrefix(s, "sha256:") {
		return false
	}
	b, err := hex.DecodeString(s[7:])
	return err == nil && len(b) == 32 && strings.ToLower(s) == s
}

type Head struct{ Ref, Digest string }

func (h Head) Valid() bool {
	return (h.Ref == "" && h.Digest == "") || (h.Ref != "" && DigestValid(h.Digest))
}

type Receipt struct {
	EventID, RunID, TenantID string
	SessionID                string
	Sequence                 int64
	Outcome                  string
}
type Run struct {
	Request                                Requested
	SessionID                              string
	Sequence                               int64
	Status                                 Status
	WaitReason                             string
	Policy                                 Policy
	AcceptedAt, RunDeadline, ReplyDeadline time.Time
	ExecutionDeadline                      *time.Time
	Attempts                               int
	Generation, LeaseEpoch                 int64
	CurrentAttemptID                       string
}
type Grant struct {
	Run                        Run
	AttemptID, WorkerID, Token string
	Generation, LeaseEpoch     int64
	LeaseUntil                 time.Time
	Parent                     Head
}
type ClaimRequest struct {
	TenantID, RunID, WorkerID string
	MaxRunSeconds             int64
	MaxActive                 int
}
type Candidate struct {
	Ref, Digest string
	Parent      Head
}
type Finish struct {
	Attachments       []Attachment
	MemoryDigest      string
	Grant             Grant
	Status            Status
	Candidate         Candidate
	FinalText, Reason string
}

// FinishDigest identifies the complete immutable result, including Final text.
func FinishDigest(f Finish) string {
	value := struct {
		Attempt           string
		Generation, Epoch int64
		Status            Status
		Candidate         Candidate
		Text, Reason      string
		MemoryDigest      string       `json:",omitempty"`
		Attachments       []Attachment `json:",omitempty"`
	}{f.Grant.AttemptID, f.Grant.Generation, f.Grant.LeaseEpoch, f.Status, f.Candidate, f.FinalText, f.Reason, f.MemoryDigest, f.Attachments}
	b, _ := json.Marshal(value)
	return Digest(b)
}

type Completion struct {
	MemoryDigest                                   string `json:",omitempty"`
	MemoryStatus                                   string `json:",omitempty"`
	ResultDigest                                   string
	TenantID, RunID, CompletionID, AttemptID, Kind string
	Status                                         Status
	Candidate                                      Head
	FinalIntentID, ReplyDisposition, Reason        string
	CompletedAt                                    time.Time
}
type Final struct {
	TenantID, IntentID, Digest, AdmissionID, RunID, AttemptID, CompletionID, ManifestDigest string
	ExecutionGeneration, Sequence                                                           int64
	Payload                                                                                 []byte
}
type OutboxItem struct {
	IntentID, Digest string
	Payload          []byte
}
