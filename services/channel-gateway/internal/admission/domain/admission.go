// Package domain owns immutable Gateway acceptance facts, not Provider DTOs.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
)

var (
	ErrInvalidInput       = errors.New("invalid inbound event")
	ErrConflict           = errors.New("event identity conflicts with previous content")
	ErrAccountUnavailable = errors.New("account use eligibility unavailable")
	ErrUnavailable        = errors.New("admission temporarily unavailable")
	ErrRouteChanged       = errors.New("route changed before acceptance")
	ErrClaimLost          = errors.New("outbox claim expired or replaced")
	ErrUsageDenied        = errors.New("tenant usage policy denied admission")
	ErrRateLimited        = errors.New("tenant usage rate reached")
)

type EventKey struct {
	Provider  string `json:"provider"`
	AccountID string `json:"account_id"`
	EventID   string `json:"event_id"`
}

// ConnectionFence is a process-local authorization fence, not source content or wire data.
// Revision identifies the connection configuration, independently of route generation.
type ConnectionFence struct {
	InstanceID string
	Epoch      int64
	Revision   int64
}

func (f ConnectionFence) Validate() error {
	if !identifier.MatchString(f.InstanceID) || f.Epoch < 1 || f.Revision < 1 {
		return ErrInvalidInput
	}
	return nil
}

// ReplyOrigin preserves the first accepted callback's local socket identity.
// It is stored only beside the Admission, never in execution wire/source digest.
// A nil origin means legacy/unknown, not permission to infer a current socket.
type ReplyOrigin struct {
	InstanceID       string `json:"instance_id"`
	Epoch            int64  `json:"epoch"`
	Revision         int64  `json:"revision"`
	SocketGeneration uint64 `json:"socket_generation"`
}

func (o ReplyOrigin) Validate() error {
	if (ConnectionFence{InstanceID: o.InstanceID, Epoch: o.Epoch, Revision: o.Revision}).Validate() != nil || o.SocketGeneration == 0 {
		return ErrInvalidInput
	}
	return nil
}

type Inbound struct {
	// TelegramFence is local polling authority, not source content or a reply socket.
	TelegramFence   *TelegramFence   `json:"-"`
	ReplyOrigin     *ReplyOrigin     `json:"-"`
	ConnectionFence *ConnectionFence `json:"-"`
	Key             EventKey         `json:"key"`
	Kind            string           `json:"kind"`
	ConversationID  string           `json:"conversation_id"`
	ThreadID        string           `json:"thread_id,omitempty"`
	SenderID        string           `json:"sender_id,omitempty"`
	Text            string           `json:"text,omitempty"`
	ReplyContext    json.RawMessage  `json:"reply_context,omitempty"`
	SourceDigest    string           `json:"source_digest"`
	ReceivedAt      time.Time        `json:"received_at"`
}

type TelegramFence struct {
	ScopeID, InstanceID, InstanceEpoch string
	Epoch, Revision                    int64
}

func (f TelegramFence) Validate() error {
	if !identifier.MatchString(f.ScopeID) || !identifier.MatchString(f.InstanceID) || f.InstanceEpoch == "" || f.Epoch < 1 || f.Revision < 1 || f.Epoch > 9007199254740991 || f.Revision > 9007199254740991 {
		return ErrInvalidInput
	}
	return nil
}

type Receipt struct {
	Decision    string `json:"decision"`
	Reason      string `json:"reason,omitempty"`
	AdmissionID string `json:"admission_id,omitempty"`
	RunID       string `json:"run_id,omitempty"`
}
type RouteSnapshot struct {
	Provider             string `json:"provider"`
	AccountID            string `json:"account_id"`
	TenantID             string `json:"tenant_id"`
	BindingID            string `json:"binding_id"`
	Generation           int64  `json:"generation"`
	DeploymentRevisionID string `json:"deployment_revision_id"`
	ManifestRef          string `json:"manifest_ref"`
	ManifestDigest       string `json:"manifest_digest"`
	RolloutID            string `json:"-"`
	RolloutVariant       string `json:"-"`
}
type Acceptance struct {
	Input   Inbound
	Receipt Receipt
	Route   *RouteSnapshot
	Policy  *governancev1.Policy
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var manifestReference = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`)
var manifestDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var sourceDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

func opaque(value string, max int, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > max || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func (i Inbound) Validate() error {
	if i.Key.Provider == "wecom" {
		if i.TelegramFence != nil {
			return ErrInvalidInput
		}
		if i.ConnectionFence == nil || i.ConnectionFence.Validate() != nil {
			return ErrInvalidInput
		}
		if i.ReplyOrigin != nil && (i.ReplyOrigin.Validate() != nil || i.ReplyOrigin.InstanceID != i.ConnectionFence.InstanceID || i.ReplyOrigin.Epoch != i.ConnectionFence.Epoch || i.ReplyOrigin.Revision != i.ConnectionFence.Revision) {
			return ErrInvalidInput
		}
	} else if i.ConnectionFence != nil || i.ReplyOrigin != nil {
		return ErrInvalidInput
	}
	if i.TelegramFence != nil && (i.Key.Provider != "telegram" || i.TelegramFence.Validate() != nil) {
		return ErrInvalidInput
	}
	if (i.Key.Provider != "telegram" && i.Key.Provider != "wecom") || !identifier.MatchString(i.Key.AccountID) || !opaque(i.Key.EventID, 256, true) || !sourceDigest.MatchString(i.SourceDigest) || i.ReceivedAt.IsZero() {
		return ErrInvalidInput
	}
	if !opaque(i.ConversationID, 256, false) || !opaque(i.ThreadID, 256, false) || !opaque(i.SenderID, 256, false) || len(i.Text) > 65536 || len(i.ReplyContext) > 65536 || (len(i.ReplyContext) > 0 && !json.Valid(i.ReplyContext)) {
		return ErrInvalidInput
	}
	switch i.Kind {
	case "text":
		if strings.TrimSpace(i.Text) == "" || i.ConversationID == "" || i.SenderID == "" {
			return ErrInvalidInput
		}
	case "ignore", "interaction":
		if i.Text != "" {
			return ErrInvalidInput
		}
	default:
		return ErrInvalidInput
	}
	return nil
}
func (r RouteSnapshot) ValidateFor(k EventKey) error {
	rolloutValid := r.RolloutID == "" && r.RolloutVariant == "" || identifier.MatchString(r.RolloutID) && (r.RolloutVariant == "stable" || r.RolloutVariant == "canary")
	// Report each predicate separately. A single opaque ErrInvalidInput leaves an
	// operator unable to tell a routing mismatch from a malformed snapshot, and
	// both surface as admission temporarily unavailable.
	switch {
	case r.Provider != k.Provider:
		return fmt.Errorf("%w: provider %q does not match event %q", ErrInvalidInput, r.Provider, k.Provider)
	case r.AccountID != k.AccountID:
		return fmt.Errorf("%w: account %q does not match event %q", ErrInvalidInput, r.AccountID, k.AccountID)
	case r.Generation < 1 || r.Generation > 9007199254740991:
		return fmt.Errorf("%w: generation %d out of range", ErrInvalidInput, r.Generation)
	case !identifier.MatchString(r.TenantID):
		return fmt.Errorf("%w: tenant_id %q is malformed", ErrInvalidInput, r.TenantID)
	case !identifier.MatchString(r.BindingID):
		return fmt.Errorf("%w: binding_id %q is malformed", ErrInvalidInput, r.BindingID)
	case !identifier.MatchString(r.DeploymentRevisionID):
		return fmt.Errorf("%w: deployment_revision_id %q is malformed", ErrInvalidInput, r.DeploymentRevisionID)
	case len(r.ManifestRef) > 2048 || !manifestReference.MatchString(r.ManifestRef):
		return fmt.Errorf("%w: manifest_ref %q is malformed", ErrInvalidInput, r.ManifestRef)
	case !manifestDigest.MatchString(r.ManifestDigest):
		return fmt.Errorf("%w: manifest_digest %q is malformed", ErrInvalidInput, r.ManifestDigest)
	case !rolloutValid:
		return fmt.Errorf("%w: rollout %q/%q is malformed", ErrInvalidInput, r.RolloutID, r.RolloutVariant)
	}
	return nil
}
func (r Receipt) Validate() error {
	switch r.Decision {
	case "admit-run":
		if !identifier.MatchString(r.AdmissionID) || !identifier.MatchString(r.RunID) || r.Reason != "" {
			return ErrInvalidInput
		}
	case "ignore", "interaction":
		if r.AdmissionID != "" || r.RunID != "" || !opaque(r.Reason, 256, false) {
			return ErrInvalidInput
		}
	default:
		return ErrInvalidInput
	}
	return nil
}
func (c Acceptance) Validate() error {
	if err := c.Receipt.Validate(); err != nil {
		return err
	}
	if err := c.Input.Validate(); err != nil {
		return err
	}
	switch c.Receipt.Decision {
	case "admit-run":
		if c.Input.Kind != "text" || c.Route == nil || c.Policy != nil && (c.Policy.TenantID != c.Route.TenantID || c.Policy.Validate() != nil) || !identifier.MatchString(c.Receipt.AdmissionID) || !identifier.MatchString(c.Receipt.RunID) || c.Receipt.Reason != "" {
			return ErrInvalidInput
		}
		return c.Route.ValidateFor(c.Input.Key)
	case "ignore", "interaction":
		if c.Receipt.Decision != c.Input.Kind || c.Route != nil || c.Policy != nil || c.Receipt.AdmissionID != "" || c.Receipt.RunID != "" || !opaque(c.Receipt.Reason, 256, false) {
			return ErrInvalidInput
		}
	default:
		return ErrInvalidInput
	}
	return nil
}
