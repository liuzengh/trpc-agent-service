// Package gateway contains the protocol-neutral execution boundary for the
// tenant-scoped HTTP and Channel Gateway.
package gateway

import (
	"errors"
	"fmt"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

var (
	// ErrInvalid reports malformed Gateway input or an invalid principal.
	ErrInvalid = errors.New("invalid gateway input")
	// ErrUnauthenticated reports that a caller did not present a valid principal.
	ErrUnauthenticated = errors.New("gateway authentication failed")
	// ErrPlanUnavailable is the stable, redacted plan-resolution failure.
	ErrPlanUnavailable = errors.New("execution plan unavailable")
	// ErrNotReady reports that the Gateway dependencies cannot accept traffic.
	ErrNotReady = errors.New("gateway is not ready")
	// ErrClosed reports that a Gateway-owned component has been closed.
	ErrClosed = errors.New("gateway is closed")
	// ErrRateLimited reports that the tenant admission budget is exhausted.
	ErrRateLimited = errors.New("tenant rate limit exceeded")
	// ErrDuplicateMessage reports an already accepted idempotency key.
	ErrDuplicateMessage = errors.New("duplicate message")
)

const (
	// PrincipalAPI identifies a principal created by an API Authenticator.
	PrincipalAPI PrincipalKind = "api"
	// PrincipalChannel identifies a principal created from a verified Channel
	// Binding RoutingTarget.
	PrincipalChannel PrincipalKind = "channel"

	// ContentTypeText identifies a plain text message.
	ContentTypeText = "text"
	// ContentTypeMedia identifies an inbound message carrying media and an
	// optional caption. The gateway preserves the caption/marker as Content.
	ContentTypeMedia = "media"
	// ContentTypeRich identifies a structured/rich inbound message. Adapters
	// must provide a deterministic textual representation in Content.
	ContentTypeRich = "rich"

	// ConversationDirect identifies a direct conversation.
	ConversationDirect = channels.ConversationDirect
	// ConversationGroup identifies a group conversation.
	ConversationGroup = channels.ConversationGroup

	maxPrincipalIDRunes   = 256
	maxMessageRunes       = 64 * 1024
	maxExternalIDRunes    = 1024
	maxInboundAttachments = 10
)

// PrincipalKind distinguishes the two independent authentication paths.
type PrincipalKind string

// Principal is an immutable trusted caller identity. Its fields are private
// so request payloads cannot construct or mutate a trusted route by copying
// JSON into this value.
type Principal struct {
	kind          PrincipalKind
	tenantID      string
	appID         string
	subjectID     string
	apiProof      *apiAuthProof
	routingTarget channels.RoutingTarget
}

// newAPIPrincipal is intentionally private. Only a proof-bearing result from
// an APIAuthenticator can cross this boundary into a trusted Principal.
func newAPIPrincipal(authenticated AuthenticatedAPI) (Principal, error) {
	if err := authenticated.Validate(); err != nil {
		return Principal{}, fmt.Errorf("%w: invalid authentication result", ErrUnauthenticated)
	}
	identity := authenticated.identity
	return Principal{
		kind: PrincipalAPI, tenantID: identity.TenantID, appID: identity.AppID,
		subjectID: identity.SubjectID, apiProof: authenticated.proof,
	}, nil
}

// NewChannelPrincipal seals a RoutingTarget produced by the Issue #26
// candidate verification boundary. No request field is accepted by this
// constructor.
func NewChannelPrincipal(target channels.RoutingTarget) (Principal, error) {
	if err := target.Validate(); err != nil {
		return Principal{}, fmt.Errorf("%w: channel target: %v", ErrInvalid, err)
	}
	return Principal{
		kind:          PrincipalChannel,
		tenantID:      target.TenantID,
		appID:         target.AppID,
		routingTarget: target,
	}, nil
}

// Validate checks the complete trusted principal boundary.
func (p Principal) Validate() error {
	switch p.kind {
	case PrincipalAPI:
		if p.apiProof == nil || p.apiProof.identity.TenantID != p.tenantID || p.apiProof.identity.AppID != p.appID || p.apiProof.identity.SubjectID != p.subjectID {
			return fmt.Errorf("%w: principal proof is missing or inconsistent", ErrInvalid)
		}
		if err := p.apiProof.validate(); err != nil {
			return err
		}
	case PrincipalChannel:
		if err := p.routingTarget.Validate(); err != nil {
			return fmt.Errorf("%w: channel target: %v", ErrInvalid, err)
		}
		if p.tenantID != p.routingTarget.TenantID || p.appID != p.routingTarget.AppID {
			return fmt.Errorf("%w: principal target scope is inconsistent", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown principal kind %q", ErrInvalid, p.kind)
	}
	return nil
}

// Kind returns the authentication path that created the principal.
func (p Principal) Kind() PrincipalKind { return p.kind }

// TenantID returns the fixed Tenant identity.
func (p Principal) TenantID() string { return p.tenantID }

// AppID returns the fixed Agent App identity.
func (p Principal) AppID() string { return p.appID }

// SubjectID returns the authenticated API subject, or an empty string for a
// Channel principal whose external user is carried by InboundMessage.
func (p Principal) SubjectID() string { return p.subjectID }

// RoutingTarget returns the fixed Channel route. The value contains no secret
// and is safe to copy; an API principal returns false.
func (p Principal) RoutingTarget() (channels.RoutingTarget, bool) {
	if p.kind != PrincipalChannel {
		return channels.RoutingTarget{}, false
	}
	return p.routingTarget, true
}

// InboundMessage is the sanitized, protocol-neutral input to Dispatch. It has
// no Tenant/App/Binding fields; those are obtained only from Principal.
type InboundMessage struct {
	Content           string
	ContentType       string
	ExternalMessageID string
	ExternalUserID    string
	ConversationKind  channels.ConversationKind
	ExternalPeerID    string
	ExternalChatID    string
	ExternalThreadID  string
	// Attachments contains verified, tenant-owned media references. It never
	// contains provider URLs, credentials, or direct fetch instructions.
	Attachments []attachment.Reference
}

// Normalize validates the message without consulting untrusted route hints.
func (m InboundMessage) Normalize() (InboundMessage, error) {
	clone := m
	clone.Content = strings.TrimSpace(clone.Content)
	clone.Attachments = append([]attachment.Reference(nil), clone.Attachments...)
	if clone.ContentType == "" {
		clone.ContentType = ContentTypeText
		if len(clone.Attachments) > 0 {
			clone.ContentType = ContentTypeMedia
		}
	}
	switch clone.ContentType {
	case ContentTypeText, ContentTypeMedia, ContentTypeRich:
	default:
		return InboundMessage{}, fmt.Errorf("%w: unsupported content type", ErrInvalid)
	}
	if n := len([]rune(clone.Content)); n > maxMessageRunes {
		return InboundMessage{}, fmt.Errorf("%w: content must contain at most %d characters", ErrInvalid, maxMessageRunes)
	}
	if clone.Content == "" && len(clone.Attachments) == 0 {
		return InboundMessage{}, fmt.Errorf("%w: content or attachment is required", ErrInvalid)
	}
	if len(clone.Attachments) > maxInboundAttachments {
		return InboundMessage{}, fmt.Errorf("%w: too many attachments", ErrInvalid)
	}
	for index, value := range clone.Attachments {
		normalized, err := value.Normalize()
		if err != nil {
			return InboundMessage{}, fmt.Errorf("%w: attachment %d: %v", ErrInvalid, index, err)
		}
		clone.Attachments[index] = normalized
	}
	if clone.ExternalMessageID != "" {
		if err := validateExternalID(clone.ExternalMessageID, "external message ID"); err != nil {
			return InboundMessage{}, err
		}
	}
	if err := validateExternalID(clone.ExternalUserID, "external user ID"); err != nil {
		return InboundMessage{}, err
	}
	switch clone.ConversationKind {
	case channels.ConversationDirect:
		if err := validateExternalID(clone.ExternalPeerID, "external peer ID"); err != nil {
			return InboundMessage{}, err
		}
	case channels.ConversationGroup:
		if err := validateExternalID(clone.ExternalChatID, "external chat ID"); err != nil {
			return InboundMessage{}, err
		}
	default:
		return InboundMessage{}, fmt.Errorf("%w: conversation kind is required", ErrInvalid)
	}
	if clone.ExternalThreadID != "" {
		if err := validateExternalID(clone.ExternalThreadID, "external thread ID"); err != nil {
			return InboundMessage{}, err
		}
	}
	return clone, nil
}

func validateExternalID(value, label string) error {
	if strings.TrimSpace(value) == "" || hasControl(value) || len([]rune(value)) > maxExternalIDRunes {
		return fmt.Errorf("%w: %s is invalid", ErrInvalid, label)
	}
	return nil
}

func validateScopedID(value, prefix, label string) error {
	if len(value) != len(prefix)+26 || !strings.HasPrefix(value, prefix) {
		return fmt.Errorf("%w: %s ID is invalid", ErrInvalid, label)
	}
	for _, character := range strings.TrimPrefix(value, prefix) {
		if !strings.ContainsRune("0123456789ABCDEFGHJKMNPQRSTVWXYZ", character) {
			return fmt.Errorf("%w: %s ID is invalid", ErrInvalid, label)
		}
	}
	return nil
}

func hasControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}
