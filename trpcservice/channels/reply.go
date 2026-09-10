package channels

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// ReplyKind selects the provider operation used for one durable reply.
// Text remains the zero value so existing text projections keep their wire
// and identity semantics.
type ReplyKind string

const (
	ReplyKindText   ReplyKind = "text"
	ReplyKindStream ReplyKind = "stream"
	ReplyKindCard   ReplyKind = "card"
)

// StreamPhase is the lifecycle state of a provider message stream. Content
// is always the full snapshot after projection; providers never need to
// reconstruct an answer from an in-memory delta buffer.
type StreamPhase string

const (
	StreamPhaseStart  StreamPhase = "start"
	StreamPhaseUpdate StreamPhase = "update"
	StreamPhaseEnd    StreamPhase = "end"
	StreamPhaseAbort  StreamPhase = "abort"
)

// ErrReplyCardActionsUnsupported is returned until channel ingress has an
// authenticated callback and a durable action whitelist. Emitting an action
// without that matching boundary would create an untrusted command path, so
// action-bearing cards fail closed at the durable reply boundary.
var ErrReplyCardActionsUnsupported = errors.New("reply card actions are unsupported without an authenticated callback")

// CardAction is a provider-neutral action rendered as a button when the
// provider supports interactive cards. Value is opaque to the provider and
// is returned in the provider callback payload.
type CardAction struct {
	ID    string
	Label string
	Value string
}

// ReplyCard is the small common card model shared by the supported IM
// providers. Provider-specific JSON is generated only at the adapter edge.
type ReplyCard struct {
	Title   string
	Body    string
	Status  string
	Actions []CardAction
}

func (c ReplyCard) Validate() error {
	if strings.TrimSpace(c.Title) == "" {
		return errors.New("reply card title is required")
	}
	if c.Body == "" {
		return errors.New("reply card body is required")
	}
	if len(c.Actions) > 0 {
		return ErrReplyCardActionsUnsupported
	}
	return nil
}

// ReplyTarget identifies the internal platform record from which a provider
// target is resolved. It never contains a raw provider identifier.
type ReplyTarget struct {
	Kind             TargetKind
	InternalEntityID string
}

// Validate checks the target reference shape.
func (t ReplyTarget) Validate() error {
	if t.Kind != TargetKindUser && t.Kind != TargetKindConversation && t.Kind != TargetKindTopic && t.Kind != TargetKindMessage {
		return errors.New("reply target kind is invalid")
	}
	if t.InternalEntityID == "" {
		return errors.New("reply target internal entity id is required")
	}
	return nil
}

// Reply is the durable provider-neutral payload passed from Reply Projection
// to one provider outbound call.
type Reply struct {
	TenantID        string
	AppID           string
	RequestID       string
	SourceEventID   string
	Channel         Channel
	BindingID       string
	BindingRevision int64
	ReplyID         string
	Revision        int64
	Target          ReplyTarget
	Text            string
	Kind            ReplyKind
	StreamID        string
	StreamPhase     StreamPhase
	StreamSequence  int64
	Card            *ReplyCard
}

func (r Reply) normalizedKind() ReplyKind {
	if r.Kind == "" {
		return ReplyKindText
	}
	return r.Kind
}

// ReplyKind returns the effective kind, treating the zero value as text.
func (r Reply) ReplyKind() ReplyKind {
	return r.normalizedKind()
}

// StableID returns the deterministic identity shared by projection and the
// Reply Outbox, so event replay cannot create a second send.
func (r Reply) StableID() string {
	identityParts := []string{
		r.TenantID, r.AppID, r.BindingID, r.RequestID,
		r.SourceEventID, strconv.FormatInt(r.Revision, 10),
	}
	if r.normalizedKind() != ReplyKindText {
		identityParts = append(identityParts, string(r.normalizedKind()), r.StreamID, string(r.StreamPhase), strconv.FormatInt(r.StreamSequence, 10))
	}
	identity := strings.Join(identityParts, "\x1f")
	return uuid.NewSHA1(uuid.Nil, []byte(identity)).String()
}

// Validate checks the platform-level fields required by a text reply.
func (r Reply) Validate() error {
	if r.TenantID == "" || r.AppID == "" {
		return errors.New("reply scope is required")
	}
	if r.RequestID == "" || r.SourceEventID == "" || r.ReplyID == "" {
		return errors.New("reply identity is required")
	}
	if err := r.Channel.Validate(); err != nil {
		return err
	}
	if r.BindingID == "" {
		return errors.New("reply binding_id is required")
	}
	if r.Revision <= 0 || r.BindingRevision <= 0 {
		return errors.New("reply revision must be positive")
	}
	switch r.normalizedKind() {
	case ReplyKindText:
		if r.Text == "" {
			return errors.New("reply text is required")
		}
		if r.Card != nil || r.StreamID != "" || r.StreamPhase != "" || r.StreamSequence != 0 {
			return errors.New("text reply contains rich fields")
		}
	case ReplyKindStream:
		if err := validateStream(r); err != nil {
			return err
		}
		if r.Card != nil {
			return errors.New("stream reply contains card")
		}
	case ReplyKindCard:
		if r.Card == nil {
			return errors.New("card reply payload is required")
		}
		if err := r.Card.Validate(); err != nil {
			return err
		}
		if r.Text != "" {
			return errors.New("card reply contains text")
		}
		if r.StreamID == "" && r.StreamPhase == "" && r.StreamSequence == 0 {
			break
		}
		if err := validateStream(r); err != nil {
			return fmt.Errorf("card lifecycle: %w", err)
		}
	default:
		return errors.New("reply kind is invalid")
	}
	return r.Target.Validate()
}

func validateStream(r Reply) error {
	if r.StreamID == "" {
		return errors.New("stream id is required")
	}
	if r.StreamSequence <= 0 {
		return errors.New("stream sequence must be positive")
	}
	switch r.StreamPhase {
	case StreamPhaseStart, StreamPhaseUpdate, StreamPhaseEnd, StreamPhaseAbort:
	default:
		return errors.New("stream phase is invalid")
	}
	content := r.Text
	if r.ReplyKind() == ReplyKindCard && r.Card != nil {
		content = r.Card.Body
	}
	if r.StreamPhase != StreamPhaseAbort && content == "" {
		return errors.New("stream content is required")
	}
	return nil
}

// ProviderOutboundClient performs exactly one ordinary text send. The caller
// resolves the provider target immediately before this call.
type ProviderOutboundClient interface {
	SendOnce(context.Context, Reply, string) (ProviderReceipt, error)
}

// ProviderStreamOutboundClient updates one provider message for each durable
// stream frame. previousProviderMessageID is empty for the first frame and
// is the receipt of the last successfully sent frame after recovery.
type ProviderStreamOutboundClient interface {
	SendStream(context.Context, Reply, string, string) (ProviderReceipt, error)
}

// ProviderCardOutboundClient sends or updates a provider-native card. The
// previous receipt allows a provider to patch an existing card when it has
// that capability.
type ProviderCardOutboundClient interface {
	SendCard(context.Context, Reply, string, string) (ProviderReceipt, error)
}

// ProviderReceipt contains the scoped delivery receipt returned by one
// provider operation.
type ProviderReceipt struct {
	ProviderMessageID string
}

// ValidateProviderReceipt checks a receipt returned by a provider operation.
func (r ProviderReceipt) Validate() error {
	if r.ProviderMessageID == "" {
		return fmt.Errorf("provider message id is required")
	}
	return nil
}
