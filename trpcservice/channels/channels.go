// Package channels owns the platform contracts and adapters that turn IM
// events (WeCom, Feishu, and others) into tRPC-Agent-Go Runner inputs. The
// pinned framework version exposes no reusable OpenClaw Go Channel package.
//
// This file holds the protocol-independent input contract: what an adapter has
// to have already established before the platform will accept an event, and
// what shape that event has once it has. Everything here is a value type with a
// Validate method and a deep copy, because these values cross a Store boundary
// in both directions and a caller that could reach into stored state by holding
// on to a slice header would make every isolation claim below conditional.
//
// # What is sensitive here
//
// ExternalEventID, InboundMessage, AttachmentRef and DeliveryTarget.Payload all
// carry third-party data: real conversation ids, real message bodies, real
// recipient references. They may be stored in PostgreSQL, which is access
// controlled and auditable. They may not appear in an error string, a log line,
// a Redis key or a Stream payload, none of which are. That rule is why the
// validation errors in this file name the field and never the value, and why
// the Store interfaces separate ordinary tenant-scoped reads — which return
// bodies — from platform scans, which do not.
package channels

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ChannelType names the protocol an adapter speaks. It is part of every
// persisted row and of every DeliveryTarget, because a stored recipient
// reference is only interpretable by the adapter that wrote it.
type ChannelType string

const (
	// ChannelWeCom is WeCom (企业微信).
	ChannelWeCom ChannelType = "wecom"
	// ChannelFeishu is Feishu / Lark.
	ChannelFeishu ChannelType = "feishu"
)

// channelTypePattern keeps a channel type usable as a key segment and as a
// stable label. It is a lowercase slug rather than a closed enum: adapters
// arrive after this batch, and a Store that refused an unknown-but-well-formed
// type would have to be edited in lockstep with every new adapter. What the
// pattern does buy is that no channel type can carry a separator, a space or a
// control character into a composite key.
var channelTypePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9_-]{0,30}[a-z0-9])?$`)

// Validate rejects a channel type that could not be used as a key segment.
func (c ChannelType) Validate() error {
	if !channelTypePattern.MatchString(string(c)) {
		return fmt.Errorf("%w: invalid channel type", tenant.ErrInvalidArgument)
	}
	return nil
}

// Bounds on everything an adapter can put in front of the platform.
//
// Each of these is the last place a value can be refused before it becomes a
// row, an index entry and a retry that repeats forever. They are deliberately
// generous rather than tuned: the point is that every field has *a* bound, not
// that these particular numbers are optimal for any one protocol. An adapter
// that needs less applies its own limit earlier, where it can still answer the
// upstream platform with that protocol's own error.
const (
	// MaxExternalEventIDBytes bounds the third-party event id. It is part of a
	// unique index, so an unbounded one is an unbounded index entry.
	MaxExternalEventIDBytes = 256

	// MaxMessageTextBytes bounds one inbound or outbound message body.
	MaxMessageTextBytes = 32 * 1024

	// MaxAttachments bounds how many attachment references one message carries.
	MaxAttachments = 16

	// MaxAttachmentFieldBytes bounds each individual attachment string.
	MaxAttachmentFieldBytes = 512

	// MaxDeliveryTargetBytes bounds the adapter-defined recipient reference.
	MaxDeliveryTargetBytes = 8 * 1024

	// MaxExternalMessageIDBytes bounds the platform message reference a
	// successful send records.
	MaxExternalMessageIDBytes = 256
)

// AttachmentKind is the coarse media class of an attachment. It is coarse on
// purpose: the platform routes and bounds attachments, it does not interpret
// them, and a taxonomy fine enough to be useful to one protocol would be wrong
// for the next.
type AttachmentKind string

const (
	AttachmentImage AttachmentKind = "image"
	AttachmentAudio AttachmentKind = "audio"
	AttachmentVideo AttachmentKind = "video"
	AttachmentFile  AttachmentKind = "file"
)

// Validate rejects an attachment kind this platform does not model.
func (k AttachmentKind) Validate() error {
	switch k {
	case AttachmentImage, AttachmentAudio, AttachmentVideo, AttachmentFile:
		return nil
	default:
		return fmt.Errorf("%w: unsupported attachment kind", tenant.ErrInvalidArgument)
	}
}

// AttachmentRef points at media the adapter can fetch again later. It is a
// reference and never the bytes: an inbox row that inlined media would make
// every accept as large as the media, and the retention rules for content and
// for routing metadata are not the same.
type AttachmentRef struct {
	Kind AttachmentKind `json:"kind"`
	// ExternalID is the adapter's own handle for the media.
	ExternalID string `json:"external_id"`
	// MediaType is the reported IANA media type, when the protocol gives one.
	MediaType string `json:"media_type,omitempty"`
	// Name is the reported file name, when the protocol gives one.
	Name string `json:"name,omitempty"`
	// SizeBytes is the reported size. Zero means the protocol did not say.
	SizeBytes int64 `json:"size_bytes,omitempty"`
}

// Validate rejects an attachment reference that could not be resolved later.
func (a AttachmentRef) Validate() error {
	if err := a.Kind.Validate(); err != nil {
		return err
	}
	if err := boundedText("attachment external id", a.ExternalID, MaxAttachmentFieldBytes, true); err != nil {
		return err
	}
	if err := boundedText("attachment media type", a.MediaType, MaxAttachmentFieldBytes, false); err != nil {
		return err
	}
	if err := boundedText("attachment name", a.Name, MaxAttachmentFieldBytes, false); err != nil {
		return err
	}
	if a.SizeBytes < 0 {
		return fmt.Errorf("%w: attachment size cannot be negative", tenant.ErrInvalidArgument)
	}
	return nil
}

// MessageReference names another message on the same channel, for a reply or a
// quote. It stores the adapter's real id rather than a hash, because a reply
// has to be addressable again after a restart; see the note on DeliveryTarget.
type MessageReference struct {
	ExternalMessageID string `json:"external_message_id"`
}

// Validate rejects a reference that names no message.
func (r MessageReference) Validate() error {
	return boundedText(
		"reply-to external message id", r.ExternalMessageID, MaxExternalMessageIDBytes, true)
}

// InboundMessage is the canonical, persistable form of what a user sent. The
// first version carries text plus bounded, typed references for media and
// replies; an adapter that supports more adds fields here rather than smuggling
// protocol JSON through DeliveryTarget.
type InboundMessage struct {
	Text        string            `json:"text"`
	Attachments []AttachmentRef   `json:"attachments,omitempty"`
	ReplyTo     *MessageReference `json:"reply_to,omitempty"`
}

// Validate rejects a message the platform could not store or replay.
//
// An empty Text with at least one attachment is legal: "user sent a picture and
// nothing else" is an ordinary turn on every IM protocol. A message with
// neither is not, because there would be nothing for the agent to answer.
func (m InboundMessage) Validate() error {
	if err := boundedText("message text", m.Text, MaxMessageTextBytes, false); err != nil {
		return err
	}
	if len(m.Attachments) > MaxAttachments {
		return fmt.Errorf(
			"%w: message carries more than %d attachments", tenant.ErrInvalidArgument, MaxAttachments)
	}
	for _, attachment := range m.Attachments {
		if err := attachment.Validate(); err != nil {
			return err
		}
	}
	if m.ReplyTo != nil {
		if err := m.ReplyTo.Validate(); err != nil {
			return err
		}
	}
	if strings.TrimSpace(m.Text) == "" && len(m.Attachments) == 0 {
		return fmt.Errorf("%w: message carries no text and no attachment", tenant.ErrInvalidArgument)
	}
	return nil
}

// Clone returns a copy that shares nothing with m.
func (m InboundMessage) Clone() InboundMessage {
	copied := m
	copied.Attachments = cloneSlice(m.Attachments)
	if m.ReplyTo != nil {
		reference := *m.ReplyTo
		copied.ReplyTo = &reference
	}
	return copied
}

// OutboundMessage is one part of an answer on its way back to a channel. Parts
// exist because protocols cap message length: one Run's answer may have to be
// several messages, and each of them is separately sent, separately retried and
// separately idempotent.
type OutboundMessage struct {
	Text string `json:"text"`
}

// Validate rejects an outbound part with nothing to deliver.
func (m OutboundMessage) Validate() error {
	if err := boundedText("outbound text", m.Text, MaxMessageTextBytes, true); err != nil {
		return err
	}
	if strings.TrimSpace(m.Text) == "" {
		return fmt.Errorf("%w: outbound text is empty", tenant.ErrInvalidArgument)
	}
	return nil
}

// Clone returns a copy that shares nothing with m.
func (m OutboundMessage) Clone() OutboundMessage { return m }

// DeliveryTarget is everything the owning adapter needs to send an answer back
// to where the question came from, after this process has restarted.
//
// Payload is versioned JSON that only the named adapter parses. Two things it
// must not be, both of which are easy mistakes:
//
//   - An irreversible hash of the recipient. A hash identifies a conversation
//     for deduplication but cannot be sent to, so an outbox row holding one is
//     an answer that can never be delivered.
//   - An in-process handle — a client, a channel, an `any`. It does not survive
//     the restart that the outbox exists to survive.
//
// Version is the adapter's own schema version for Payload, so an adapter can
// change its shape without a migration and without misreading old rows.
type DeliveryTarget struct {
	Channel ChannelType     `json:"channel"`
	Version uint16          `json:"version"`
	Payload json.RawMessage `json:"payload"`
}

// Validate rejects a target that could not be parsed by its own adapter.
//
// The payload is checked for being a JSON object rather than merely valid JSON.
// A bare string or number is valid JSON and is exactly what an adapter that
// stored a hash or an id would produce, and it leaves no room for the adapter
// to add a field later without changing the shape of everything already stored.
func (t DeliveryTarget) Validate() error {
	if err := t.Channel.Validate(); err != nil {
		return err
	}
	if t.Version == 0 {
		return fmt.Errorf("%w: delivery target version must be positive", tenant.ErrInvalidArgument)
	}
	if len(t.Payload) == 0 {
		return fmt.Errorf("%w: delivery target payload is empty", tenant.ErrInvalidArgument)
	}
	if len(t.Payload) > MaxDeliveryTargetBytes {
		return fmt.Errorf(
			"%w: delivery target payload exceeds %d bytes",
			tenant.ErrInvalidArgument, MaxDeliveryTargetBytes)
	}
	if !json.Valid(t.Payload) {
		return fmt.Errorf("%w: delivery target payload is not valid JSON", tenant.ErrInvalidArgument)
	}
	if trimmed := strings.TrimSpace(string(t.Payload)); !strings.HasPrefix(trimmed, "{") {
		return fmt.Errorf(
			"%w: delivery target payload must be a JSON object", tenant.ErrInvalidArgument)
	}
	return nil
}

// Clone returns a copy that shares no backing array with t.
func (t DeliveryTarget) Clone() DeliveryTarget {
	copied := t
	copied.Payload = cloneBytes(t.Payload)
	return copied
}

// InboundEnvelope is one external event, after the adapter has verified its
// signature, decrypted it, bounded its body and parsed it, and after the
// binding resolver has turned the external account into a tenant, an app and an
// internal principal. Nothing before those steps may construct one.
//
// There is no revision hint. An IM message cannot choose which revision answers
// it: a new session takes the app's current route and an existing one takes the
// pin the server already recorded. The HTTP development entry keeps strict hint
// semantics, and that asymmetry is deliberate — a hint is a developer
// affordance, and an IM user is not a developer of this app.
type InboundEnvelope struct {
	TenantID         string
	Channel          ChannelType
	ChannelBindingID string
	AgentAppID       string
	PrincipalID      string
	SessionID        string
	ExternalEventID  string
	ReceivedAt       time.Time
	Message          InboundMessage
	DeliveryTarget   DeliveryTarget
}

// SessionKey is the conversation this event belongs to.
func (e InboundEnvelope) SessionKey() sessiondir.Key {
	return sessiondir.Key{
		TenantID:    e.TenantID,
		AppID:       e.AgentAppID,
		PrincipalID: e.PrincipalID,
		SessionID:   e.SessionID,
	}
}

// Validate rejects an envelope the platform must not accept.
//
// SessionID is held to the ordinary resource-id rules, which is what keeps a
// raw external user, group or topic id out of it: those carry separators, and
// an adapter that concatenated them would produce a session id that fails here
// rather than one that quietly collides with another conversation.
func (e InboundEnvelope) Validate(scope tenant.TenantContext) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	// The scope is the authenticated fact and the envelope field is a claim, so
	// they are compared rather than one being trusted. An accept that read the
	// tenant out of the envelope would let a mis-wired adapter write into
	// another tenant's inbox.
	if e.TenantID != scope.TenantID {
		return fmt.Errorf(
			"%w: envelope tenant %q does not match %q",
			tenant.ErrTenantScope, e.TenantID, scope.TenantID)
	}
	if err := e.Channel.Validate(); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("channel binding id", e.ChannelBindingID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("app id", e.AgentAppID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("principal id", e.PrincipalID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("session id", e.SessionID); err != nil {
		return err
	}
	if err := boundedText(
		"external event id", e.ExternalEventID, MaxExternalEventIDBytes, true); err != nil {
		return err
	}
	if e.ReceivedAt.IsZero() {
		return fmt.Errorf("%w: received_at is required", tenant.ErrInvalidArgument)
	}
	if err := e.Message.Validate(); err != nil {
		return err
	}
	if err := e.DeliveryTarget.Validate(); err != nil {
		return err
	}
	// One event is answered on the channel it arrived on. A target naming a
	// different one would be an answer this envelope cannot authorise.
	if e.DeliveryTarget.Channel != e.Channel {
		return fmt.Errorf(
			"%w: delivery target channel does not match the envelope channel",
			tenant.ErrInvalidArgument)
	}
	return nil
}

// Clone returns a copy that shares nothing mutable with e.
func (e InboundEnvelope) Clone() InboundEnvelope {
	copied := e
	copied.Message = e.Message.Clone()
	copied.DeliveryTarget = e.DeliveryTarget.Clone()
	return copied
}

// boundedText is the one place a free-text field is checked.
//
// The error names the field and never the value. Every string this is called on
// is third-party data, and an error is the one place in this package that is
// allowed to reach a log.
//
// Valid UTF-8 is required because these values become PostgreSQL text, which
// rejects invalid encodings at the wire protocol — a check that would otherwise
// surface as an unexplained storage failure at accept time. Control characters
// are refused for the same reason they are refused in resource ids: a value
// carrying a newline can forge a line in whatever reads the log.
func boundedText(field string, value string, limit int, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("%w: %s is required", tenant.ErrInvalidArgument, field)
		}
		return nil
	}
	if len(value) > limit {
		return fmt.Errorf("%w: %s exceeds %d bytes", tenant.ErrInvalidArgument, field, limit)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is not valid UTF-8", tenant.ErrInvalidArgument, field)
	}
	for _, r := range value {
		// \t, \n and \r are allowed in a message body and nowhere else; the
		// callers that must not have them pass a limit on a single-line field
		// and are checked by the resource-id pattern instead.
		if r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf(
				"%w: %s contains a control character", tenant.ErrInvalidArgument, field)
		}
	}
	return nil
}

// cloneBytes copies a byte slice, preserving the nil/empty distinction so a
// round trip through a Store does not change what a caller sees.
func cloneBytes[T ~[]byte](value T) T {
	if value == nil {
		return nil
	}
	copied := make(T, len(value))
	copy(copied, value)
	return copied
}

// cloneSlice copies a slice of value types. Every element type it is used with
// is a struct of scalars and strings, so element copies are complete.
func cloneSlice[T any](values []T) []T {
	if values == nil {
		return nil
	}
	copied := make([]T, len(values))
	copy(copied, values)
	return copied
}

// cloneTime copies an optional timestamp.
func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

// errInvalidf builds a caller-error carrying no third-party value.
func errInvalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", tenant.ErrInvalidArgument, fmt.Sprintf(format, args...))
}
