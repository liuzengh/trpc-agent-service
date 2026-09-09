// Package wecom speaks the WeCom intelligent-bot long-connection protocol for
// one thing only: single-chat text in, one final text reply out.
//
// It is a protocol adapter and nothing else. It owns the wire format, the
// subscribe handshake, the heartbeat, the reconnect policy and the reply
// receipt, and it hands the caller a normalized value per accepted message. It
// has no store, no queue, no dispatcher and no Runner: an inbound message
// exists here only until a consumer takes it off the channel, and a reply is
// sent because the caller asked for one.
//
// What that means for the guarantees this package may claim:
//
//   - No deduplication. Every accepted message carries the platform's
//     ExternalMessageID unchanged so that a durable Ingress downstream can
//     deduplicate on it. Two deliveries of one message are two values here.
//   - No durability and no crash recovery. There is no protocol-level
//     acknowledgement for an inbound callback — the platform does not wait for
//     one and this package never invents one — so a message that has been read
//     off the wire and not yet consumed is lost if the process stops.
//   - No exactly-once reply. A receipt is a definite success, a rejection is a
//     definite failure, and a timeout or a disconnect after the frame was
//     written is an unknown outcome that this package reports rather than
//     retries.
//
// Nothing here is logged. The Bot ID, the Secret, external user and message
// ids and message bodies are all sensitive, and the cheapest way to keep them
// out of logs is to have no log statement in the package at all. Errors are
// sentinels with fixed text; none of them repeats a credential, an external
// identifier, a message body or the platform's own errmsg.
package wecom

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrConfig reports a Binding or Config this package will not run with. It
	// is a startup fault: the same configuration fails identically forever.
	ErrConfig = errors.New("wecom: invalid adapter configuration")

	// ErrAuthRejected reports a subscribe frame the platform answered with a
	// non-zero errcode. It is terminal: the credential cannot fix itself, and
	// redialing with a rejected Secret is how an account gets locked out.
	ErrAuthRejected = errors.New("wecom: the platform rejected the subscribe request")

	// ErrTakenOver reports the platform's disconnected_event: a second client
	// subscribed with this bot's credential and now owns the connection. It is
	// terminal because reconnecting would take the connection back and start a
	// takeover fight that drops messages on both sides.
	ErrTakenOver = errors.New("wecom: another connection took this bot over")

	// ErrInboundOverflow reports a consumer that fell far enough behind to fill
	// the inbound buffer. It is terminal, and it is not a dropped message: the
	// protocol has no inbound acknowledgement, so discarding a frame the
	// platform believes was delivered would be a silent loss. Failing loudly is
	// the honest option.
	ErrInboundOverflow = errors.New("wecom: inbound buffer is full")

	// ErrHeartbeatLost reports consecutive unanswered pings. It is an ordinary
	// connection failure and reconnects.
	ErrHeartbeatLost = errors.New("wecom: heartbeat acknowledgements stopped")

	// ErrReconnectExhausted reports that the reconnect budget ran out.
	ErrReconnectExhausted = errors.New("wecom: reconnect attempts exhausted")

	// ErrRunning reports a second concurrent Run. One Binding is one
	// connection: a second one would be taken over by the first.
	ErrRunning = errors.New("wecom: client is already running")

	// ErrNotConnected reports a reply attempted with no authenticated
	// connection to write it on.
	ErrNotConnected = errors.New("wecom: no authenticated connection")

	// ErrReplyTargetExpired reports a reply target minted by an earlier
	// connection. Targets are only valid on the connection that received the
	// message: the platform correlates a reply by the callback's req_id, and a
	// req_id from a connection that no longer exists addresses nothing.
	ErrReplyTargetExpired = errors.New("wecom: reply target belongs to an earlier connection")

	// ErrReplyAlreadySent reports a second reply for one inbound message. The
	// final stream for a callback is sent once; a second independent final
	// stream on the same req_id is not a retry, it is a second answer.
	ErrReplyAlreadySent = errors.New("wecom: a final reply was already sent for this message")

	// ErrReplyRejected reports a receipt carrying a non-zero errcode. The reply
	// definitely did not land, and it is not retried here.
	ErrReplyRejected = errors.New("wecom: the platform rejected the reply")

	// ErrReplyOutcomeUnknown reports a reply that was written but never
	// receipted — an ack timeout, a disconnect, or a write that failed after
	// bytes may already have left. It is neither success nor failure, and the
	// connection is retired so that a late receipt cannot be matched to
	// anything.
	ErrReplyOutcomeUnknown = errors.New("wecom: reply outcome unknown")

	// ErrTextTooLong reports reply text above the documented stream limit. It
	// is raised before anything is written.
	ErrTextTooLong = errors.New("wecom: reply text exceeds the 20480-byte limit")

	// ErrTextInvalid reports reply text that is blank or not valid UTF-8.
	ErrTextInvalid = errors.New("wecom: reply text must be non-blank valid UTF-8")

	// errRejected is the internal reason a callback was not accepted. It never
	// reaches a caller: a rejected callback is simply not delivered, and the
	// connection stays up. Its messages name fields, never values.
	errRejected = errors.New("wecom: callback rejected")
)

// Binding is the static, server-side trust anchor for one WeCom bot: which
// tenant and app own it, which bot account it is, and which credential
// reference unlocks it.
//
// Everything about who a message belongs to comes from here. Nothing in an
// inbound frame selects a tenant, an app or a binding — a frame that claims a
// different bot is rejected rather than routed — because a payload that could
// pick its own tenant is a payload that could pick someone else's.
//
// It carries a SecretRef, never a Secret. The reference goes through the same
// entitlement table and the same parser as a model key does.
type Binding struct {
	TenantID   string
	AgentAppID string
	BindingID  string
	// BotID is the platform's bot account id. It is matched exactly against
	// body.aibotid on every callback and is never derived from one.
	BotID string
	// SecretRef names the bot Secret in the platform's "env:VAR" syntax.
	SecretRef string
}

// Validate reports whether this binding is well formed. It is checked before a
// Client is built and before the Secret is resolved.
func (b Binding) Validate() error {
	if err := tenant.ValidateResourceID("tenant_id", b.TenantID); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if err := tenant.ValidateResourceID("agent_app_id", b.AgentAppID); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if err := tenant.ValidateResourceID("channel_binding_id", b.BindingID); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	// The bot id is an external identifier, so it is bounded and never
	// repeated in the refusal.
	if b.BotID == "" || len(b.BotID) > maxExternalIDBytes {
		return fmt.Errorf("%w: bot_id is empty or longer than %d bytes",
			ErrConfig, maxExternalIDBytes)
	}
	// secretref owns the syntax. Parsing it here means a reference that the
	// entitlement table would read as one variable cannot be resolved as
	// another.
	if _, err := secretref.EnvName(b.SecretRef); err != nil {
		return fmt.Errorf("%w: secret_ref: %w", ErrConfig, err)
	}
	return nil
}

// DirectText is one accepted WeCom single-chat text message, normalized onto
// platform identifiers.
//
// It is deliberately not the platform's durable InboundEnvelope. This package
// is a protocol slice with no store behind it, and a second value claiming to
// be the canonical envelope would be a second contract to keep in step. A
// later Ingress translates this into the durable one; every field it needs to
// do that is here, and the external message id is carried through unchanged so
// that the translation, not this package, owns deduplication.
type DirectText struct {
	// TenantID, AgentAppID and BindingID are copied from the static Binding,
	// never from the frame.
	TenantID   string
	AgentAppID string
	BindingID  string
	// PrincipalID is the sender, mapped into this tenant and binding. It is a
	// digest: the external userid never enters a session namespace, a cache
	// key or a log.
	PrincipalID string
	// SessionID is the deterministic direct-mode session for this principal.
	SessionID string
	// ExternalMessageID is body.msgid, unchanged, for downstream deduplication.
	// It is sensitive and belongs in PostgreSQL, not in logs or metric labels.
	ExternalMessageID string
	// Text is body.text.content: non-blank, valid UTF-8, bounded.
	Text string
	// ReceivedAt is when this process read the frame, not a platform timestamp.
	ReceivedAt time.Time
	// Reply addresses the answer to this message. It is only usable on the
	// connection that delivered it.
	Reply ReplyTarget
}

// ReplyTarget addresses one final reply.
//
// Its fields are unexported so that a target can only come from a message this
// client accepted: a caller that could assemble one could address a req_id the
// platform never sent, or one belonging to a connection that has since been
// replaced.
//
// The generation is a per-connection random identity. It makes a stale target
// — held across a reconnect, or across a process restart — refusable by value
// rather than by hoping the req_id space does not repeat.
type ReplyTarget struct {
	generation string
	reqID      string
	streamID   string
}

// Identity domains. Each digest names its own scheme so that two hashes of the
// same fields for different purposes cannot collide, and so a v2 of a scheme
// produces different values by construction rather than by remembering to.
const (
	principalDomain = "im-principal-v1"
	sessionDomain   = "im-session-v1"
	streamDomain    = "im-stream-v1"

	// directEpoch is the session epoch this package emits. Bumping an epoch
	// clears a conversation's context, which is a stored per-session decision;
	// with no store here there is nothing to read, so it is fixed at zero and
	// the field stays in the digest for the day there is.
	directEpoch = "0"

	// directThreadID is the empty thread id direct chats carry: WeCom
	// single-chat has no thread, and the field stays in the digest so that
	// adding threads later cannot silently collide with today's sessions.
	directThreadID = ""
)

// digest is the platform's deterministic identifier hash: hex(SHA-256) over a
// JSON string array.
//
// The array is the point. Unbounded concatenation lets "ab"+"c" and "a"+"bc"
// produce the same bytes, which is how a derived id starts addressing another
// tenant's session; JSON quotes and escapes every element, so the boundaries
// survive.
//
// Fields must be valid UTF-8 before they get here, because encoding/json
// replaces invalid bytes with U+FFFD: two distinct external ids could
// otherwise hash to one principal. checkExternalID enforces that on the values
// this package derives ids from.
func digest(fields ...string) string {
	// A []string always marshals: no field type here can fail, and the error
	// return exists for types that can.
	encoded, err := json.Marshal(fields)
	if err != nil {
		panic("wecom: marshalling a string array cannot fail: " + err.Error())
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// principalID maps one external WeCom user onto a platform principal.
//
// The scope is (tenant, binding, external userid) and nothing else, which is
// the uniqueness key the data model gives external principals. The same person
// reached through two bindings is two principals: cross-channel identities are
// not merged by accident, and one binding's user id can never resolve into
// another tenant's session.
func principalID(tenantID, bindingID, externalUserID string) string {
	return "p-" + digest(principalDomain, tenantID, bindingID, "user", externalUserID)
}

// directSessionID is the direct-mode session name from the platform's session
// naming rules: "d-" + H(base + ["direct", principal_id, thread_id, epoch]).
//
// The result is 66 characters, inside the 67-character session limit, and uses
// only characters ValidateResourceID accepts.
func directSessionID(tenantID, agentAppID, bindingID, principal string) string {
	return "d-" + digest(
		sessionDomain, tenantID, agentAppID, bindingID,
		"direct", principal, directThreadID, directEpoch)
}

// streamID is the stable stream id for the reply to one inbound message.
//
// Stable, so that the same message answered twice — by a retry above this
// package, after a reconnect — names the same stream rather than opening a
// second one. Derived rather than random for that reason, and hashed so the
// external message id does not travel back out on the wire in a field the
// platform echoes.
func streamID(tenantID, bindingID, externalMessageID string) string {
	return "s-" + digest(streamDomain, tenantID, bindingID, externalMessageID)
}
