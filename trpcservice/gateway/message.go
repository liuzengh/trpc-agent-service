package gateway

import (
	"fmt"
)

// InboundMessage is the platform-independent message produced by an adapter.
type InboundMessage struct {
	Channel        string
	RouteKey       string
	MsgID          string
	ChatType       string
	SenderID       string
	GroupID        string
	AddressedToBot bool
	Text           string
	Raw            any
	TraceID        string
}

// Outcome is the typed terminal result of inbound routing.
type Outcome string

const (
	OutcomeIngested     Outcome = "ingested"
	OutcomeDropped      Outcome = "dropped"
	OutcomeNeedsBinding Outcome = "needs_binding"
	OutcomeAgentOffline Outcome = "agent_offline"
)

// DropReason gives a stable audit/metric label for dropped messages.
type DropReason string

const (
	DropDuplicate           DropReason = "duplicate"
	DropNotAddressedInGroup DropReason = "not_addressed_in_group"
	DropInvalidEvent        DropReason = "invalid_event"
	DropRevokedBinding      DropReason = "revoked_binding"
	DropUnboundBinding      DropReason = "unbound_binding"
)

// Result is returned to adapters so they can ACK without waiting for execution.
type Result struct {
	Outcome    Outcome    `json:"outcome"`
	DropReason DropReason `json:"drop_reason,omitempty"`
	SessionID  string     `json:"session_id,omitempty"`
}

// DeriveSessionID deterministically isolates tenant/channel conversations.
// The chat-type tag keeps the p2p and group namespaces disjoint, so a sender
// ID like "group:x" can never collide with a group conversation ID "x".
func DeriveSessionID(tenantID, channel string, message InboundMessage) string {
	if message.ChatType == "group" {
		return fmt.Sprintf("%s:%s:group:%s", tenantID, channel, message.GroupID)
	}
	return fmt.Sprintf("%s:%s:p2p:%s", tenantID, channel, message.SenderID)
}

// BuildDedupKey scopes platform message IDs to a tenant channel binding.
func BuildDedupKey(tenantID, bindingID, msgID string) string {
	return fmt.Sprintf("%s:%s:%s", tenantID, bindingID, msgID)
}
