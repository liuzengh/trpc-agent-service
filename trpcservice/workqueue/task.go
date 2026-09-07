// Package workqueue defines the durable payload exchanged between Gateway and
// Agent Worker implementations.
package workqueue

import (
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

// AgentTask contains everything a Worker needs after an inbound transaction
// commits. It contains trusted scope resolved by the Gateway.
type AgentTask struct {
	Media             *runtimecontext.MediaReference `json:"media,omitempty"`
	InboundID         string                         `json:"inbound_id"`
	RequestID         string                         `json:"request_id"`
	ConversationID    string                         `json:"conversation_id"`
	Scope             runtimecontext.Scope           `json:"scope"`
	MessageID         string                         `json:"message_id"`
	UserID            string                         `json:"user_id"`
	SessionID         string                         `json:"session_id"`
	Text              string                         `json:"text"`
	ReplyTarget       string                         `json:"reply_target"`
	TurnSeq           int64                          `json:"turn_seq"`
	Attempt           int                            `json:"attempt"`
	TraceParent       string                         `json:"traceparent,omitempty"`
	TraceState        string                         `json:"tracestate,omitempty"`
	ApprovedTools     []string                       `json:"approved_tools,omitempty"`
	ApprovedToolCalls []governance.ApprovedToolCall  `json:"approved_tool_calls,omitempty"`
	ApprovalID        string                         `json:"approval_id,omitempty"`
}
