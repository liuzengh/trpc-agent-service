// Package openclaw exposes an OpenClaw-compatible HTTP message surface.
package openclaw

import "encoding/json"

// ContentPart is one text, image, or file input. PR5 executes text parts and preserves the DTO for adapters.
type ContentPart struct {
	Type     string `json:"type,omitempty"`
	Text     string `json:"text,omitempty"`
	URL      string `json:"url,omitempty"`
	MIMEType string `json:"mime_type,omitempty"`
}

// MessageRequest is the stable external gateway request shape.
type MessageRequest struct {
	Channel                  string         `json:"channel,omitempty"`
	From                     string         `json:"from,omitempty"`
	To                       string         `json:"to,omitempty"`
	ConversationID           string         `json:"conversation_id,omitempty"`
	Thread                   string         `json:"thread,omitempty"`
	ThreadID                 string         `json:"thread_id,omitempty"`
	MessageID                string         `json:"message_id"`
	Text                     string         `json:"text,omitempty"`
	ContentParts             []ContentPart  `json:"content_parts,omitempty"`
	RequestSystemPrompt      string         `json:"request_system_prompt,omitempty"`
	RequestLateContextPrompt string         `json:"request_late_context_prompt,omitempty"`
	UserID                   string         `json:"user_id,omitempty"`
	SessionID                string         `json:"session_id,omitempty"`
	RequestID                string         `json:"request_id,omitempty"`
	Model                    string         `json:"model,omitempty"`
	Extensions               map[string]any `json:"extensions,omitempty"`
}

// MessageResponse acknowledges durable acceptance without waiting for the Agent.
type MessageResponse struct {
	RequestID string `json:"request_id"`
	SessionID string `json:"session_id"`
	TraceID   string `json:"trace_id"`
	Accepted  bool   `json:"accepted"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Reply     string `json:"reply,omitempty"`
	Usage     *Usage `json:"usage,omitempty"`
}

// Usage is the OpenClaw token accounting shape.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// APIError and ErrorResponse preserve the OpenClaw error envelope.
type APIError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}
type ErrorResponse struct {
	Error APIError `json:"error"`
}

// StreamEvent is serialized as one SSE data record.
type StreamEvent struct {
	Type       string          `json:"type"`
	RequestID  string          `json:"request_id"`
	SessionID  string          `json:"session_id,omitempty"`
	TraceID    string          `json:"trace_id"`
	Delta      string          `json:"delta,omitempty"`
	Reply      string          `json:"reply,omitempty"`
	Stage      string          `json:"stage,omitempty"`
	Summary    string          `json:"summary,omitempty"`
	ToolName   string          `json:"tool_name,omitempty"`
	ToolDetail string          `json:"tool_detail,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolStatus string          `json:"tool_status,omitempty"`
	ElapsedMS  int64           `json:"elapsed_ms,omitempty"`
	Usage      *Usage          `json:"usage,omitempty"`
	Ignored    bool            `json:"ignored,omitempty"`
	Error      string          `json:"error,omitempty"`
	Terminal   bool            `json:"terminal,omitempty"`
	Extensions json.RawMessage `json:"extensions,omitempty"`
}
