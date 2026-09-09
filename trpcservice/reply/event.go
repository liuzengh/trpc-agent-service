// Package reply is the platform-neutral Agent event seen by Channel adapters.
// It lives outside worker so IM adapters do not import the execution package.
package reply

// Event is the projection of one Worker output item onto an IM reply stream.
type Event struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	ToolName    string `json:"tool_name,omitempty"`
	Error       string `json:"error,omitempty"`
	UsageTokens int    `json:"usage_tokens,omitempty"`
	Decision    string `json:"decision,omitempty"`
}
