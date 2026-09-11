package wecommcp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

const (
	sessionsTool = "message_aibot_sessions_list"
	messagesTool = "chat_messages_list"
	replyTool    = "message_aibot_send"
)

func endpointHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// toolPayload normalizes the structured and text encodings supported by the
// MCP channel. The caller validates the channel-specific payload contract.
func toolPayload(result *mcp.CallToolResult) (json.RawMessage, error) {
	if result.StructuredContent != nil {
		return json.Marshal(result.StructuredContent)
	}
	if len(result.Content) == 1 {
		switch item := result.Content[0].(type) {
		case mcp.TextContent:
			if json.Valid([]byte(item.Text)) {
				return json.RawMessage(item.Text), nil
			}
		case *mcp.TextContent:
			if json.Valid([]byte(item.Text)) {
				return json.RawMessage(item.Text), nil
			}
		}
	}
	return json.Marshal(result.Content)
}
