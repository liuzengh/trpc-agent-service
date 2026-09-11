package channels

import "strings"

// PlatformCommand is handled at the channel boundary and never reaches the
// Runner or the model.
type PlatformCommand string

const (
	PlatformCommandNone       PlatformCommand = ""
	PlatformCommandNewSession PlatformCommand = "new_session"

	NewSessionSuccessReply = "已开启新会话，当前对话上下文已清空；长期偏好仍保留。"
)

// RoutePlatformCommand recognizes only exact provider-neutral commands. A
// command must not be inferred from ordinary natural language.
func RoutePlatformCommand(message InboundMessage) PlatformCommand {
	if message.Action != nil || len(message.ReceivedFiles) > 0 || len(message.Files) > 0 {
		return PlatformCommandNone
	}
	if strings.TrimSpace(message.Text) == "/new" {
		return PlatformCommandNewSession
	}
	return PlatformCommandNone
}
