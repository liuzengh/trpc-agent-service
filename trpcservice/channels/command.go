package channels

import (
	"context"
	"errors"
	"strings"
)

// PlatformCommand is a control command handled by the channel boundary. It
// is never passed to the model or represented as a tool call.
type PlatformCommand string

const (
	PlatformCommandNone       PlatformCommand = ""
	PlatformCommandNewSession PlatformCommand = "new_session"

	NewSessionSuccessReply = "已开启新会话。"
	NewSessionFailureReply = "暂时无法开启新会话，请稍后重试。"
	ChannelFailureReply    = "消息处理失败，请稍后重试。"
)

// RoutePlatformCommand recognizes only exact, normalized platform commands.
// Keeping this router provider-neutral makes WeCom and Feishu share the same
// command contract before either request reaches Gateway admission.
func RoutePlatformCommand(input ChannelInput) PlatformCommand {
	if input.MessageType != MessageTypeText || strings.TrimSpace(input.Text) != "/new" {
		return PlatformCommandNone
	}
	return PlatformCommandNewSession
}

// NewSessionRequest is the normalized, provider-neutral input for /new.
type NewSessionRequest struct {
	RequestID string
	Input     ChannelInput
}

// Validate checks the stable scope and mapping material required by the
// authoritative PostgreSQL command handler.
func (r NewSessionRequest) Validate() error {
	if r.RequestID == "" {
		return errors.New("new session request_id is required")
	}
	if err := r.Input.Validate(); err != nil {
		return err
	}
	if RoutePlatformCommand(r.Input) != PlatformCommandNewSession {
		return errors.New("new session command is invalid")
	}
	if _, ok := r.Input.MappingInput(); !ok {
		return errors.New("new session channel mapping is required")
	}
	return nil
}

// NewSessionHandler is the durable platform command boundary. Implementations
// must commit the active-session change and enqueue its reply atomically.
type NewSessionHandler interface {
	HandleNewSession(context.Context, NewSessionRequest) error
}
