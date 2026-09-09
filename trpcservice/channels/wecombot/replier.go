package wecombot

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

// streamUpdateInterval throttles intermediate stream refreshes; the protocol
// requires finish=true within 10 minutes of the first segment, which matches
// the gateway execution timeout.
const (
	streamUpdateInterval = 300 * time.Millisecond
	fallbackReply        = "请求已处理，但没有可展示的文本结果。"
)

type replier struct {
	adapter  *Adapter
	snapshot tenant.Snapshot
}

// Reply streams Worker events back as one WeCom AI-bot stream message.
func (r *replier) Reply(
	ctx context.Context,
	_ string,
	message gateway.InboundMessage,
	events <-chan reply.Event,
) error {
	target, ok := message.Raw.(replyTarget)
	if !ok || target.RouteKey == "" || target.ReqID == "" {
		for range events {
		}
		return errors.New("WeCom bot reply target is missing")
	}
	bot := r.adapter.connection(target.RouteKey)
	if bot == nil {
		for range events {
		}
		return errors.New("WeCom bot connection is not active")
	}
	streamID, err := newRequestID()
	if err != nil {
		for range events {
		}
		return err
	}

	var (
		content    strings.Builder
		lastUpdate time.Time
		started    bool
	)
	for event := range events {
		switch event.Type {
		case "text_delta":
			content.WriteString(event.Text)
		case "tool_call":
			if event.ToolName != "" {
				fmt.Fprintf(&content, "\n\n> 正在调用工具：%s", event.ToolName)
			}
		case "error":
			if event.Error != "" {
				fmt.Fprintf(&content, "\n\n执行失败：%s", event.Error)
			}
		}
		if content.Len() == 0 {
			continue
		}
		if !started {
			if err := r.sendStream(bot, target.ReqID, streamID, content.String(), false); err != nil {
				return err
			}
			started = true
			lastUpdate = time.Now()
			continue
		}
		if event.Type == "done" || time.Since(lastUpdate) >= streamUpdateInterval {
			finish := event.Type == "done"
			if err := r.sendStream(bot, target.ReqID, streamID, content.String(), finish); err != nil {
				return err
			}
			lastUpdate = time.Now()
			if finish {
				return nil
			}
		}
	}
	if content.Len() == 0 {
		content.WriteString(fallbackReply)
	}
	if !started {
		return r.sendStream(bot, target.ReqID, streamID, content.String(), true)
	}
	// The event stream ended without a done event; close the stream anyway.
	return r.sendStream(bot, target.ReqID, streamID, content.String(), true)
}

func (r *replier) sendStream(
	bot *botConn,
	reqID string,
	streamID string,
	content string,
	finish bool,
) error {
	if err := bot.send(streamFrame(reqID, streamID, content, finish)); err != nil {
		return fmt.Errorf("send wecombot stream: %w", err)
	}
	return nil
}
