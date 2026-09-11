package wecombot

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func (c *Client) normalizeInbound(ctx context.Context, frame WireFrame, body InboundMsgBody) (channels.InboundMessage, bool, error) {
	if body.AIBotID != "" && body.AIBotID != c.config.BotID {
		return channels.InboundMessage{}, false, nil
	}
	messageID := strings.TrimSpace(body.MsgID)
	if messageID == "" {
		messageID = strings.TrimSpace(frame.Headers.RequestID)
	}
	senderID := strings.TrimSpace(body.From.UserID)
	conversationID := senderID
	scope := channels.ConversationDirect
	switch body.ChatType {
	case "group":
		conversationID = strings.TrimSpace(body.ChatID)
		scope = channels.ConversationGroup
	case "single":
	default:
		return channels.InboundMessage{}, false, nil
	}

	var textParts []string
	var files []channels.ReceivedFile
	appendMedia := func(mediaType string, media mediaContent) error {
		file, err := c.downloadMedia(ctx, mediaType, media)
		if err != nil {
			return err
		}
		files = append(files, file)
		return nil
	}
	switch body.MsgType {
	case "text":
		textParts = appendText(textParts, body.Text.Content)
	case "voice":
		textParts = appendText(textParts, body.Voice.Content)
	case "image":
		if err := appendMedia("image", body.Image); err != nil {
			return channels.InboundMessage{}, false, err
		}
	case "file":
		if err := appendMedia("file", body.File); err != nil {
			return channels.InboundMessage{}, false, err
		}
	case "video":
		if err := appendMedia("video", body.Video); err != nil {
			return channels.InboundMessage{}, false, err
		}
	case "mixed":
		for _, item := range body.Mixed.Items {
			switch item.MsgType {
			case "text":
				textParts = appendText(textParts, item.Text.Content)
			case "image":
				if err := appendMedia("image", item.Image); err != nil {
					return channels.InboundMessage{}, false, err
				}
			}
		}
	default:
		return channels.InboundMessage{}, false, nil
	}
	if len(textParts) == 0 && len(files) == 0 {
		return channels.InboundMessage{}, false, nil
	}
	inbound := channels.InboundMessage{
		MessageID: messageID, ProviderRequestID: strings.TrimSpace(frame.Headers.RequestID), Channel: channels.WeCom,
		ConversationID: conversationID, SenderID: senderID,
		ConversationScope: scope, Text: strings.Join(textParts, "\n"), ReceivedFiles: files,
		ProviderReplyToken: strings.TrimSpace(frame.Headers.RequestID), ReceivedAt: time.Now().UTC(),
	}
	if strings.HasPrefix(strings.TrimSpace(inbound.Text), "/") {
		inbound.TriggerType = channels.TriggerCommand
	} else if scope == channels.ConversationGroup {
		inbound.TriggerType = channels.TriggerMention
	} else {
		inbound.TriggerType = channels.TriggerDirect
	}
	if body.CreateTime > 0 {
		inbound.ReceivedAt = time.Unix(body.CreateTime, 0).UTC()
	}
	if err := inbound.Validate(); err != nil {
		return channels.InboundMessage{}, false, fmt.Errorf("normalize WeCom message: %w", err)
	}
	return inbound, true, nil
}

func appendText(parts []string, value string) []string {
	if value = strings.TrimSpace(value); value != "" {
		return append(parts, value)
	}
	return parts
}

func normalizeCardAction(frame WireFrame, body EventCallbackBody) (channels.InboundMessage, bool) {
	if body.Event.Type != "template_card_event" {
		return channels.InboundMessage{}, false
	}
	eventKey := strings.TrimSpace(body.Event.TemplateCardEvent.EventKey)
	if eventKey == "" {
		eventKey = strings.TrimSpace(body.Event.EventKey)
	}
	taskID := strings.TrimSpace(body.Event.TemplateCardEvent.TaskID)
	if taskID == "" {
		taskID = strings.TrimSpace(body.Event.TaskID)
	}
	senderID := strings.TrimSpace(body.From.UserID)
	if senderID == "" || eventKey == "" {
		return channels.InboundMessage{}, false
	}
	scope := channels.ConversationDirect
	conversationID := senderID
	if body.ChatType == "group" {
		scope = channels.ConversationGroup
		conversationID = strings.TrimSpace(body.ChatID)
	}
	if conversationID == "" {
		return channels.InboundMessage{}, false
	}
	messageID := strings.TrimSpace(body.MsgID)
	if messageID == "" {
		messageID = "card:" + strings.TrimSpace(frame.Headers.RequestID)
	}
	inbound := channels.InboundMessage{
		MessageID: messageID, ProviderRequestID: strings.TrimSpace(frame.Headers.RequestID), Channel: channels.WeCom,
		ConversationID: conversationID, SenderID: senderID,
		ConversationScope: scope, TriggerType: channels.TriggerAction, ReceivedAt: time.Now().UTC(),
		ProviderReplyToken: strings.TrimSpace(frame.Headers.RequestID),
		Action: &channels.InboundAction{
			ActionID: eventKey, CallbackID: strings.TrimSpace(frame.Headers.RequestID), Token: taskID,
		},
	}
	if body.CreateTime > 0 {
		inbound.ReceivedAt = time.Unix(body.CreateTime, 0).UTC()
	}
	return inbound, inbound.Validate() == nil
}
