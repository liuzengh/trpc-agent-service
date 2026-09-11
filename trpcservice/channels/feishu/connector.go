package feishu

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	larkchannel "github.com/larksuite/channel-sdk-go"
	channeltypes "github.com/larksuite/channel-sdk-go/types"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type ConnectorConfig struct {
	AppID          string
	AppSecret      string
	BaseURL        string
	HTTPClient     *http.Client
	TokenCache     larkcore.Cache
	OnReady        func()
	OnError        func(error)
	OnReconnecting func()
	OnReconnected  func()
	OnDisconnected func()
	MaxFileBytes   int64
}

type runtimeChannel interface {
	OnMessage(func(context.Context, *channeltypes.NormalizedMessage) error)
	OnCardAction(func(context.Context, *channeltypes.CardActionEvent) error)
	Send(context.Context, *channeltypes.SendInput) (*channeltypes.SendResult, error)
	DownloadFile(context.Context, string, string) ([]byte, error)
	OnReady(func())
	OnError(func(error))
	OnReconnecting(func())
	OnReconnected(func())
	OnDisconnected(func())
	Start(context.Context) error
	Stop(context.Context) error
}

// Connector wraps the official high-level Feishu Channel package. The SDK
// owns WebSocket reconnect, event decoding, deduplication and mention parsing;
// this wrapper only enforces the product policy and maps into our channel DTO.
type Connector struct {
	channel      runtimeChannel
	apiClient    *lark.Client
	maxFileBytes int64
}

func NewConnector(config ConnectorConfig) (*Connector, error) {
	if strings.TrimSpace(config.AppID) == "" || strings.TrimSpace(config.AppSecret) == "" {
		return nil, errors.New("feishu app id and secret are required")
	}
	channelOptions := []larkchannel.Option{}
	if strings.TrimSpace(config.BaseURL) != "" {
		channelOptions = append(channelOptions, larkchannel.WithOpenBaseURL(strings.TrimSpace(config.BaseURL)))
	}
	if config.HTTPClient != nil {
		channelOptions = append(channelOptions, larkchannel.WithHTTPClient(config.HTTPClient))
	}
	if config.TokenCache != nil {
		channelOptions = append(channelOptions, larkchannel.WithTokenCache(config.TokenCache), larkchannel.WithEnableTokenCache(true))
	}
	requireMention, respondToAll := true, false
	channelOptions = append(channelOptions, larkchannel.WithPolicyConfig(channeltypes.PolicyConfig{
		RequireMention: &requireMention, RespondToMentionAll: &respondToAll, DMMode: "open",
	}))
	channel, err := larkchannel.New(config.AppID, config.AppSecret, channelOptions...)
	if err != nil {
		return nil, fmt.Errorf("construct Feishu channel: %w", err)
	}
	if config.OnReady != nil {
		channel.OnReady(config.OnReady)
	}
	if config.OnError != nil {
		channel.OnError(config.OnError)
	}
	if config.OnReconnecting != nil {
		channel.OnReconnecting(config.OnReconnecting)
	}
	if config.OnReconnected != nil {
		channel.OnReconnected(config.OnReconnected)
	}
	if config.OnDisconnected != nil {
		channel.OnDisconnected(config.OnDisconnected)
	}
	connector := newConnectorWithChannel(channel)
	connector.apiClient = channel.RawClient()
	if config.MaxFileBytes > 0 {
		connector.maxFileBytes = config.MaxFileBytes
	}
	return connector, nil
}

func newConnectorWithChannel(channel runtimeChannel) *Connector {
	return &Connector{channel: channel, maxFileBytes: 16 << 20}
}

// Sender returns the outbound adapter backed by the same official Channel
// instance as this connector. This keeps one SDK client/token cache per
// binding instead of rebuilding a second Feishu client for every delivery.
func (c *Connector) Sender() (*Sender, error) {
	if c == nil || c.channel == nil || c.apiClient == nil {
		return nil, errors.New("feishu connector is not fully configured")
	}
	return newSender(c.channel, c.apiClient), nil
}

type MessageHandler = channels.InboundHandler

func (c *Connector) Run(ctx context.Context, handler MessageHandler) error {
	if c == nil || c.channel == nil {
		return errors.New("feishu connector is not configured")
	}
	c.channel.OnMessage(func(messageCtx context.Context, message *channeltypes.NormalizedMessage) error {
		if message == nil {
			return nil
		}
		scope := channels.ConversationDirect
		groupAttachmentWithoutMention := false
		if strings.EqualFold(strings.TrimSpace(message.ChatType), "group") {
			groupAttachmentWithoutMention = !message.MentionedBot && len(message.Resources) > 0
			if !message.MentionedBot && !groupAttachmentWithoutMention {
				return nil
			}
			scope = channels.ConversationGroup
		}
		messageID := strings.TrimSpace(message.MessageID)
		if messageID == "" {
			messageID = strings.TrimSpace(message.EventID)
		}
		receivedFiles, err := c.downloadResources(messageCtx, message.Resources)
		if err != nil {
			return err
		}
		if strings.TrimSpace(message.Content) == "" && len(receivedFiles) == 0 {
			return nil
		}
		content := sanitizeMentions(message.Content, message.Mentions)
		inbound := channels.InboundMessage{
			MessageID: messageID, ProviderRequestID: strings.TrimSpace(message.EventID), Channel: channels.Feishu,
			ConversationID: strings.TrimSpace(message.ChatID), SenderID: strings.TrimSpace(message.UserID),
			ConversationScope: scope, Text: content, ReceivedFiles: receivedFiles,
		}
		if strings.HasPrefix(strings.TrimSpace(content), "/") {
			inbound.TriggerType = channels.TriggerCommand
		} else if scope == channels.ConversationGroup && !groupAttachmentWithoutMention {
			inbound.TriggerType = channels.TriggerMention
		} else if scope == channels.ConversationDirect {
			inbound.TriggerType = channels.TriggerDirect
		}
		if message.CreateTimeMs > 0 {
			inbound.ReceivedAt = time.UnixMilli(message.CreateTimeMs).UTC()
		}
		if err := inbound.Validate(); err != nil {
			return err
		}
		if handler == nil {
			return nil
		}
		return handler(messageCtx, inbound)
	})
	c.channel.OnCardAction(func(actionCtx context.Context, event *channeltypes.CardActionEvent) error {
		if event == nil || handler == nil {
			return nil
		}
		senderID := strings.TrimSpace(event.Operator.OpenID)
		if senderID == "" {
			senderID = strings.TrimSpace(event.Operator.UserID)
		}
		actionID := ""
		values := make(map[string]string, len(event.Action.Value))
		for key, value := range event.Action.Value {
			text := fmt.Sprint(value)
			values[key] = text
			if key == "action_id" {
				actionID = text
			}
		}
		scope := channels.ConversationDirect
		if values["conversation_scope"] == string(channels.ConversationGroup) {
			scope = channels.ConversationGroup
		}
		messageID := strings.TrimSpace(event.EventID)
		if messageID == "" {
			messageID = "card:" + strings.TrimSpace(event.MessageID) + ":" + actionID
		}
		inbound := channels.InboundMessage{
			MessageID: messageID, ProviderRequestID: strings.TrimSpace(event.EventID), Channel: channels.Feishu,
			ConversationID: strings.TrimSpace(event.ChatID), SenderID: senderID,
			ConversationScope: scope, TriggerType: channels.TriggerAction, ReceivedAt: time.Now().UTC(),
			Action: &channels.InboundAction{ActionID: actionID, OriginMessageID: strings.TrimSpace(event.MessageID), Token: strings.TrimSpace(event.Token), Values: values},
		}
		if err := inbound.Validate(); err != nil {
			return err
		}
		slog.Info("feishu: card action received", "message_id", inbound.Action.OriginMessageID, "event_id", inbound.MessageID)
		return handler(actionCtx, inbound)
	})
	return c.channel.Start(ctx)
}

func sanitizeMentions(content string, mentions []channeltypes.Mention) string {
	for _, mention := range mentions {
		key := strings.TrimSpace(mention.Key)
		if key == "" {
			continue
		}
		replacement := ""
		if !mention.IsBot {
			replacement = strings.TrimSpace(mention.Name)
		}
		content = strings.ReplaceAll(content, key, replacement)
	}
	return strings.Join(strings.Fields(content), " ")
}

func (c *Connector) downloadResources(ctx context.Context, resources []channeltypes.Resource) ([]channels.ReceivedFile, error) {
	files := make([]channels.ReceivedFile, 0, len(resources))
	for _, resource := range resources {
		mediaType := strings.ToLower(strings.TrimSpace(resource.Type))
		switch mediaType {
		case "image", "file", "audio", "video":
		default:
			continue
		}
		fileKey := strings.TrimSpace(resource.FileKey)
		if fileKey == "" {
			continue
		}
		data, err := c.channel.DownloadFile(ctx, fileKey, mediaType)
		if err != nil {
			return nil, fmt.Errorf("download Feishu %s attachment: %w", mediaType, err)
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("download Feishu %s attachment: empty file", mediaType)
		}
		if int64(len(data)) > c.maxFileBytes {
			return nil, fmt.Errorf("Feishu attachment exceeds %d bytes", c.maxFileBytes)
		}
		name := strings.TrimSpace(resource.FileName)
		if name == "" {
			name = defaultResourceName(mediaType)
		}
		files = append(files, channels.ReceivedFile{Name: filepath.Base(name), Data: data})
	}
	return files, nil
}

func defaultResourceName(mediaType string) string {
	switch mediaType {
	case "image":
		return "image.jpg"
	case "audio":
		return "audio"
	case "video":
		return "video"
	default:
		return "attachment"
	}
}

func (c *Connector) Stop(ctx context.Context) error {
	if c == nil || c.channel == nil {
		return nil
	}
	return c.channel.Stop(ctx)
}
