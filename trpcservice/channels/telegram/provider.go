package telegram

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

// Provider delivers durable outbox segments through one trusted Telegram
// destination. The stable outbox key is also used as the local provider
// idempotency key; Telegram itself remains at-least-once unless it supports a
// matching external idempotency facility.
type Provider struct {
	client      BotClient
	chatID      int64
	threadID    int
	attachments attachment.Reader
	mu          sync.Mutex
	receipts    map[string]string
}

type telegramPhotoSender interface {
	SendPhoto(context.Context, *bot.SendPhotoParams) (*models.Message, error)
}

type telegramDocumentSender interface {
	SendDocument(context.Context, *bot.SendDocumentParams) (*models.Message, error)
}

// ProviderOption configures optional Telegram reply-provider dependencies.
type ProviderOption func(*Provider)

// WithAttachmentReader enables native media delivery from durable attachment
// references. Nil readers are ignored and keep the text-only fallback path.
func WithAttachmentReader(reader attachment.Reader) ProviderOption {
	return func(provider *Provider) {
		if reader != nil {
			provider.attachments = reader
		}
	}
}

// NewProvider creates a Telegram reply provider for a chat and optional thread.
func NewProvider(client BotClient, chatID int64, threadID int, options ...ProviderOption) (*Provider, error) {
	if client == nil || chatID == 0 || threadID < 0 {
		return nil, outbox.ErrInvalid
	}
	provider := &Provider{client: client, chatID: chatID, threadID: threadID, receipts: map[string]string{}}
	for _, option := range options {
		if option != nil {
			option(provider)
		}
	}
	return provider, nil
}

// Deliver sends one durable reply segment and returns the provider message ID.
func (p *Provider) Deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	if p == nil || p.client == nil || ctx == nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	key := deliveryKey(value)
	p.mu.Lock()
	if receipt := p.receipts[key]; receipt != "" {
		p.mu.Unlock()
		return receipt, nil
	}
	p.mu.Unlock()
	message, err := p.deliverMessage(ctx, value)
	if err != nil {
		return "", err
	}
	if message == nil || message.ID <= 0 {
		return "", &outbox.DeliveryError{Class: "provider_invalid_receipt", Retryable: false}
	}
	receipt := strconv.Itoa(message.ID)
	p.mu.Lock()
	p.receipts[key] = receipt
	p.mu.Unlock()
	return receipt, nil
}

func (p *Provider) deliverMessage(ctx context.Context, value runtimestorage.ReplyOutbox) (*models.Message, error) {
	if value.Kind == "" || value.Kind == runtimestorage.ReplyKindText {
		return p.sendText(ctx, value.Payload)
	}
	normalized, err := runtimestorage.NormalizeReplyOutbox(value)
	if err != nil {
		return nil, &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	switch normalized.Kind {
	case runtimestorage.ReplyKindImage:
		return p.sendPhoto(ctx, normalized)
	case runtimestorage.ReplyKindDocument:
		return p.sendDocument(ctx, normalized)
	default:
		return p.sendText(ctx, normalized.Fallback)
	}
}

func (p *Provider) sendPhoto(ctx context.Context, value runtimestorage.ReplyOutbox) (*models.Message, error) {
	sender, ok := p.client.(telegramPhotoSender)
	if !ok || p.attachments == nil {
		return p.sendText(ctx, value.Fallback)
	}
	file, ok, err := p.attachmentUpload(ctx, value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return p.sendText(ctx, value.Fallback)
	}
	message, err := sender.SendPhoto(ctx, &bot.SendPhotoParams{ChatID: p.chatID, MessageThreadID: p.threadID, Photo: file, Caption: value.Payload})
	if err != nil {
		return nil, telegramDeliveryError(ctx, err)
	}
	return message, nil
}

func (p *Provider) sendDocument(ctx context.Context, value runtimestorage.ReplyOutbox) (*models.Message, error) {
	sender, ok := p.client.(telegramDocumentSender)
	if !ok || p.attachments == nil {
		return p.sendText(ctx, value.Fallback)
	}
	file, ok, err := p.attachmentUpload(ctx, value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return p.sendText(ctx, value.Fallback)
	}
	message, err := sender.SendDocument(ctx, &bot.SendDocumentParams{ChatID: p.chatID, MessageThreadID: p.threadID, Document: file, Caption: value.Payload})
	if err != nil {
		return nil, telegramDeliveryError(ctx, err)
	}
	return message, nil
}

func (p *Provider) attachmentUpload(ctx context.Context, value runtimestorage.ReplyOutbox) (*models.InputFileUpload, bool, error) {
	content, err := p.attachments.Load(ctx, value.TenantID, value.EventID, value.Attachment)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return nil, false, &outbox.DeliveryError{Class: "canceled", Retryable: true}
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, false, &outbox.DeliveryError{Class: "timeout", Retryable: true}
		}
		return nil, false, nil
	}
	if err := content.Validate(value.Attachment); err != nil {
		return nil, false, nil
	}
	name := value.Attachment.Name
	if name == "" {
		name = value.Attachment.ID
	}
	return &models.InputFileUpload{Filename: name, Data: bytes.NewReader(content.Data)}, true, nil
}

func (p *Provider) sendText(ctx context.Context, text string) (*models.Message, error) {
	message, err := p.client.SendMessage(ctx, &bot.SendMessageParams{ChatID: p.chatID, MessageThreadID: p.threadID, Text: text})
	if err != nil {
		return nil, telegramDeliveryError(ctx, err)
	}
	return message, nil
}

func telegramDeliveryError(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return &outbox.DeliveryError{Class: "canceled", Retryable: true}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &outbox.DeliveryError{Class: "timeout", Retryable: true}
	}
	return &outbox.DeliveryError{Class: "provider_error", Retryable: true}
}

// Reconcile checks whether a previously attempted segment can be confirmed.
func (p *Provider) Reconcile(_ context.Context, value runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if p == nil {
		return outbox.DeliveryUnknown, "", nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if receipt := p.receipts[deliveryKey(value)]; receipt != "" {
		return outbox.DeliveryAccepted, receipt, nil
	}
	return outbox.DeliveryUnknown, "", nil
}

func deliveryKey(value runtimestorage.ReplyOutbox) string {
	return value.TenantID + "\x00" + value.ReplyID + "\x00" + strconv.Itoa(value.SegmentIndex)
}
