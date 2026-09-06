package wecommcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var ErrSourceContract = errors.New("WeCom MCP source contract unsupported")

type Adapter struct {
	secrets secret.Store
	state   Store
	client  *http.Client
}

func New(secrets secret.Store, state Store, client *http.Client) (*Adapter, error) {
	if secrets == nil || state == nil {
		return nil, errors.New("WeCom MCP secret and state stores are required")
	}
	return &Adapter{secrets: secrets, state: state, client: client}, nil
}
func (a *Adapter) Type() string { return ChannelType }
func (a *Adapter) Capabilities() channels.Capabilities {
	return channels.Capabilities{MaxTextRunes: 4000}
}

func (a *Adapter) endpoint(ctx context.Context, b controlplane.ChannelBinding, purpose string) (string, error) {
	value, err := a.secrets.Resolve(ctx, b.TenantID, purpose, b.SecretRef)
	if err != nil {
		return "", errors.New("WeCom MCP secret access denied or unavailable")
	}
	if err := ValidateEndpoint(value); err != nil {
		return "", err
	}
	return value, nil
}

func deliveryError(unknown bool) error {
	message := "WeCom MCP delivery rejected"
	if unknown {
		message = "WeCom MCP delivery outcome unknown; automatic resend blocked"
	}
	return &channels.DeliveryError{Cause: errors.New(message), Retryable: false, Unknown: unknown}
}

func (a *Adapter) Send(ctx context.Context, b controlplane.ChannelBinding, message channels.OutboundMessage) (channels.DeliveryReceipt, error) {
	ctx, span := otel.Tracer("trpc-agent-service/channel").Start(ctx, "wecom_mcp.send")
	defer span.End()
	span.SetAttributes(attribute.String("tenant.id", b.TenantID), attribute.String("channel.binding.id", b.ID))
	cfg, err := ParseBinding(b)
	if err != nil || b.Status != controlplane.StatusActive || message.OutboundID == "" || !slices.Contains(cfg.AllowedChatIDs, message.ReplyTarget) || strings.TrimSpace(message.Text) == "" || len(message.Text) > 20480 {
		return channels.DeliveryReceipt{}, deliveryError(false)
	}
	endpoint, err := a.endpoint(ctx, b, secret.WeComMCPSend)
	if err != nil {
		return channels.DeliveryReceipt{}, deliveryError(false)
	}
	input, _ := json.Marshal([]string{ConfigFingerprint(b, cfg), message.ReplyTarget, message.Text})
	hash := endpointHash(string(input))
	key := DeliveryKey{b.TenantID, b.ID, message.OutboundID}
	previous, owner, err := a.state.BeginDelivery(ctx, key, hash)
	if err != nil {
		return channels.DeliveryReceipt{}, deliveryError(true)
	}
	if !owner {
		if previous.Status == "sent" {
			return channels.DeliveryReceipt{SentAt: previous.UpdatedAt}, nil
		}
		return channels.DeliveryReceipt{}, deliveryError(previous.Status != "rejected")
	}
	args := map[string]any{"chat_id": message.ReplyTarget, "msg_type": "markdown", "markdown": map[string]any{"content": message.Text}}
	payload, isError, callErr := callRuntime(ctx, endpoint, a.client, replyTool, args)
	var response struct {
		ErrorCode *int  `json:"errcode"`
		Success   *bool `json:"success"`
	}
	status := "unknown"
	if callErr == nil && json.Unmarshal(payload, &response) == nil && response.ErrorCode != nil {
		if !isError && *response.ErrorCode == 0 && response.Success != nil && *response.Success {
			status = "sent"
		} else if *response.ErrorCode != 0 || (response.Success != nil && !*response.Success) {
			status = "rejected"
		}
	}
	// If cancellation/DB failure prevents this write, the durable attempting row
	// still prevents retry. Never convert an unavailable result into success.
	if err := a.state.FinishDelivery(ctx, key, hash, status); err != nil {
		return channels.DeliveryReceipt{}, deliveryError(true)
	}
	if status != "sent" {
		return channels.DeliveryReceipt{}, deliveryError(status == "unknown")
	}
	return channels.DeliveryReceipt{SentAt: time.Now().UTC()}, nil // Provider supplied no message ID.
}

// ReadWindow fetches an explicit [from,to) group window completely before
// returning oldest-first messages. Missing/repeated cursors fail closed.
func (a *Adapter) ReadWindow(ctx context.Context, b controlplane.ChannelBinding, chat string, from, to time.Time) ([]channels.InboundEnvelope, error) {
	ctx, span := otel.Tracer("trpc-agent-service/channel").Start(ctx, "wecom_mcp.read")
	defer span.End()
	span.SetAttributes(attribute.String("tenant.id", b.TenantID), attribute.String("channel.binding.id", b.ID))
	cfg, err := ParseBinding(b)
	if err != nil || b.Status != controlplane.StatusActive || !slices.Contains(cfg.AllowedChatIDs, chat) || !to.After(from) || to.Sub(from) > 2*time.Minute || from.Nanosecond() != 0 || to.Nanosecond() != 0 {
		return nil, errors.New("invalid WeCom MCP read scope or window")
	}
	endpoint, err := a.endpoint(ctx, b, secret.WeComMCPRead)
	if err != nil {
		return nil, err
	}
	var messages []channels.InboundEnvelope
	cursors := map[string]bool{}
	cursor := ""
	total := 0
	for page := 0; page < 20; page++ {
		args := map[string]any{"chat_id": chat, "begin_time": from.In(cfg.Location()).Format("2006-01-02 15:04:05"), "end_time": to.In(cfg.Location()).Format("2006-01-02 15:04:05")}
		if cursor != "" {
			args["cursor"] = cursor
		}
		payload, isError, err := callRuntime(ctx, endpoint, a.client, messagesTool, args)
		if err != nil || isError {
			return nil, errors.New("WeCom MCP message read failed")
		}
		items, next, more, count, err := decodePage(payload, b, cfg, chat, from, to)
		if err != nil {
			return nil, err
		}
		total += count
		if total > 1000 {
			return nil, errors.New("WeCom MCP window message limit exceeded")
		}
		messages = append(messages, items...)
		if !more {
			slices.SortFunc(messages, func(x, y channels.InboundEnvelope) int {
				if c := x.OccurredAt.Compare(y.OccurredAt); c != 0 {
					return c
				}
				return strings.Compare(x.ExternalMessageID, y.ExternalMessageID)
			})
			return messages, nil
		}
		if next == "" || len(next) > 256 || cursors[next] {
			return nil, fmt.Errorf("%w: pagination cursor missing or repeated", ErrSourceContract)
		}
		cursors[next] = true
		cursor = next
	}
	return nil, fmt.Errorf("%w: pagination limit exceeded", ErrSourceContract)
}

func decodePage(payload []byte, b controlplane.ChannelBinding, cfg BindingConfig, chat string, from, to time.Time) ([]channels.InboundEnvelope, string, bool, int, error) {
	var page struct {
		ErrorCode *int              `json:"errcode"`
		Count     *int              `json:"messages_count"`
		HasMore   *bool             `json:"has_more"`
		Next      string            `json:"next_cursor"`
		Messages  []json.RawMessage `json:"messages"`
	}
	if json.Unmarshal(payload, &page) != nil || page.ErrorCode == nil || *page.ErrorCode != 0 || page.Count == nil || *page.Count != len(page.Messages) || page.HasMore == nil {
		return nil, "", false, 0, fmt.Errorf("%w: invalid message page", ErrSourceContract)
	}
	result := []channels.InboundEnvelope{}
	for _, raw := range page.Messages {
		var m struct {
			UserID string `json:"userid"`
			Type   string `json:"msg_type"`
			Time   string `json:"send_time"`
			Text   struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if json.Unmarshal(raw, &m) != nil {
			return nil, "", false, 0, fmt.Errorf("%w: invalid message", ErrSourceContract)
		}
		// Human-ID allowlist is mandatory. No reliance on names, model output,
		// unverified is_bot fields or an assumption that bots are never returned.
		if !slices.Contains(cfg.AllowedUserIDs, m.UserID) {
			continue
		}
		stamp, err := time.ParseInLocation("2006-01-02 15:04:05", m.Time, cfg.Location())
		if err != nil || m.Type == "" {
			return nil, "", false, 0, fmt.Errorf("%w: invalid message fields", ErrSourceContract)
		}
		if stamp.Before(from) || !stamp.Before(to) {
			continue
		}
		// Only the observed text contract is enabled. Unknown/media contracts
		// stop the window visibly instead of dropping messages or downloading.
		if m.Type != "text" || len(m.Text.Content) > 32768 {
			return nil, "", false, 0, fmt.Errorf("%w: text-only message size/type limit", ErrSourceContract)
		}
		body := strings.TrimSpace(m.Text.Content)
		if !strings.HasPrefix(body, cfg.MentionPrefix) {
			continue
		}
		tail := strings.TrimPrefix(body, cfg.MentionPrefix)
		if tail == "" {
			continue
		}
		first := []rune(tail)[0]
		if cfg.MentionStyle != "prefix" && !unicode.IsSpace(first) {
			continue
		}
		text := strings.TrimSpace(tail)
		if text == "" {
			continue
		}
		identity, _ := json.Marshal([]string{b.TenantID, b.ID, chat, m.UserID, m.Time, m.Type, m.Text.Content})
		result = append(result, channels.InboundEnvelope{ExternalMessageID: "mcp_fp1_" + endpointHash(string(identity)), ExternalUserID: m.UserID, ExternalChatID: chat, ChatType: "group", MessageType: "text", Text: text, ReplyTarget: chat, OccurredAt: stamp.UTC()})
	}
	return result, page.Next, *page.HasMore, len(page.Messages), nil
}
