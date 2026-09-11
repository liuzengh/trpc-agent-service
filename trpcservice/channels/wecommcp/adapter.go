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

type RejectedMessage struct {
	Fingerprint string    `json:"fingerprint"`
	Reason      string    `json:"reason"`
	ObservedAt  time.Time `json:"observed_at"`
	WindowFrom  time.Time `json:"window_from"`
	WindowTo    time.Time `json:"window_to"`
	TraceID     string    `json:"trace_id,omitempty"`
}
type WindowBatch struct {
	Messages []channels.InboundEnvelope
	Rejected []RejectedMessage
}

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
	batch, err := a.ReadWindowBatch(ctx, b, chat, from, to)
	if err != nil {
		return nil, err
	}
	if len(batch.Rejected) > 0 {
		return nil, fmt.Errorf("%w: batch reader required for rejected records", ErrSourceContract)
	}
	return batch.Messages, nil
}

func (a *Adapter) ReadWindowBatch(ctx context.Context, b controlplane.ChannelBinding, chat string, from, to time.Time) (WindowBatch, error) {
	ctx, span := otel.Tracer("trpc-agent-service/channel").Start(ctx, "wecom_mcp.read")
	defer span.End()
	span.SetAttributes(attribute.String("tenant.id", b.TenantID), attribute.String("channel.binding.id", b.ID))
	cfg, err := ParseBinding(b)
	if err != nil || b.Status != controlplane.StatusActive || !slices.Contains(cfg.AllowedChatIDs, chat) || !to.After(from) || to.Sub(from) > 2*time.Minute || from.Nanosecond() != 0 || to.Nanosecond() != 0 {
		return WindowBatch{}, errors.New("invalid WeCom MCP read scope or window")
	}
	if cfg.ManagedIdentity && (len(cfg.GroupGrants[chat].Members) == 0 || from.Before(cfg.StartFor(chat))) {
		return WindowBatch{}, errors.New("group read is outside its authorization boundary")
	}
	endpoint, err := a.endpoint(ctx, b, secret.WeComMCPRead)
	if err != nil {
		return WindowBatch{}, err
	}
	var messages []channels.InboundEnvelope
	var rejected []RejectedMessage
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
			return WindowBatch{}, errors.New("WeCom MCP message read failed")
		}
		batch, next, more, count, err := decodePageBatch(payload, b, cfg, chat, from, to)
		if err != nil {
			return WindowBatch{}, err
		}
		total += count
		if total > 1000 {
			return WindowBatch{}, errors.New("WeCom MCP window message limit exceeded")
		}
		messages = append(messages, batch.Messages...)
		rejected = append(rejected, batch.Rejected...)
		if !more {
			slices.SortFunc(messages, func(x, y channels.InboundEnvelope) int {
				if c := x.OccurredAt.Compare(y.OccurredAt); c != 0 {
					return c
				}
				return strings.Compare(x.ExternalMessageID, y.ExternalMessageID)
			})
			return WindowBatch{Messages: messages, Rejected: rejected}, nil
		}
		if next == "" || len(next) > 256 || cursors[next] {
			return WindowBatch{}, fmt.Errorf("%w: pagination cursor missing or repeated", ErrSourceContract)
		}
		cursors[next] = true
		cursor = next
	}
	return WindowBatch{}, fmt.Errorf("%w: pagination limit exceeded", ErrSourceContract)
}

func decodePage(payload []byte, b controlplane.ChannelBinding, cfg BindingConfig, chat string, from, to time.Time) ([]channels.InboundEnvelope, string, bool, int, error) {
	batch, next, more, count, err := decodePageBatch(payload, b, cfg, chat, from, to)
	if err == nil && len(batch.Rejected) > 0 {
		err = fmt.Errorf("%w: rejected message", ErrSourceContract)
	}
	return batch.Messages, next, more, count, err
}

func decodePageBatch(payload []byte, b controlplane.ChannelBinding, cfg BindingConfig, chat string, from, to time.Time) (WindowBatch, string, bool, int, error) {
	var page struct {
		ErrorCode *int              `json:"errcode"`
		Count     *int              `json:"messages_count"`
		HasMore   *bool             `json:"has_more"`
		Next      string            `json:"next_cursor"`
		Messages  []json.RawMessage `json:"messages"`
	}
	if json.Unmarshal(payload, &page) != nil || page.ErrorCode == nil || *page.ErrorCode != 0 || page.Count == nil || *page.Count != len(page.Messages) || page.HasMore == nil {
		return WindowBatch{}, "", false, 0, fmt.Errorf("%w: invalid message page", ErrSourceContract)
	}
	result := []channels.InboundEnvelope{}
	rejected := []RejectedMessage{}
	reject := func(raw json.RawMessage, reason string) {
		var fields map[string]json.RawMessage
		canonical := raw
		prefix := "mcp_reject1_"
		if json.Unmarshal(raw, &fields) == nil && fields != nil {
			delete(fields, "user_name")
			delete(fields, "extra_identity_context")
			if reason == "unsupported_type" {
				// Observed responses rotate image.media_id on repeated reads;
				// it cannot serve as a stable source message ID.
				// Unsupported-media rejections aggregate by verified envelope:
				// same sender/second/type can coalesce multiple attachments.
				// Version this narrower identity; never rewrite old audit facts.
				fields = map[string]json.RawMessage{
					"userid": fields["userid"], "send_time": fields["send_time"], "msg_type": fields["msg_type"],
				}
				prefix = "mcp_reject2_"
			}
			canonical, _ = json.Marshal(fields)
		}
		identity, _ := json.Marshal([]string{b.TenantID, b.ID, chat, string(canonical)})
		rejected = append(rejected, RejectedMessage{Fingerprint: prefix + endpointHash(string(identity)), Reason: reason, WindowFrom: from, WindowTo: to})
	}
	for _, raw := range page.Messages {
		var header struct {
			UserID string `json:"userid"`
		}
		if json.Unmarshal(raw, &header) != nil || header.UserID == "" {
			reject(raw, "invalid_identity")
			continue
		}
		if !cfg.AllowsUser(chat, header.UserID) {
			continue
		}
		var m struct {
			UserID string `json:"userid"`
			Type   string `json:"msg_type"`
			Time   string `json:"send_time"`
			Text   struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if json.Unmarshal(raw, &m) != nil {
			reject(raw, "invalid_message")
			continue
		}
		// Human-ID allowlist is mandatory. No reliance on names, model output,
		// unverified is_bot fields or an assumption that bots are never returned.
		if !cfg.AllowsUser(chat, m.UserID) {
			continue
		}
		stamp, err := time.ParseInLocation("2006-01-02 15:04:05", m.Time, cfg.Location())
		if err != nil || m.Type == "" {
			reject(raw, "invalid_timestamp_or_type")
			continue
		}
		if stamp.Before(from) || !stamp.Before(to) {
			continue
		}
		if !cfg.AllowsMessage(chat, m.UserID, stamp) {
			continue
		}
		if m.Type == "text" && m.Text.Content == "" {
			reject(raw, "invalid_message")
			continue
		}
		// Unknown individual records become durable rejection metadata. The
		// receiver must persist it before advancing, without downloading media.
		if m.Type != "text" || len(m.Text.Content) > 32768 {
			reason := "unsupported_type"
			if m.Type == "text" {
				reason = "oversized_text"
			}
			reject(raw, reason)
			continue
		}
		body := strings.TrimSpace(m.Text.Content)
		if connectMarkerPattern.MatchString(strings.TrimSpace(strings.TrimPrefix(body, cfg.MentionPrefix))) {
			continue
		}
		if cfg.SetupMarker != "" && strings.HasSuffix(body, cfg.SetupMarker) {
			continue
		}
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
	return WindowBatch{Messages: result, Rejected: rejected}, page.Next, *page.HasMore, len(page.Messages), nil
}
