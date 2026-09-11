package wecombot

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	wecomUploadChunkBytes       = 512 << 10
	wecomProgressUpdateInterval = time.Second
	wecomInitialProgressText    = "正在处理…"
	wecomChunkAttempts          = 3
	wecomChunkRetryBase         = 500 * time.Millisecond
)

type requester interface {
	Request(context.Context, string, string, any) error
	RequestFrame(context.Context, string, string, any) (WireFrame, error)
}

// Sender implements the proactive and callback reply capabilities of the
// WeCom Smart Bot WebSocket protocol. The Client owns connection/reconnect;
// Sender only translates the channel-neutral payload onto official commands.
type Sender struct {
	client                 requester
	maxFileBytes           int64
	progressUpdateInterval time.Duration
	streamsMu              sync.Mutex
	streams                map[string]*streamState
}

type streamState struct {
	mu          sync.Mutex
	lastContent string
	lastSentAt  time.Time
	cardTaskID  string
}

func NewSender(client *Client) (*Sender, error) {
	if client == nil {
		return nil, errors.New("wecombot: client cannot be nil")
	}
	maxFileBytes := client.config.MaxFileBytes
	if maxFileBytes <= 0 {
		maxFileBytes = 16 << 20
	}
	return newSender(client, maxFileBytes), nil
}

func newSender(client requester, maxFileBytes int64) *Sender {
	return &Sender{
		client: client, maxFileBytes: maxFileBytes,
		progressUpdateInterval: wecomProgressUpdateInterval,
		streams:                make(map[string]*streamState),
	}
}

func (s *Sender) Send(ctx context.Context, target channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	if err := validateWeComTarget(target); err != nil {
		return channels.SendReceipt{}, err
	}
	text := compactWeComMarkdown(message.Text)
	if strings.TrimSpace(text) == "" && message.Card == nil && len(message.Files) == 0 {
		return channels.SendReceipt{}, errors.New("wecombot: message content is required")
	}

	// If a progress stream was started from this inbound callback, the final
	// durable reply can finish the same provider stream while the callback token
	// is still valid. Otherwise the Outbox falls back to proactive send below.
	if message.UpdateMessageID != "" && target.ProviderReplyToken != "" && len(message.Files) == 0 && message.Card == nil {
		state := s.stream(message.UpdateMessageID)
		state.mu.Lock()
		body := streamBody(message.UpdateMessageID, text, true)
		if state.cardTaskID != "" {
			body = streamWithTemplateCardBody(message.UpdateMessageID, text, true, nil)
		}
		err := s.client.Request(ctx, "aibot_respond_msg", target.ProviderReplyToken, body)
		state.mu.Unlock()
		s.forgetStream(message.UpdateMessageID, state)
		if err == nil {
			return channels.SendReceipt{ExternalMessageID: message.UpdateMessageID}, nil
		}
		// Callback tokens are short lived. Final delivery is durable, so an
		// expired stream token must fall through to proactive send rather than
		// leaving the Outbox permanently retrying an impossible stream update.
	}

	var receipt channels.SendReceipt
	if message.Card != nil {
		body, err := activeCardBody(target.ConversationID, *message.Card)
		if err != nil {
			return channels.SendReceipt{}, err
		}
		reqID := uuid.NewString()
		if err := s.client.Request(ctx, "aibot_send_msg", reqID, body); err != nil {
			return channels.SendReceipt{}, fmt.Errorf("wecombot: send template card: %w", err)
		}
		receipt.ExternalMessageID = reqID
	} else if strings.TrimSpace(text) != "" {
		reqID := uuid.NewString()
		body := map[string]any{
			"chatid":   target.ConversationID,
			"msgtype":  "markdown",
			"markdown": map[string]string{"content": text},
		}
		if err := s.client.Request(ctx, "aibot_send_msg", reqID, body); err != nil {
			return channels.SendReceipt{}, fmt.Errorf("wecombot: send message: %w", err)
		}
		receipt.ExternalMessageID = reqID
	}

	for _, file := range message.Files {
		mediaType, mediaID, err := s.uploadMedia(ctx, file)
		if err != nil {
			return channels.SendReceipt{}, err
		}
		reqID := uuid.NewString()
		body := map[string]any{
			"chatid":  target.ConversationID,
			"msgtype": mediaType,
			mediaType: map[string]string{"media_id": mediaID},
		}
		if err := s.client.Request(ctx, "aibot_send_msg", reqID, body); err != nil {
			return channels.SendReceipt{}, fmt.Errorf("wecombot: send %s: %w", mediaType, err)
		}
		if receipt.ExternalMessageID == "" {
			receipt.ExternalMessageID = reqID
		}
	}
	return receipt, nil
}

func (s *Sender) StartProgress(ctx context.Context, target channels.ReplyTarget) (channels.SendReceipt, error) {
	if err := validateWeComTarget(target); err != nil {
		return channels.SendReceipt{}, err
	}
	if strings.TrimSpace(target.ProviderReplyToken) == "" {
		return channels.SendReceipt{}, errors.New("wecombot: progress requires inbound reply token")
	}
	streamID := strings.ReplaceAll(uuid.NewString(), "-", "")
	state := s.stream(streamID)
	state.mu.Lock()
	err := s.client.Request(ctx, "aibot_respond_msg", target.ProviderReplyToken, streamBody(streamID, wecomInitialProgressText, false))
	if err == nil {
		state.lastContent = wecomInitialProgressText
	}
	state.mu.Unlock()
	if err != nil {
		s.forgetStream(streamID, state)
		return channels.SendReceipt{}, fmt.Errorf("wecombot: start stream: %w", err)
	}
	return channels.SendReceipt{ExternalMessageID: streamID}, nil
}

func (s *Sender) UpdateProgress(ctx context.Context, target channels.ReplyTarget, streamID, text string) error {
	if err := validateWeComTarget(target); err != nil {
		return err
	}
	if strings.TrimSpace(target.ProviderReplyToken) == "" || strings.TrimSpace(streamID) == "" {
		return errors.New("wecombot: progress update requires reply token and stream id")
	}
	text = compactWeComMarkdown(text)
	if text == "" {
		return nil
	}
	state := s.stream(streamID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if text == state.lastContent {
		return nil
	}
	now := time.Now()
	if !state.lastSentAt.IsZero() && s.progressUpdateInterval > 0 && now.Sub(state.lastSentAt) < s.progressUpdateInterval {
		return nil
	}
	body := streamBody(streamID, text, false)
	if state.cardTaskID != "" {
		body = streamWithTemplateCardBody(streamID, text, false, nil)
	}
	if err := s.client.Request(ctx, "aibot_respond_msg", target.ProviderReplyToken, body); err != nil {
		return fmt.Errorf("wecombot: update stream: %w", err)
	}
	state.lastContent = text
	state.lastSentAt = now
	return nil
}

func (s *Sender) UpdateProgressCard(ctx context.Context, target channels.ReplyTarget, streamID string, card channels.InteractiveCard) error {
	if err := validateWeComTarget(target); err != nil {
		return err
	}
	if strings.TrimSpace(target.ProviderReplyToken) == "" || strings.TrimSpace(streamID) == "" {
		return errors.New("wecombot: progress card requires reply token and stream id")
	}
	taskID := approvalTaskID(card)
	if taskID == "" {
		taskID = strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	template, err := templateCard(card, taskID)
	if err != nil {
		return err
	}
	state := s.stream(streamID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cardTaskID != "" {
		if state.cardTaskID == taskID {
			return nil
		}
		return errors.New("wecombot: progress stream already contains a template card")
	}
	if err := s.client.Request(ctx, "aibot_respond_msg", target.ProviderReplyToken, streamWithTemplateCardBody(streamID, state.lastContent, false, template)); err != nil {
		return fmt.Errorf("wecombot: update stream with template card: %w", err)
	}
	state.cardTaskID = taskID
	return nil
}

func (s *Sender) UpdateCard(ctx context.Context, target channels.ReplyTarget, taskID string, card channels.InteractiveCard) error {
	if err := validateWeComTarget(target); err != nil {
		return err
	}
	if strings.TrimSpace(target.ProviderReplyToken) == "" || strings.TrimSpace(taskID) == "" {
		return errors.New("wecombot: card update requires callback token and task id")
	}
	template, err := templateCard(card, taskID)
	if err != nil {
		return err
	}
	body := map[string]any{"response_type": "update_template_card", "template_card": template}
	if err := s.client.Request(ctx, "aibot_respond_update_msg", target.ProviderReplyToken, body); err != nil {
		return fmt.Errorf("wecombot: update template card: %w", err)
	}
	return nil
}

func streamBody(streamID, text string, finish bool) map[string]any {
	stream := map[string]any{"id": streamID, "finish": finish}
	if text != "" {
		stream["content"] = text
	}
	return map[string]any{
		"msgtype": "stream",
		"stream":  stream,
	}
}

func streamWithTemplateCardBody(streamID, text string, finish bool, template map[string]any) map[string]any {
	stream := map[string]any{"id": streamID, "finish": finish}
	if text != "" {
		stream["content"] = text
	}
	body := map[string]any{
		"msgtype": "stream_with_template_card",
		"stream":  stream,
	}
	if template != nil {
		body["template_card"] = template
	}
	return body
}

func compactWeComMarkdown(text string) string {
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	result := make([]string, 0, len(lines))
	inFence := false
	lastBlank := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inFence = !inFence
			result = append(result, line)
			lastBlank = false
			continue
		}
		if !inFence {
			if trimmed == "" {
				if lastBlank {
					continue
				}
				result = append(result, "")
				lastBlank = true
				continue
			}
			lastBlank = false
			for level := 6; level >= 1; level-- {
				prefix := strings.Repeat("#", level) + " "
				if strings.HasPrefix(trimmed, prefix) {
					trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
					line = "**" + trimmed + "**"
					break
				}
			}
		}
		result = append(result, line)
	}
	return strings.TrimSpace(strings.Join(result, "\n"))
}

func (s *Sender) stream(streamID string) *streamState {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	state := s.streams[streamID]
	if state == nil {
		state = &streamState{}
		s.streams[streamID] = state
	}
	return state
}

func (s *Sender) forgetStream(streamID string, state *streamState) {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	if s.streams[streamID] == state {
		delete(s.streams, streamID)
	}
}

func activeCardBody(chatID string, card channels.InteractiveCard) (map[string]any, error) {
	taskID := approvalTaskID(card)
	if taskID == "" {
		taskID = strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	template, err := templateCard(card, taskID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"chatid": chatID, "msgtype": "template_card", "template_card": template}, nil
}

func approvalTaskID(card channels.InteractiveCard) string {
	for _, action := range card.Actions {
		parts := strings.Split(strings.TrimSpace(action.ActionID), ":")
		if len(parts) == 3 && parts[0] == "approval" && strings.TrimSpace(parts[2]) != "" {
			return strings.TrimSpace(parts[2])
		}
	}
	return ""
}

func templateCard(card channels.InteractiveCard, taskID string) (map[string]any, error) {
	body := strings.TrimSpace(card.Body)
	if body == "" {
		return nil, errors.New("wecombot: card body is required")
	}
	if card.State == "approved" || card.State == "rejected" {
		title := strings.TrimSpace(card.Title)
		if title == "" {
			title = "操作已处理"
		}
		return map[string]any{
			"card_type":      "text_notice",
			"main_title":     map[string]string{"title": title},
			"sub_title_text": body,
			"card_action":    map[string]any{"type": 0},
			"task_id":        taskID,
		}, nil
	}
	buttons := make([]map[string]any, 0, len(card.Actions))
	jumps := make([]map[string]any, 0, len(card.Actions))
	for _, action := range card.Actions {
		label := strings.TrimSpace(action.Label)
		url := strings.TrimSpace(action.URL)
		if url != "" {
			if label == "" {
				continue
			}
			jumps = append(jumps, map[string]any{"type": 1, "title": label, "url": url})
			continue
		}
		if strings.TrimSpace(action.ActionID) == "" || label == "" {
			continue
		}
		style := 1
		switch action.Style {
		case "primary":
			style = 2
		case "danger":
			style = 4
		}
		buttons = append(buttons, map[string]any{"text": label, "style": style, "key": action.ActionID})
	}
	if len(buttons) > 0 && len(jumps) > 0 {
		return nil, errors.New("wecombot: callback buttons and URL actions cannot share one template card")
	}
	if len(jumps) > 3 {
		return nil, errors.New("wecombot: template card supports at most 3 URL actions")
	}

	// Non-interactive cards and URL-only cards use the native text_notice
	// shape. URL actions are jump_list entries; the first URL is also the
	// required whole-card action in providers that enforce that field.
	if len(buttons) == 0 {
		template := map[string]any{
			"card_type":      "text_notice",
			"sub_title_text": body,
			"card_action":    map[string]any{"type": 0},
			"task_id":        taskID,
		}
		if title := strings.TrimSpace(card.Title); title != "" {
			template["main_title"] = map[string]string{"title": title}
		}
		if len(jumps) > 0 {
			template["jump_list"] = jumps
			template["card_action"] = map[string]any{"type": 1, "url": jumps[0]["url"]}
		}
		return template, nil
	}

	template := map[string]any{
		"card_type":      "button_interaction",
		"sub_title_text": body,
		"task_id":        taskID,
	}
	if title := strings.TrimSpace(card.Title); title != "" {
		template["main_title"] = map[string]string{"title": title}
	}
	if len(buttons) > 0 {
		template["button_list"] = buttons
	}
	return template, nil
}

func (s *Sender) uploadMedia(ctx context.Context, file channels.OutboundFile) (string, string, error) {
	path := strings.TrimSpace(file.Path)
	if path == "" {
		return "", "", errors.New("wecombot: outbound file path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("wecombot: read outbound file: %w", err)
	}
	if len(data) == 0 {
		return "", "", errors.New("wecombot: outbound file is empty")
	}
	if int64(len(data)) > s.maxFileBytes {
		return "", "", fmt.Errorf("wecombot: outbound file exceeds %d bytes", s.maxFileBytes)
	}
	totalChunks := (len(data) + wecomUploadChunkBytes - 1) / wecomUploadChunkBytes
	if totalChunks > 100 {
		return "", "", errors.New("wecombot: outbound file exceeds provider chunk limit")
	}
	filename := strings.TrimSpace(file.Name)
	if filename == "" {
		filename = filepath.Base(path)
	}
	mediaType := wecomMediaType(filename)
	digest := md5.Sum(data)
	initResponse, err := s.client.RequestFrame(ctx, "aibot_upload_media_init", uuid.NewString(), map[string]any{
		"type": mediaType, "filename": filename, "total_size": len(data), "total_chunks": totalChunks,
		"md5": hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return "", "", fmt.Errorf("wecombot: initialize media upload: %w", err)
	}
	var initBody struct {
		UploadID string `json:"upload_id"`
	}
	if json.Unmarshal(initResponse.Body, &initBody) != nil || strings.TrimSpace(initBody.UploadID) == "" {
		return "", "", errors.New("wecombot: media upload init returned no upload_id")
	}
	for index := 0; index < totalChunks; index++ {
		start := index * wecomUploadChunkBytes
		end := min(start+wecomUploadChunkBytes, len(data))
		if err := s.uploadMediaChunk(ctx, map[string]any{
			"upload_id": initBody.UploadID, "chunk_index": index, "base64_data": base64.StdEncoding.EncodeToString(data[start:end]),
		}); err != nil {
			return "", "", fmt.Errorf("wecombot: upload media chunk %d: %w", index, err)
		}
	}
	finishResponse, err := s.client.RequestFrame(ctx, "aibot_upload_media_finish", uuid.NewString(), map[string]string{"upload_id": initBody.UploadID})
	if err != nil {
		return "", "", fmt.Errorf("wecombot: finish media upload: %w", err)
	}
	var finishBody struct {
		Type    string `json:"type"`
		MediaID string `json:"media_id"`
	}
	if json.Unmarshal(finishResponse.Body, &finishBody) != nil || strings.TrimSpace(finishBody.MediaID) == "" {
		return "", "", errors.New("wecombot: media upload finish returned no media_id")
	}
	if strings.TrimSpace(finishBody.Type) != "" {
		mediaType = finishBody.Type
	}
	return mediaType, finishBody.MediaID, nil
}

func (s *Sender) uploadMediaChunk(ctx context.Context, body map[string]any) error {
	var lastErr error
	for attempt := 0; attempt < wecomChunkAttempts; attempt++ {
		if err := s.client.Request(ctx, "aibot_upload_media_chunk", uuid.NewString(), body); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == wecomChunkAttempts-1 || ctx.Err() != nil {
			break
		}
		delay := wecomChunkRetryBase * time.Duration(attempt+1)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func wecomMediaType(filename string) string {
	switch strings.ToLower(filepath.Ext(filename)) {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp":
		return "image"
	case ".mp3", ".amr", ".wav", ".ogg":
		return "voice"
	case ".mp4", ".mov", ".avi":
		return "video"
	default:
		return "file"
	}
}

func validateWeComTarget(target channels.ReplyTarget) error {
	if target.Channel != channels.WeCom || strings.TrimSpace(target.ConversationID) == "" {
		return errors.New("wecombot: invalid reply target")
	}
	return nil
}

var _ channels.Sender = (*Sender)(nil)
var _ channels.ProgressSender = (*Sender)(nil)
var _ channels.ProgressCardSender = (*Sender)(nil)
var _ channels.CardUpdater = (*Sender)(nil)
