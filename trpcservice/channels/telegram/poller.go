package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	DefaultBaseURL        = "https://api.telegram.org"
	DefaultPollTimeoutSec = 20
	defaultMaxFileBytes   = int64(16 << 20)
)

var (
	ErrPollerTokenRequired = errors.New("telegram poller: bot token is required")
)

// MessageHandler keeps the package surface readable while sharing the platform
// receiver contract.
type MessageHandler = channels.InboundHandler

// OffsetStore persists the next Telegram update ID that may be consumed.
// Implementations must keep the stored value monotonic.
type OffsetStore interface {
	Load(context.Context) (int64, error)
	Save(context.Context, int64) error
}

// Poller continuously fetches updates from the Telegram Bot API via getUpdates long polling.
type Poller struct {
	botToken       string
	baseURL        string
	client         *http.Client
	timeoutSeconds int
	onReady        func()
	onError        func(error)
	maxFileBytes   int64
	offsetStore    OffsetStore

	mu          sync.RWMutex
	offset      int64
	botUsername string
}

// PollerConfig configures one Telegram Bot API long-polling connection.
type PollerConfig struct {
	BotToken     string
	BaseURL      string
	HTTPClient   *http.Client
	OnReady      func()
	OnError      func(error)
	MaxFileBytes int64
	OffsetStore  OffsetStore
}

// NewPoller constructs a Telegram long poller.
func NewPoller(config PollerConfig) (*Poller, error) {
	if strings.TrimSpace(config.BotToken) == "" {
		return nil, ErrPollerTokenRequired
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if config.MaxFileBytes <= 0 {
		config.MaxFileBytes = defaultMaxFileBytes
	}
	return &Poller{
		botToken:       strings.TrimSpace(config.BotToken),
		baseURL:        strings.TrimRight(config.BaseURL, "/"),
		client:         config.HTTPClient,
		timeoutSeconds: DefaultPollTimeoutSec,
		onReady:        config.OnReady,
		onError:        config.OnError,
		maxFileBytes:   config.MaxFileBytes,
		offsetStore:    config.OffsetStore,
	}, nil
}

// Offset returns the current update offset.
func (p *Poller) Offset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.offset
}

// SetOffset sets the update offset.
func (p *Poller) SetOffset(offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.offset = offset
}

type getUpdatesResponse struct {
	OK          bool     `json:"ok"`
	Description string   `json:"description,omitempty"`
	ErrorCode   int      `json:"error_code,omitempty"`
	Result      []update `json:"result,omitempty"`
	Parameters  struct {
		RetryAfter int `json:"retry_after,omitempty"`
	} `json:"parameters,omitempty"`
}

// PollOnce performs a single long-polling request against getUpdates.
// It returns the number of processed messages and any error encountered.
func (p *Poller) PollOnce(ctx context.Context, handler MessageHandler) (int, error) {
	p.mu.RLock()
	currentOffset := p.offset
	p.mu.RUnlock()

	endpoint := fmt.Sprintf("%s/bot%s/getUpdates?timeout=%d&allowed_updates=[\"message\",\"callback_query\"]",
		p.baseURL,
		url.PathEscape(p.botToken),
		p.timeoutSeconds,
	)
	if currentOffset > 0 {
		endpoint += fmt.Sprintf("&offset=%d", currentOffset)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("create getUpdates request: %w", err)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		var rateResp getUpdatesResponse
		_ = json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&rateResp)
		retryAfter := time.Duration(rateResp.Parameters.RetryAfter) * time.Second
		if retryAfter <= 0 {
			retryAfter = 2 * time.Second
		}
		slog.Warn("telegram poller: rate limited, backing off", "retry_after", retryAfter)
		select {
		case <-time.After(retryAfter):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		return 0, nil
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("telegram getUpdates HTTP status %d", resp.StatusCode)
	}

	var envelope getUpdatesResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&envelope); err != nil {
		return 0, fmt.Errorf("decode getUpdates response: %w", err)
	}

	if !envelope.OK {
		return 0, fmt.Errorf("telegram getUpdates failed: %s (code %d)", envelope.Description, envelope.ErrorCode)
	}

	processed := 0

	for _, upd := range envelope.Result {
		if currentOffset > 0 && upd.ID < currentOffset {
			continue
		}
		if upd.CallbackQuery != nil {
			if err := p.handleCallbackQuery(ctx, upd, handler); err != nil {
				return processed, err
			}
			processed++
			if err := p.advanceOffset(ctx, upd.ID+1); err != nil {
				return processed, err
			}
			continue
		}
		if upd.Message == nil {
			if err := p.advanceOffset(ctx, upd.ID+1); err != nil {
				return processed, err
			}
			continue
		}
		if upd.ID <= 0 || upd.Message.ID <= 0 || upd.Message.Chat.ID == 0 || upd.Message.From.ID <= 0 {
			if err := p.advanceOffset(ctx, upd.ID+1); err != nil {
				return processed, err
			}
			continue
		}
		text := strings.TrimSpace(upd.Message.Text)
		if text == "" {
			text = strings.TrimSpace(upd.Message.Caption)
		}
		rawText := text
		scope := telegramConversationScope(upd.Message.Chat.Type)
		p.mu.RLock()
		botUsername := p.botUsername
		p.mu.RUnlock()
		if scope == channels.ConversationGroup {
			var triggered bool
			text, triggered = normalizeTelegramGroupText(text, botUsername)
			if !triggered {
				if err := p.advanceOffset(ctx, upd.ID+1); err != nil {
					return processed, err
				}
				continue
			}
		}
		receivedFiles, err := p.downloadMessageFiles(ctx, upd.Message)
		if err != nil {
			p.reportError(err)
			return processed, fmt.Errorf("telegram update %d attachment download: %w", upd.ID, err)
		}
		if text == "" && len(receivedFiles) == 0 {
			if err := p.advanceOffset(ctx, upd.ID+1); err != nil {
				return processed, err
			}
			continue
		}

		inbound := channels.InboundMessage{
			MessageID:         strconv.FormatInt(upd.ID, 10),
			ProviderRequestID: strconv.FormatInt(upd.ID, 10),
			Channel:           channels.Telegram,
			ConversationID:    strconv.FormatInt(upd.Message.Chat.ID, 10),
			SenderID:          strconv.FormatInt(upd.Message.From.ID, 10),
			ConversationScope: scope,
			Text:              text,
			ReceivedFiles:     receivedFiles,
			ReceivedAt:        time.Now().UTC(),
		}
		if strings.HasPrefix(strings.TrimSpace(rawText), "/") {
			inbound.TriggerType = channels.TriggerCommand
		} else if scope == channels.ConversationGroup {
			inbound.TriggerType = channels.TriggerMention
		} else {
			inbound.TriggerType = channels.TriggerDirect
		}
		if upd.Message.Date > 0 {
			inbound.ReceivedAt = time.Unix(upd.Message.Date, 0).UTC()
		}

		if handler != nil {
			if err := handler(ctx, inbound); err != nil {
				return processed, fmt.Errorf("telegram update %d handler: %w", upd.ID, err)
			}
		}
		processed++
		if err := p.advanceOffset(ctx, upd.ID+1); err != nil {
			return processed, err
		}
	}

	return processed, nil
}

func (p *Poller) handleCallbackQuery(ctx context.Context, upd update, handler MessageHandler) error {
	callback := upd.CallbackQuery
	if callback == nil || strings.TrimSpace(callback.ID) == "" || callback.From.ID <= 0 || callback.Message == nil || callback.Message.Chat.ID == 0 {
		return nil
	}
	inbound := channels.InboundMessage{
		MessageID:         fmt.Sprintf("callback:%d:%s", upd.ID, callback.ID),
		ProviderRequestID: strconv.FormatInt(upd.ID, 10),
		Channel:           channels.Telegram,
		ConversationID:    strconv.FormatInt(callback.Message.Chat.ID, 10),
		SenderID:          strconv.FormatInt(callback.From.ID, 10),
		ConversationScope: telegramConversationScope(callback.Message.Chat.Type),
		TriggerType:       channels.TriggerAction,
		ReceivedAt:        time.Now().UTC(),
		Action: &channels.InboundAction{
			ActionID:        strings.TrimSpace(callback.Data),
			OriginMessageID: strconv.FormatInt(callback.Message.ID, 10),
			CallbackID:      callback.ID,
		},
	}
	if handler != nil {
		if err := handler(ctx, inbound); err != nil {
			return fmt.Errorf("telegram callback %s handler: %w", callback.ID, err)
		}
	}
	// Telegram keeps a spinner visible until answerCallbackQuery is called. A
	// successful platform handler means the action has been accepted.
	sender, err := NewSender(p.botToken, p.baseURL, p.client)
	if err == nil {
		if answerErr := sender.AnswerCallback(ctx, callback.ID, "已处理"); answerErr != nil {
			// 业务处理已成功，仅回执失败，不应中断轮询；记录以便观测。
			slog.Warn("telegram answer callback query failed",
				"callback_id", callback.ID, "error", answerErr)
		}
	}
	return nil
}

func (p *Poller) commitOffset(offset int64) {
	if offset <= 0 {
		return
	}
	p.mu.Lock()
	if offset > p.offset {
		p.offset = offset
	}
	p.mu.Unlock()
}

func (p *Poller) advanceOffset(ctx context.Context, offset int64) error {
	if offset <= 0 {
		return nil
	}
	if p.offsetStore != nil {
		if err := p.offsetStore.Save(ctx, offset); err != nil {
			return fmt.Errorf("persist telegram update offset %d: %w", offset, err)
		}
	}
	p.commitOffset(offset)
	return nil
}

func (p *Poller) loadPersistedOffset(ctx context.Context) error {
	if p.offsetStore == nil {
		return nil
	}
	offset, err := p.offsetStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("load telegram update offset: %w", err)
	}
	if offset < 0 {
		return fmt.Errorf("load telegram update offset: invalid negative offset %d", offset)
	}
	p.commitOffset(offset)
	return nil
}

func (p *Poller) downloadMessageFiles(ctx context.Context, message *message) ([]channels.ReceivedFile, error) {
	if message == nil {
		return nil, nil
	}
	candidates := make([]telegramFile, 0, 5)
	if message.Document != nil {
		candidates = append(candidates, telegramFile{
			FileID: message.Document.FileID, Name: message.Document.FileName,
			MimeType: message.Document.MimeType, Size: message.Document.FileSize,
		})
	}
	if len(message.Photo) > 0 {
		photo := message.Photo[0]
		for _, candidate := range message.Photo[1:] {
			if candidate.FileSize > photo.FileSize || (candidate.FileSize == photo.FileSize && candidate.Width*candidate.Height > photo.Width*photo.Height) {
				photo = candidate
			}
		}
		candidates = append(candidates, telegramFile{FileID: photo.FileID, Name: "photo.jpg", MimeType: "image/jpeg", Size: photo.FileSize})
	}
	if message.Video != nil {
		candidates = append(candidates, telegramFile{FileID: message.Video.FileID, Name: fallbackFilename(message.Video.FileName, "video.mp4"), MimeType: message.Video.MimeType, Size: message.Video.FileSize})
	}
	if message.Audio != nil {
		candidates = append(candidates, telegramFile{FileID: message.Audio.FileID, Name: fallbackFilename(message.Audio.FileName, "audio.mp3"), MimeType: message.Audio.MimeType, Size: message.Audio.FileSize})
	}
	if message.Voice != nil {
		candidates = append(candidates, telegramFile{FileID: message.Voice.FileID, Name: "voice.ogg", MimeType: message.Voice.MimeType, Size: message.Voice.FileSize})
	}
	files := make([]channels.ReceivedFile, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.FileID) == "" {
			continue
		}
		if candidate.Size > p.maxFileBytes {
			return nil, fmt.Errorf("telegram file %q exceeds %d bytes", candidate.Name, p.maxFileBytes)
		}
		data, err := p.downloadFile(ctx, candidate.FileID)
		if err != nil {
			return nil, fmt.Errorf("download telegram file %q: %w", candidate.Name, err)
		}
		files = append(files, channels.ReceivedFile{Name: fallbackFilename(candidate.Name, "attachment"), MimeType: candidate.MimeType, Data: data})
	}
	return files, nil
}

func (p *Poller) downloadFile(ctx context.Context, fileID string) ([]byte, error) {
	metadataURL := fmt.Sprintf("%s/bot%s/getFile?file_id=%s", p.baseURL, url.PathEscape(p.botToken), url.QueryEscape(fileID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("getFile HTTP status %d", resp.StatusCode)
	}
	var result struct {
		OK     bool `json:"ok"`
		Result struct {
			FilePath string `json:"file_path"`
		} `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode getFile response: %w", err)
	}
	cleanPath := strings.TrimPrefix(path.Clean("/"+result.Result.FilePath), "/")
	if !result.OK || cleanPath == "" || cleanPath == "." {
		return nil, errors.New("getFile response did not contain a file path")
	}
	downloadURL := fmt.Sprintf("%s/file/bot%s/%s", p.baseURL, url.PathEscape(p.botToken), cleanPath)
	downloadReq, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, err
	}
	downloadResp, err := p.client.Do(downloadReq)
	if err != nil {
		return nil, err
	}
	defer downloadResp.Body.Close()
	if downloadResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("file download HTTP status %d", downloadResp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(downloadResp.Body, p.maxFileBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > p.maxFileBytes {
		return nil, fmt.Errorf("telegram file exceeds %d bytes", p.maxFileBytes)
	}
	return data, nil
}

func fallbackFilename(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}

type telegramFile struct {
	FileID, Name, MimeType string
	Size                   int64
}

// Run continuously polls getUpdates until ctx is cancelled.
func (p *Poller) Run(ctx context.Context, handler MessageHandler) error {
	if err := p.loadBotIdentity(ctx); err != nil {
		p.reportError(err)
		return err
	}
	if err := p.loadPersistedOffset(ctx); err != nil {
		p.reportError(err)
		return err
	}
	if sender, err := NewSender(p.botToken, p.baseURL, p.client); err == nil {
		if err := sender.SetCommands(ctx, DefaultCommands()); err != nil {
			p.reportError(err)
		}
		if err := sender.SetMenuButtonCommands(ctx); err != nil {
			p.reportError(err)
		}
	}
	p.reportReady()
	attempt := 0
	for ctx.Err() == nil {
		_, err := p.PollOnce(ctx, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			p.reportError(err)
			attempt++
			backoff := time.Duration(1<<min(attempt, 5)) * 100 * time.Millisecond
			slog.Warn("telegram poller: poll error, backing off", "error", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		attempt = 0
		p.reportReady()
	}
	return ctx.Err()
}

func (p *Poller) loadBotIdentity(ctx context.Context) error {
	endpoint := fmt.Sprintf("%s/bot%s/getMe", p.baseURL, url.PathEscape(p.botToken))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("create getMe request: %w", err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram getMe request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram getMe HTTP status %d", resp.StatusCode)
	}
	var result struct {
		OK          bool   `json:"ok"`
		Description string `json:"description,omitempty"`
		Result      struct {
			Username string `json:"username"`
		} `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&result); err != nil {
		return fmt.Errorf("decode telegram getMe response: %w", err)
	}
	if !result.OK || strings.TrimSpace(result.Result.Username) == "" {
		return fmt.Errorf("telegram getMe failed: %s", result.Description)
	}
	p.mu.Lock()
	p.botUsername = strings.TrimPrefix(strings.TrimSpace(result.Result.Username), "@")
	p.mu.Unlock()
	return nil
}

func (p *Poller) reportReady() {
	if p.onReady != nil {
		p.onReady()
	}
}

func (p *Poller) reportError(err error) {
	if p.onError != nil && err != nil {
		p.onError(err)
	}
}

func normalizeTelegramGroupText(text, botUsername string) (string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", false
	}
	botUsername = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(botUsername), "@"))
	if botUsername == "" {
		// PollOnce is intentionally usable in isolation in tests. Production Run
		// resolves getMe before polling, so a real connector always has identity.
		return text, true
	}
	target := "@" + botUsername
	triggered := strings.HasPrefix(text, "/")
	fields := strings.Fields(text)
	cleaned := make([]string, 0, len(fields))
	for _, field := range fields {
		lower := strings.ToLower(field)
		trimmed := strings.Trim(lower, ".,!?;:()[]{}<>，。！？；：（）【】")
		if trimmed == target {
			triggered = true
			continue
		}
		if strings.HasSuffix(trimmed, target) {
			triggered = true
			// lower preserves the byte offsets of the original UTF-8 field for
			// ASCII bot usernames. Locate the mention itself instead of trimming
			// len(target) bytes from the raw field: trailing full-width punctuation
			// such as '！' occupies multiple bytes and would otherwise cut through
			// the username.
			if mention := strings.LastIndex(lower, target); mention >= 0 {
				field = strings.TrimSpace(field[:mention])
			}
		}
		if field != "" {
			cleaned = append(cleaned, field)
		}
	}
	return strings.TrimSpace(strings.Join(cleaned, " ")), triggered
}

type update struct {
	ID            int64          `json:"update_id"`
	Message       *message       `json:"message"`
	CallbackQuery *callbackQuery `json:"callback_query,omitempty"`
}

type callbackQuery struct {
	ID      string   `json:"id"`
	From    user     `json:"from"`
	Message *message `json:"message,omitempty"`
	Data    string   `json:"data,omitempty"`
}

type message struct {
	ID       int64       `json:"message_id"`
	Date     int64       `json:"date"`
	Chat     chat        `json:"chat"`
	From     user        `json:"from"`
	Text     string      `json:"text"`
	Caption  string      `json:"caption"`
	Document *document   `json:"document,omitempty"`
	Photo    []photoSize `json:"photo,omitempty"`
	Video    *mediaFile  `json:"video,omitempty"`
	Audio    *mediaFile  `json:"audio,omitempty"`
	Voice    *mediaFile  `json:"voice,omitempty"`
}

type document struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
	FileSize int64  `json:"file_size"`
}

type photoSize struct {
	FileID   string `json:"file_id"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
	FileSize int64  `json:"file_size"`
}

type mediaFile struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
	FileSize int64  `json:"file_size,omitempty"`
}

type chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type user struct {
	ID int64 `json:"id"`
}

func telegramConversationScope(chatType string) channels.ConversationScope {
	switch strings.ToLower(strings.TrimSpace(chatType)) {
	case "group", "supergroup", "channel":
		return channels.ConversationGroup
	default:
		return channels.ConversationDirect
	}
}
