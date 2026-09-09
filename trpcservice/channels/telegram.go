package channels

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type TelegramAdapter struct {
	bindingID string
	accountID string
	token     string
	serverURL string
	client    *http.Client
	bot       *bot.Bot
	botUser   *models.User

	mu      sync.RWMutex
	offset  int64
	started bool
	ready   bool
	cancel  context.CancelFunc
}

func NewTelegramAdapter(bindingID, accountID, token, serverURL string) (*TelegramAdapter, error) {
	if strings.TrimSpace(bindingID) == "" || strings.TrimSpace(accountID) == "" || strings.TrimSpace(token) == "" {
		return nil, errors.New("telegram binding, account and token are required")
	}
	if strings.TrimSpace(serverURL) == "" {
		serverURL = "https://api.telegram.org"
	}
	client := &http.Client{Timeout: 65 * time.Second}
	instance, err := bot.New(token, bot.WithServerURL(strings.TrimRight(serverURL, "/")), bot.WithHTTPClient(65*time.Second, client), bot.WithSkipGetMe())
	if err != nil {
		return nil, err
	}
	return &TelegramAdapter{bindingID: bindingID, accountID: accountID, token: token, serverURL: strings.TrimRight(serverURL, "/"), client: client, bot: instance}, nil
}

func (a *TelegramAdapter) Name() string { return a.bindingID }

func (a *TelegramAdapter) Ready(context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.bot == nil || a.botUser == nil || !a.started || !a.ready {
		return errors.New("telegram adapter is not ready")
	}
	return nil
}

func (a *TelegramAdapter) Start(ctx context.Context, sink IngressSink) error {
	if sink == nil {
		return errors.New("telegram ingress sink is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		cancel()
		return errors.New("telegram adapter is already running")
	}
	a.started, a.cancel = true, cancel
	a.mu.Unlock()
	defer func() {
		cancel()
		a.mu.Lock()
		a.started, a.ready, a.cancel = false, false, nil
		a.mu.Unlock()
	}()
	backoff := time.Second
	for {
		if runCtx.Err() != nil {
			return nil
		}
		if err := a.authenticate(runCtx); err != nil {
			a.markTelegramReady(false)
			if !waitTelegramRetry(runCtx, backoff) {
				return nil
			}
			backoff = nextTelegramBackoff(backoff)
			continue
		}
		updates, err := a.getUpdates(runCtx)
		if err != nil {
			if runCtx.Err() != nil {
				return nil
			}
			a.markTelegramReady(false)
			delay := backoff
			var retryErr *telegramRetryError
			if errors.As(err, &retryErr) && retryErr.after > delay {
				delay = retryErr.after
			}
			if !waitTelegramRetry(runCtx, delay) {
				return nil
			}
			backoff = nextTelegramBackoff(backoff)
			continue
		}
		a.markTelegramReady(true)
		backoff = time.Second
		retryBatch := false
		for _, update := range updates {
			if err := a.handleUpdate(runCtx, sink, update); err != nil {
				// Poison updates are terminally ignored so one malformed platform
				// payload cannot block the bot forever.
				if errors.Is(err, errTelegramUnsupported) || errors.Is(err, errTelegramPoison) || errors.Is(err, ErrIngressConflict) {
					a.advance(update.ID)
					continue
				}
				a.markTelegramReady(false)
				retryBatch = true
				break
			}
		}
		if retryBatch && !waitTelegramRetry(runCtx, backoff) {
			return nil
		}
	}
}

var (
	errTelegramUnsupported = errors.New("unsupported telegram update")
	errTelegramPoison      = errors.New("invalid telegram update")
)

type telegramRetryError struct {
	after time.Duration
}

func (e *telegramRetryError) Error() string { return "telegram API requested retry" }

func (a *TelegramAdapter) authenticate(ctx context.Context) error {
	a.mu.RLock()
	readyUser := a.botUser
	a.mu.RUnlock()
	if readyUser != nil {
		return nil
	}
	me, err := a.bot.GetMe(ctx)
	if err != nil {
		return errors.New("telegram authentication failed")
	}
	if strconv.FormatInt(me.ID, 10) != a.accountID {
		return errors.New("telegram bot identity does not match binding")
	}
	a.mu.Lock()
	a.botUser = me
	a.mu.Unlock()
	return nil
}

func (a *TelegramAdapter) getUpdates(ctx context.Context) ([]*models.Update, error) {
	a.mu.RLock()
	offset := a.offset
	a.mu.RUnlock()
	query := a.serverURL + "/bot" + a.token + "/getUpdates?timeout=50&limit=100"
	if offset > 0 {
		query += "&offset=" + strconv.FormatInt(offset, 10)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, query, nil)
	if err != nil {
		return nil, err
	}
	response, err := a.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusTooManyRequests {
		var failure struct {
			Parameters struct {
				RetryAfter int `json:"retry_after"`
			} `json:"parameters"`
		}
		_ = json.Unmarshal(data, &failure)
		after := time.Duration(failure.Parameters.RetryAfter) * time.Second
		return nil, &telegramRetryError{after: after}
	}
	if response.StatusCode >= 500 {
		return nil, fmt.Errorf("telegram polling status %d", response.StatusCode)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, errTelegramPoison
	}
	var envelope struct {
		OK        bool             `json:"ok"`
		Result    []*models.Update `json:"result"`
		ErrorCode int              `json:"error_code"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || !envelope.OK {
		return nil, fmt.Errorf("telegram polling response invalid")
	}
	return envelope.Result, nil
}

func (a *TelegramAdapter) handleUpdate(ctx context.Context, sink IngressSink, update *models.Update) error {
	ctx, span := telemetry.Start(ctx, "channel.ingress")
	defer span.End()
	if update == nil || update.Message == nil || update.Message.From == nil {
		return errTelegramUnsupported
	}
	current := update.Message
	conversationType := message.ConversationDirect
	conversationID := strconv.FormatInt(current.Chat.ID, 10)
	text := current.Text
	if current.Chat.Type != "private" {
		conversationType = message.ConversationGroup
		if !mentionsBot(current, a.botUser) {
			return errTelegramUnsupported
		}
		text = removeBotMention(text, current.Entities, a.botUser)
	}
	if strings.TrimSpace(text) == "" {
		return errTelegramUnsupported
	}
	_, err := sink.Accept(ctx, message.InboundMessage{
		Channel: "telegram", BindingID: a.bindingID, ExternalAccountID: a.accountID,
		PlatformMessageID: strconv.FormatInt(update.ID, 10), ActorUserID: strconv.FormatInt(current.From.ID, 10),
		ConversationID: conversationID, ConversationType: conversationType, Text: text,
		ReceivedAt: time.Unix(int64(current.Date), 0).UTC(),
	})
	if err != nil {
		return err
	}
	a.advance(update.ID)
	return nil
}

func (a *TelegramAdapter) advance(id int64) {
	a.mu.Lock()
	if id+1 > a.offset {
		a.offset = id + 1
	}
	a.mu.Unlock()
}

func (a *TelegramAdapter) Send(ctx context.Context, outbound message.OutboundMessage) error {
	if outbound.ConversationID == "" || strings.TrimSpace(outbound.Text) == "" {
		return errors.New("telegram outbound target is incomplete")
	}
	chatID := any(outbound.ConversationID)
	if parsed, err := strconv.ParseInt(outbound.ConversationID, 10, 64); err == nil {
		chatID = parsed
	}
	_, err := a.bot.SendMessage(ctx, &bot.SendMessageParams{ChatID: chatID, Text: outbound.Text})
	return err
}

func (a *TelegramAdapter) Close() error {
	a.mu.Lock()
	cancel := a.cancel
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (a *TelegramAdapter) markTelegramReady(value bool) {
	a.mu.Lock()
	a.ready = value
	a.mu.Unlock()
}

func waitTelegramRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextTelegramBackoff(current time.Duration) time.Duration {
	current *= 2
	if current > 30*time.Second {
		return 30 * time.Second
	}
	return current
}

func mentionsBot(current *models.Message, me *models.User) bool {
	if me == nil {
		return false
	}
	for _, entity := range current.Entities {
		if entity.Type != models.MessageEntityTypeMention {
			continue
		}
		value := utf16Slice(current.Text, entity.Offset, entity.Length)
		if strings.EqualFold(value, "@"+me.Username) {
			return true
		}
	}
	return false
}

func removeBotMention(text string, entities []models.MessageEntity, me *models.User) string {
	if me == nil {
		return text
	}
	for _, entity := range entities {
		if entity.Type == models.MessageEntityTypeMention && strings.EqualFold(utf16Slice(text, entity.Offset, entity.Length), "@"+me.Username) {
			units := utf16.Encode([]rune(text))
			start, end := entity.Offset, entity.Offset+entity.Length
			if start >= 0 && end <= len(units) {
				units = append(units[:start], units[end:]...)
				return strings.TrimSpace(string(utf16.Decode(units)))
			}
		}
	}
	return text
}

func utf16Slice(text string, offset, length int) string {
	units := utf16.Encode([]rune(text))
	if offset < 0 || length < 0 || offset+length > len(units) {
		return ""
	}
	return string(utf16.Decode(units[offset : offset+length]))
}
