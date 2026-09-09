// Package ilink implements the WeChat ClawBot long-poll channel.
package ilink

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const (
	channelType       = "ilink"
	defaultAPIBaseURL = "https://ilinkai.weixin.qq.com"
	channelVersion    = "1.0.3"
	contextTTL        = 24 * time.Hour
)

var ErrContextExpired = errors.New("iLink context_token is missing or expired")

// Adapter polls one iLink bot per configured route key.
type Adapter struct {
	cache     tenant.ConfigCache
	redis     redis.Cmdable
	client    *http.Client
	baseURL   string
	routeKeys []string
	uin       string
	backoff   time.Duration

	wg sync.WaitGroup
}

// New constructs an iLink adapter.
func New(
	cache tenant.ConfigCache,
	redisClient redis.Cmdable,
	client *http.Client,
	baseURL string,
	routeKeys []string,
) (*Adapter, error) {
	if cache == nil || redisClient == nil {
		return nil, errors.New("iLink config cache and Redis client are required")
	}
	if client == nil {
		client = &http.Client{}
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultAPIBaseURL
	}
	uinBytes := make([]byte, 8)
	if _, err := rand.Read(uinBytes); err != nil {
		return nil, fmt.Errorf("generate iLink UIN: %w", err)
	}
	return &Adapter{
		cache:     cache,
		redis:     redisClient,
		client:    client,
		baseURL:   strings.TrimRight(baseURL, "/"),
		routeKeys: append([]string(nil), routeKeys...),
		uin:       base64.StdEncoding.EncodeToString(uinBytes),
		backoff:   time.Second,
	}, nil
}

// Type implements channels.Adapter.
func (*Adapter) Type() string { return channelType }

// Run starts cancelable long-poll loops and returns immediately.
func (a *Adapter) Run(ctx context.Context, _ *http.ServeMux, sink channels.Sink) error {
	if sink == nil {
		return errors.New("iLink Gateway sink is required")
	}
	for _, routeKey := range a.routeKeys {
		routeKey := routeKey
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.pollLoop(ctx, routeKey, sink)
		}()
	}
	return nil
}

// NewReplier implements channels.Adapter.
func (a *Adapter) NewReplier(snapshot tenant.Snapshot) channels.Replier {
	return &replier{adapter: a, snapshot: snapshot}
}

// Wait waits for polling goroutines after their context is canceled.
func (a *Adapter) Wait() {
	a.wg.Wait()
}

func (a *Adapter) pollLoop(ctx context.Context, routeKey string, sink channels.Sink) {
	for ctx.Err() == nil {
		if err := a.pollOnce(ctx, routeKey, sink); err == nil {
			continue
		}
		timer := time.NewTimer(a.backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (a *Adapter) pollOnce(ctx context.Context, routeKey string, sink channels.Sink) error {
	snapshot, err := a.cache.ResolveBinding(ctx, channelType, routeKey)
	if err != nil {
		return fmt.Errorf("resolve iLink binding: %w", err)
	}
	token, err := resolveBotToken(snapshot)
	if err != nil {
		return err
	}
	cursor, err := a.redis.Get(ctx, cursorKey(snapshot.Binding.ID)).Result()
	if errors.Is(err, redis.Nil) {
		cursor = ""
	} else if err != nil {
		return fmt.Errorf("load iLink cursor: %w", err)
	}
	requestBody, err := json.Marshal(map[string]any{
		"get_updates_buf": cursor,
		"base_info":       baseInfo(),
	})
	if err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		a.baseURL+"/ilink/bot/getupdates",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return err
	}
	a.authorize(request, token)
	response, err := a.client.Do(request)
	if err != nil {
		return fmt.Errorf("poll iLink updates: %w", err)
	}
	defer response.Body.Close()
	var result getUpdatesResponse
	if err := decodeResponse(response, &result); err != nil {
		return err
	}
	if result.Ret != 0 {
		return fmt.Errorf("iLink getupdates ret=%d", result.Ret)
	}
	for _, message := range result.Messages {
		if message.MessageType != 1 || message.FromUserID == "" || message.ContextToken == "" {
			continue
		}
		text := message.text()
		if text == "" {
			continue
		}
		messageID := strconv.FormatInt(message.MessageID, 10)
		if message.MessageID == 0 {
			messageID = message.ClientID
		}
		inbound := gateway.InboundMessage{
			Channel:        channelType,
			RouteKey:       routeKey,
			MsgID:          messageID,
			ChatType:       "p2p",
			SenderID:       message.FromUserID,
			AddressedToBot: true,
			Text:           text,
			Raw: replyTarget{
				UserID:       message.FromUserID,
				ContextToken: message.ContextToken,
			},
			TraceID: messageID,
		}
		sessionID := gateway.DeriveSessionID(snapshot.Tenant.ID, channelType, inbound)
		if err := a.redis.Set(
			ctx, contextKey(sessionID), message.ContextToken, contextTTL,
		).Err(); err != nil {
			return fmt.Errorf("cache iLink context_token: %w", err)
		}
		if _, err := sink(ctx, inbound); err != nil {
			return err
		}
	}
	if result.Cursor != "" {
		if err := a.redis.Set(ctx, cursorKey(snapshot.Binding.ID), result.Cursor, 0).Err(); err != nil {
			return fmt.Errorf("save iLink cursor: %w", err)
		}
	}
	return nil
}

func (a *Adapter) authorize(request *http.Request, token string) {
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("AuthorizationType", "ilink_bot_token")
	request.Header.Set("X-WECHAT-UIN", a.uin)
	request.Header.Set("Content-Type", "application/json")
}

func resolveBotToken(snapshot tenant.Snapshot) (string, error) {
	reference := snapshot.Binding.Config["bot_token"]
	if reference == "" {
		return "", errors.New("iLink bot_token secret reference is required")
	}
	return tenant.ResolveSecret(reference)
}

func cursorKey(bindingID string) string {
	return "ilink:cursor:" + bindingID
}

func contextKey(sessionID string) string {
	return "ilink:ctx:" + sessionID
}

func baseInfo() map[string]string {
	return map[string]string{"channel_version": channelVersion}
}

type getUpdatesResponse struct {
	Ret      int             `json:"ret"`
	Messages []weixinMessage `json:"msgs"`
	Cursor   string          `json:"get_updates_buf"`
}

type weixinMessage struct {
	MessageID    int64         `json:"message_id"`
	FromUserID   string        `json:"from_user_id"`
	ClientID     string        `json:"client_id"`
	MessageType  int           `json:"message_type"`
	MessageState int           `json:"message_state"`
	ContextToken string        `json:"context_token"`
	Items        []messageItem `json:"item_list"`
}

func (m weixinMessage) text() string {
	var result strings.Builder
	for _, item := range m.Items {
		if item.Type == 1 && item.Text.Text != "" {
			result.WriteString(item.Text.Text)
		}
	}
	return result.String()
}

type messageItem struct {
	Type int      `json:"type"`
	Text textItem `json:"text_item"`
}

type textItem struct {
	Text string `json:"text"`
}

func decodeResponse(response *http.Response, target any) error {
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("iLink API HTTP status %d", response.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target); err != nil {
		return fmt.Errorf("decode iLink response: %w", err)
	}
	return nil
}
