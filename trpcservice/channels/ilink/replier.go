package ilink

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

type replyTarget struct {
	UserID       string
	ContextToken string
}

type replier struct {
	adapter  *Adapter
	snapshot tenant.Snapshot
}

func (r *replier) Reply(
	ctx context.Context,
	sessionID string,
	message gateway.InboundMessage,
	events <-chan reply.Event,
) error {
	var content strings.Builder
	for event := range events {
		switch event.Type {
		case "text_delta":
			content.WriteString(event.Text)
		case "tool_call":
			if event.ToolName != "" {
				fmt.Fprintf(&content, "\n\n正在调用工具：%s", event.ToolName)
			}
		case "error":
			if event.Error != "" {
				fmt.Fprintf(&content, "\n\n执行失败：%s", event.Error)
			}
		}
	}
	target, _ := message.Raw.(replyTarget)
	if target.UserID == "" {
		target.UserID = message.SenderID
	}
	if target.ContextToken == "" {
		token, err := r.adapter.redis.Get(ctx, contextKey(sessionID)).Result()
		if errors.Is(err, redis.Nil) {
			return ErrContextExpired
		}
		if err != nil {
			return fmt.Errorf("load iLink context_token: %w", err)
		}
		target.ContextToken = token
	}
	if target.UserID == "" || target.ContextToken == "" {
		return ErrContextExpired
	}
	if content.Len() == 0 {
		content.WriteString("请求已处理，但没有可展示的文本结果。")
	}
	botToken, err := resolveBotToken(r.snapshot)
	if err != nil {
		return err
	}
	clientID, err := newClientID()
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"msg": map[string]any{
			"from_user_id":  "",
			"to_user_id":    target.UserID,
			"client_id":     clientID,
			"message_type":  2,
			"message_state": 2,
			"context_token": target.ContextToken,
			"item_list": []any{
				map[string]any{
					"type":      1,
					"text_item": map[string]string{"text": content.String()},
				},
			},
		},
		"base_info": baseInfo(),
	})
	if err != nil {
		return fmt.Errorf("encode iLink reply: %w", err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		r.adapter.baseURL+"/ilink/bot/sendmessage",
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	r.adapter.authorize(request, botToken)
	response, err := r.adapter.client.Do(request)
	if err != nil {
		return fmt.Errorf("send iLink message: %w", err)
	}
	defer response.Body.Close()
	var result struct {
		Ret int `json:"ret"`
	}
	if err := decodeResponse(response, &result); err != nil {
		return err
	}
	if result.Ret == -14 {
		_ = r.adapter.redis.Del(ctx, contextKey(sessionID)).Err()
		return ErrContextExpired
	}
	if result.Ret != 0 {
		return fmt.Errorf("iLink sendmessage ret=%d", result.Ret)
	}
	return nil
}

func newClientID() (string, error) {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate iLink client_id: %w", err)
	}
	return "trpc-agent:" + hex.EncodeToString(random[:]), nil
}
