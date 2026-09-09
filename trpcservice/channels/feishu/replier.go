package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const (
	requestBudget      = 2500 * time.Millisecond
	cardUpdateInterval = 300 * time.Millisecond
)

type tokenEntry struct {
	value     string
	expiresAt time.Time
}

type replier struct {
	adapter  *Adapter
	snapshot tenant.Snapshot
}

func (r *replier) Reply(
	ctx context.Context,
	_ string,
	message gateway.InboundMessage,
	events <-chan reply.Event,
) error {
	target, ok := message.Raw.(replyTarget)
	if !ok || target.ChatID == "" {
		for range events {
		}
		return errors.New("Feishu reply target is missing")
	}

	var (
		content      strings.Builder
		cardID       string
		lastUpdate   time.Time
		firstErr     error
		finalUpdated bool
	)
	for event := range events {
		switch event.Type {
		case "text_delta":
			content.WriteString(event.Text)
		case "tool_call":
			if event.ToolName != "" {
				fmt.Fprintf(&content, "\n\n> 正在调用工具：%s", event.ToolName)
			}
		case "error":
			if event.Error != "" {
				fmt.Fprintf(&content, "\n\n执行失败：%s", event.Error)
			}
		}
		if firstErr != nil || content.Len() == 0 {
			continue
		}
		if cardID == "" {
			cardID, firstErr = r.adapter.createCard(ctx, r.snapshot, target.ChatID, content.String())
			lastUpdate = r.adapter.now()
			continue
		}
		if event.Type == "done" || r.adapter.now().Sub(lastUpdate) >= cardUpdateInterval {
			firstErr = r.adapter.updateCard(ctx, r.snapshot, cardID, content.String())
			lastUpdate = r.adapter.now()
			finalUpdated = event.Type == "done" && firstErr == nil
		}
	}
	if firstErr == nil && cardID == "" {
		cardID, firstErr = r.adapter.createCard(ctx, r.snapshot, target.ChatID, content.String())
	}
	if firstErr == nil && cardID != "" && !finalUpdated {
		firstErr = r.adapter.updateCard(ctx, r.snapshot, cardID, content.String())
	}
	return firstErr
}

func (a *Adapter) accessToken(ctx context.Context, snapshot tenant.Snapshot) (string, error) {
	appID := snapshot.Binding.Config["app_id"]
	if appID == "" {
		appID = snapshot.Binding.RouteKey
	}
	appSecret, err := resolveSecretConfig(snapshot.Binding.Config, "app_secret", true)
	if err != nil {
		return "", err
	}
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	if cached, ok := a.tokens[appID]; ok && a.now().Before(cached.expiresAt) {
		return cached.value, nil
	}
	body, err := json.Marshal(map[string]string{
		"app_id":     appID,
		"app_secret": appSecret,
	})
	if err != nil {
		return "", fmt.Errorf("encode Feishu token request: %w", err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, requestBudget)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		a.baseURL+"/open-apis/auth/v3/tenant_access_token/internal",
		bytes.NewReader(body),
	)
	if err != nil {
		return "", fmt.Errorf("create Feishu token request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("request Feishu token: %w", err)
	}
	defer response.Body.Close()
	var result struct {
		Code              int    `json:"code"`
		Message           string `json:"msg"`
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int    `json:"expire"`
	}
	if err := decodeAPIResponse(response, &result); err != nil {
		return "", err
	}
	if result.Code != 0 || result.TenantAccessToken == "" {
		return "", fmt.Errorf("Feishu token API code %d: %s", result.Code, result.Message)
	}
	refreshIn := time.Duration(result.Expire) * time.Second
	if refreshIn > time.Minute {
		refreshIn -= time.Minute
	} else {
		refreshIn /= 2
	}
	a.tokens[appID] = tokenEntry{
		value:     result.TenantAccessToken,
		expiresAt: a.now().Add(refreshIn),
	}
	return result.TenantAccessToken, nil
}

func (a *Adapter) createCard(
	ctx context.Context,
	snapshot tenant.Snapshot,
	chatID string,
	content string,
) (string, error) {
	token, err := a.accessToken(ctx, snapshot)
	if err != nil {
		return "", err
	}
	card, err := cardContent(content)
	if err != nil {
		return "", err
	}
	requestBody, err := json.Marshal(map[string]string{
		"receive_id": chatID,
		"msg_type":   "interactive",
		"content":    card,
	})
	if err != nil {
		return "", fmt.Errorf("encode Feishu card request: %w", err)
	}
	endpoint := a.baseURL + "/open-apis/im/v1/messages?receive_id_type=chat_id"
	var result struct {
		Code    int    `json:"code"`
		Message string `json:"msg"`
		Data    struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if err := a.doAuthorizedJSON(ctx, http.MethodPost, endpoint, token, requestBody, &result); err != nil {
		return "", err
	}
	if result.Code != 0 || result.Data.MessageID == "" {
		return "", fmt.Errorf("Feishu create-card API code %d: %s", result.Code, result.Message)
	}
	return result.Data.MessageID, nil
}

func (a *Adapter) updateCard(
	ctx context.Context,
	snapshot tenant.Snapshot,
	messageID string,
	content string,
) error {
	token, err := a.accessToken(ctx, snapshot)
	if err != nil {
		return err
	}
	card, err := cardContent(content)
	if err != nil {
		return err
	}
	requestBody, err := json.Marshal(map[string]string{"content": card})
	if err != nil {
		return fmt.Errorf("encode Feishu card update: %w", err)
	}
	endpoint := a.baseURL + "/open-apis/im/v1/messages/" + url.PathEscape(messageID)
	var result struct {
		Code    int    `json:"code"`
		Message string `json:"msg"`
	}
	if err := a.doAuthorizedJSON(ctx, http.MethodPatch, endpoint, token, requestBody, &result); err != nil {
		return err
	}
	if result.Code != 0 {
		return fmt.Errorf("Feishu update-card API code %d: %s", result.Code, result.Message)
	}
	return nil
}

func (a *Adapter) doAuthorizedJSON(
	ctx context.Context,
	method string,
	endpoint string,
	token string,
	body []byte,
	target any,
) error {
	requestCtx, cancel := context.WithTimeout(ctx, requestBudget)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create Feishu API request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return fmt.Errorf("call Feishu API: %w", err)
	}
	defer response.Body.Close()
	return decodeAPIResponse(response, target)
}

func decodeAPIResponse(response *http.Response, target any) error {
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		var apiError struct {
			Code    int    `json:"code"`
			Message string `json:"msg"`
		}
		if json.Unmarshal(data, &apiError) == nil &&
			(apiError.Code != 0 || apiError.Message != "") {
			return fmt.Errorf(
				"Feishu API HTTP %d code %d: %s",
				response.StatusCode,
				apiError.Code,
				apiError.Message,
			)
		}
		return fmt.Errorf("Feishu API HTTP status %d", response.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target); err != nil {
		return fmt.Errorf("decode Feishu API response: %w", err)
	}
	return nil
}

func cardContent(content string) (string, error) {
	card := map[string]any{
		"config": map[string]any{
			"wide_screen_mode": true,
			"update_multi":     true,
		},
		"elements": []any{
			map[string]any{
				"tag": "div",
				"text": map[string]string{
					"tag":     "lark_md",
					"content": content,
				},
			},
		},
	}
	encoded, err := json.Marshal(card)
	if err != nil {
		return "", fmt.Errorf("encode Feishu card content: %w", err)
	}
	return string(encoded), nil
}
