package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const (
	sendRequestBudget = 4 * time.Second
	maxTextBytes      = 2048
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
	if !ok || target.UserID == "" {
		for range events {
		}
		return errors.New("WeCom reply target is missing")
	}
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
	if content.Len() == 0 {
		content.WriteString("请求已处理，但没有可展示的文本结果。")
	}
	for _, segment := range splitUTF8(content.String(), maxTextBytes) {
		if err := r.sendWithRetry(ctx, target.UserID, segment); err != nil {
			return err
		}
	}
	return nil
}

func (r *replier) sendWithRetry(ctx context.Context, userID, content string) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := r.send(ctx, userID, content); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == 2 {
			break
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
	}
	return lastErr
}

func (r *replier) send(ctx context.Context, userID, content string) error {
	token, err := r.adapter.accessToken(ctx, r.snapshot)
	if err != nil {
		return err
	}
	agentID, err := strconv.ParseInt(r.snapshot.Binding.Config["agent_id"], 10, 64)
	if err != nil || agentID <= 0 {
		return errors.New("WeCom agent_id must be a positive integer")
	}
	body, err := json.Marshal(map[string]any{
		"touser":  userID,
		"msgtype": "text",
		"agentid": agentID,
		"text": map[string]string{
			"content": content,
		},
		"safe":                     0,
		"enable_duplicate_check":   1,
		"duplicate_check_interval": 60,
	})
	if err != nil {
		return fmt.Errorf("encode WeCom message: %w", err)
	}
	endpoint := r.adapter.baseURL + "/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
	requestCtx, cancel := context.WithTimeout(ctx, sendRequestBudget)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx, http.MethodPost, endpoint, bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create WeCom send request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := r.adapter.client.Do(request)
	if err != nil {
		return fmt.Errorf("send WeCom message: %w", err)
	}
	defer response.Body.Close()
	var result apiResult
	if err := decodeAPIResponse(response, &result); err != nil {
		return err
	}
	if result.ErrCode != 0 {
		if result.ErrCode == 40014 || result.ErrCode == 42001 {
			r.adapter.invalidateToken(r.snapshot)
		}
		return fmt.Errorf("WeCom send API error %d: %s", result.ErrCode, result.ErrMsg)
	}
	return nil
}

func (a *Adapter) accessToken(ctx context.Context, snapshot tenant.Snapshot) (string, error) {
	corpID := snapshot.Binding.Config["corp_id"]
	secret, err := resolveSecretConfig(snapshot.Binding.Config, "corp_secret", true)
	if err != nil {
		return "", err
	}
	if corpID == "" {
		return "", errors.New("WeCom corp_id is required")
	}
	cacheKey := corpID + ":" + snapshot.Binding.Config["agent_id"]
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	if cached, ok := a.tokens[cacheKey]; ok && a.now().Before(cached.expiresAt) {
		return cached.value, nil
	}
	query := url.Values{"corpid": {corpID}, "corpsecret": {secret}}
	endpoint := a.baseURL + "/cgi-bin/gettoken?" + query.Encode()
	requestCtx, cancel := context.WithTimeout(ctx, sendRequestBudget)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("create WeCom token request: %w", err)
	}
	response, err := a.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("request WeCom token: %w", err)
	}
	defer response.Body.Close()
	var result struct {
		apiResult
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := decodeAPIResponse(response, &result); err != nil {
		return "", err
	}
	if result.ErrCode != 0 || result.AccessToken == "" {
		return "", fmt.Errorf("WeCom token API error %d: %s", result.ErrCode, result.ErrMsg)
	}
	refreshIn := time.Duration(result.ExpiresIn) * time.Second
	if refreshIn > time.Minute {
		refreshIn -= time.Minute
	} else {
		refreshIn /= 2
	}
	a.tokens[cacheKey] = tokenEntry{
		value:     result.AccessToken,
		expiresAt: a.now().Add(refreshIn),
	}
	return result.AccessToken, nil
}

func (a *Adapter) invalidateToken(snapshot tenant.Snapshot) {
	key := snapshot.Binding.Config["corp_id"] + ":" + snapshot.Binding.Config["agent_id"]
	a.tokenMu.Lock()
	delete(a.tokens, key)
	a.tokenMu.Unlock()
}

type apiResult struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

func decodeAPIResponse(response *http.Response, target any) error {
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("WeCom API HTTP status %d", response.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target); err != nil {
		return fmt.Errorf("decode WeCom API response: %w", err)
	}
	return nil
}

func splitUTF8(value string, maxBytes int) []string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return []string{value}
	}
	var result []string
	for len(value) > 0 {
		if len(value) <= maxBytes {
			result = append(result, value)
			break
		}
		end := maxBytes
		for end > 0 && !utf8.RuneStart(value[end]) {
			end--
		}
		if end == 0 {
			end = maxBytes
		}
		result = append(result, value[:end])
		value = value[end:]
	}
	return result
}
