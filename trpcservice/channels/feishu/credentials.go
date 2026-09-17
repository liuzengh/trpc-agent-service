package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type SendSecretResolver interface {
	Resolve(context.Context, channel.ReplyDestination) (secrets.SecretValue, error)
}

type cachedAccessToken struct {
	configVersion, secretVersion int64
	token                        string
	expiresAt                    time.Time
}

// CredentialProvider parses channel_send secrets and owns the shared Feishu
// tenant access-token cache used by media fetchers.
type CredentialProvider struct {
	Secrets SendSecretResolver
	Client  HTTPClient
	BaseURL string
	Now     func() time.Time

	mu     sync.Mutex
	tokens map[string]cachedAccessToken
}

func (p *CredentialProvider) ResolveFeishuSendCredentials(ctx context.Context, destination channel.ReplyDestination) (ClientCredentials, error) {
	value, parsed, err := p.resolveSecret(ctx, destination)
	if value.Bytes != nil {
		clear(value.Bytes)
	}
	if err != nil {
		return ClientCredentials{}, err
	}
	return ClientCredentials{AppID: parsed.AppID, AppSecret: parsed.AppSecret, Version: value.Version}, nil
}

func (p *CredentialProvider) ResolveFeishuMediaAccessToken(ctx context.Context, destination channel.ReplyDestination, forceRefresh bool) (string, error) {
	value, parsed, err := p.resolveSecret(ctx, destination)
	if value.Bytes != nil {
		defer clear(value.Bytes)
	}
	if err != nil {
		return "", err
	}
	key := destination.TenantID + "\x00" + destination.ChannelBindingID + "\x00" + destination.ExternalAccountID
	p.mu.Lock()
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now()
	}
	if cached, ok := p.tokens[key]; ok && !forceRefresh && cached.configVersion == destination.ConfigVersion &&
		cached.secretVersion == value.Version && cached.token != "" && now.Before(cached.expiresAt) {
		p.mu.Unlock()
		return cached.token, nil
	}
	p.mu.Unlock()
	token, expiry, err := p.fetchToken(ctx, parsed, now)
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tokens == nil {
		p.tokens = make(map[string]cachedAccessToken)
	}
	p.tokens[key] = cachedAccessToken{configVersion: destination.ConfigVersion, secretVersion: value.Version, token: token, expiresAt: expiry}
	return token, nil
}

type feishuSendSecret struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

func (p *CredentialProvider) resolveSecret(ctx context.Context, destination channel.ReplyDestination) (secrets.SecretValue, feishuSendSecret, error) {
	if p == nil || p.Secrets == nil || destination.Channel != "feishu" || destination.TenantID == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" || destination.ConfigVersion < 1 {
		return secrets.SecretValue{}, feishuSendSecret{}, runtime.ErrInvariantViolation
	}
	value, err := p.Secrets.Resolve(ctx, destination)
	if err != nil {
		return secrets.SecretValue{}, feishuSendSecret{}, err
	}
	var parsed feishuSendSecret
	decoder := json.NewDecoder(bytes.NewReader(value.Bytes))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&parsed); err == nil {
		var trailing any
		err = decoder.Decode(&trailing)
		if errors.Is(err, io.EOF) {
			err = nil
		}
	}
	if err != nil || strings.TrimSpace(parsed.AppID) != parsed.AppID || parsed.AppID != destination.ExternalAccountID ||
		strings.TrimSpace(parsed.AppSecret) != parsed.AppSecret || parsed.AppSecret == "" || value.Version < 1 {
		clear(value.Bytes)
		return secrets.SecretValue{}, feishuSendSecret{}, runtime.ErrVersionMismatch
	}
	return value, parsed, nil
}

func (p *CredentialProvider) fetchToken(ctx context.Context, credential feishuSendSecret, now time.Time) (string, time.Time, error) {
	base := strings.TrimRight(p.BaseURL, "/")
	if base == "" {
		base = defaultFeishuAPIBaseURL
	}
	endpoint, err := url.Parse(base + "/open-apis/auth/v3/tenant_access_token/internal")
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return "", time.Time{}, runtime.ErrInvariantViolation
	}
	body, _ := json.Marshal(credential)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, runtime.ErrInvariantViolation
	}
	request.Header.Set("Content-Type", "application/json")
	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", time.Time{}, ctx.Err()
		}
		return "", time.Time{}, runtime.ErrBackendUnavailable
	}
	if response == nil || response.Body == nil {
		return "", time.Time{}, runtime.ErrBackendUnavailable
	}
	defer response.Body.Close()
	var result struct {
		Code   int    `json:"code"`
		Token  string `json:"tenant_access_token"`
		Expire int64  `json:"expire"`
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil || result.Code != 0 || result.Token == "" || result.Expire < 1 {
		return "", time.Time{}, runtime.ErrBackendUnavailable
	}
	return result.Token, tokenExpiry(now, result.Expire), nil
}

func tokenExpiry(now time.Time, seconds int64) time.Time {
	lifetime := time.Duration(seconds) * time.Second
	skew := time.Minute
	if lifetime <= skew {
		skew = lifetime / 10
	}
	return now.Add(lifetime - skew)
}

var _ CredentialsResolver = (*CredentialProvider)(nil)
var _ MediaAccessTokenProvider = (*CredentialProvider)(nil)
