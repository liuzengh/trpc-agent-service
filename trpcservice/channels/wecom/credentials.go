package wecom

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

type wecomSendSecret struct {
	CorpID     string `json:"corp_id"`
	CorpSecret string `json:"corp_secret"`
	AgentID    int64  `json:"agent_id"`
}

type cachedToken struct {
	configVersion, secretVersion int64
	token                        string
	agentID                      int64
	expiresAt                    time.Time
}

type TokenProvider struct {
	Secrets SendSecretResolver
	Client  HTTPClient
	BaseURL string
	Now     func() time.Time

	mu     sync.Mutex
	tokens map[string]cachedToken
}

func (p *TokenProvider) ResolveWeComAccessToken(ctx context.Context, destination channel.ReplyDestination, forceRefresh bool) (string, int64, error) {
	if p == nil || p.Secrets == nil || destination.Channel != "wecom" || destination.TenantID == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" || destination.ConfigVersion < 1 {
		return "", 0, runtime.ErrInvariantViolation
	}
	value, err := p.Secrets.Resolve(ctx, destination)
	if err != nil {
		return "", 0, err
	}
	defer clear(value.Bytes)
	credential, err := decodeWeComSecret(value, destination.ExternalAccountID)
	if err != nil {
		return "", 0, err
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
		return cached.token, cached.agentID, nil
	}
	p.mu.Unlock()
	token, expiry, err := p.fetchToken(ctx, credential, now)
	if err != nil {
		return "", 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tokens == nil {
		p.tokens = make(map[string]cachedToken)
	}
	p.tokens[key] = cachedToken{configVersion: destination.ConfigVersion, secretVersion: value.Version, token: token, agentID: credential.AgentID, expiresAt: expiry}
	return token, credential.AgentID, nil
}

func decodeWeComSecret(value secrets.SecretValue, externalAccountID string) (wecomSendSecret, error) {
	var parsed wecomSendSecret
	decoder := json.NewDecoder(bytes.NewReader(value.Bytes))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&parsed)
	if err == nil {
		var trailing any
		err = decoder.Decode(&trailing)
		if errors.Is(err, io.EOF) {
			err = nil
		}
	}
	if err != nil || strings.TrimSpace(parsed.CorpID) != parsed.CorpID || parsed.CorpID != externalAccountID ||
		strings.TrimSpace(parsed.CorpSecret) != parsed.CorpSecret || parsed.CorpSecret == "" || parsed.AgentID <= 0 || value.Version < 1 {
		return wecomSendSecret{}, runtime.ErrVersionMismatch
	}
	return parsed, nil
}

func (p *TokenProvider) fetchToken(ctx context.Context, credential wecomSendSecret, now time.Time) (string, time.Time, error) {
	base := strings.TrimRight(p.BaseURL, "/")
	if base == "" {
		base = defaultAPIBaseURL
	}
	endpoint, err := url.Parse(base + "/cgi-bin/gettoken")
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return "", time.Time{}, runtime.ErrInvariantViolation
	}
	query := endpoint.Query()
	query.Set("corpid", credential.CorpID)
	query.Set("corpsecret", credential.CorpSecret)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", time.Time{}, runtime.ErrInvariantViolation
	}
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
		ErrorCode   int64  `json:"errcode"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil || result.ErrorCode != 0 || result.AccessToken == "" || result.ExpiresIn < 1 {
		return "", time.Time{}, runtime.ErrBackendUnavailable
	}
	return result.AccessToken, wecomTokenExpiry(now, result.ExpiresIn), nil
}

func wecomTokenExpiry(now time.Time, seconds int64) time.Time {
	lifetime := time.Duration(seconds) * time.Second
	skew := time.Minute
	if lifetime <= skew {
		skew = lifetime / 10
	}
	return now.Add(lifetime - skew)
}

var _ AccessTokenProvider = (*TokenProvider)(nil)
