package secret

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type HTTPProviderMode string

const (
	HTTPProviderVault HTTPProviderMode = "vault"
	HTTPProviderKMS   HTTPProviderMode = "kms"
)

// HTTPProvider is a small production adapter for Vault's KV v2 read API and
// KMS-compatible internal secret services. It intentionally returns only
// generic errors so lookup metadata never reaches logs or traces.
type HTTPProvider struct {
	Endpoint string
	Token    string
	Mode     HTTPProviderMode
	Client   *http.Client
}

func (provider *HTTPProvider) Resolve(ctx context.Context, key string) (string, error) {
	if provider == nil || ctx == nil || strings.TrimSpace(key) == "" {
		return "", errors.New("secret: external provider request is invalid")
	}
	base, err := url.Parse(provider.Endpoint)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return "", errors.New("secret: external provider configuration is invalid")
	}
	client := providerHTTPClient(provider.Client)
	var request *http.Request
	switch provider.Mode {
	case HTTPProviderVault:
		base.Path = strings.TrimRight(base.Path, "/") + "/v1/" + strings.TrimLeft(key, "/")
		request, err = http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
		if err == nil {
			request.Header.Set("X-Vault-Token", provider.Token)
		}
	case HTTPProviderKMS:
		payload, marshalErr := json.Marshal(map[string]string{"key": key})
		if marshalErr != nil {
			return "", errors.New("secret: external provider request failed")
		}
		request, err = http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(payload))
		if err == nil {
			request.Header.Set("Authorization", "Bearer "+provider.Token)
			request.Header.Set("Content-Type", "application/json")
		}
	default:
		return "", errors.New("secret: external provider mode is unsupported")
	}
	if err != nil || provider.Token == "" {
		return "", errors.New("secret: external provider request failed")
	}
	response, err := client.Do(request)
	if err != nil {
		return "", errors.New("secret: external provider request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", errors.New("secret: external provider request failed")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", errors.New("secret: external provider response failed")
	}
	var envelope struct {
		Value string `json:"value"`
		Data  struct {
			Value string `json:"value"`
			Data  struct {
				Value string `json:"value"`
			} `json:"data"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return "", errors.New("secret: external provider response failed")
	}
	value := envelope.Value
	if value == "" {
		value = envelope.Data.Value
	}
	if value == "" {
		value = envelope.Data.Data.Value
	}
	if value == "" {
		return "", errors.New("secret: resolved value is empty")
	}
	return value, nil
}

// providerHTTPClient preserves the caller's transport and timeout while
// refusing redirects. Secret provider endpoints are trust boundaries: a
// redirect must not turn a configured Vault/KMS request into a credentialed
// request to an unreviewed host.
func providerHTTPClient(configured *http.Client) *http.Client {
	if configured == nil {
		configured = &http.Client{Timeout: 5 * time.Second}
	}
	copy := *configured
	copy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &copy
}

var _ Provider = (*HTTPProvider)(nil)
