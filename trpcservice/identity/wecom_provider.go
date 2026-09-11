package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
)

// WeComConfig contains deployment settings for WeCom OAuth login. Injectable
// endpoints keep contract tests isolated while production uses official defaults.
type WeComConfig struct {
	ProviderID  string
	DisplayName string
	CorpID      string
	AgentID     int64
	Secret      string
	RedirectURI string
	// AuthBaseURL defaults to https://login.work.weixin.qq.com.
	AuthBaseURL string
	// APIBaseURL defaults to https://qyapi.weixin.qq.com.
	APIBaseURL string
	HTTPClient *http.Client
	// TokenCache is shared with message delivery; nil selects a local cache.
	TokenCache credential.TokenCache
}

// WeComProvider implements IdentityProvider through WeCom OAuth.
type WeComProvider struct {
	config WeComConfig
	tokens *credential.Manager
}

// NewWeComProvider validates and constructs the provider.
func NewWeComProvider(config WeComConfig) (*WeComProvider, error) {
	config.ProviderID = strings.TrimSpace(config.ProviderID)
	config.DisplayName = strings.TrimSpace(config.DisplayName)
	if config.ProviderID == "" || config.CorpID == "" || config.AgentID <= 0 || config.Secret == "" || config.RedirectURI == "" {
		return nil, errors.New("WeCom provider requires provider_id, corp_id, positive agent_id, secret and redirect_uri")
	}
	if config.DisplayName == "" {
		config.DisplayName = "企业微信"
	}
	if config.AuthBaseURL == "" {
		config.AuthBaseURL = "https://login.work.weixin.qq.com"
	}
	if config.APIBaseURL == "" {
		config.APIBaseURL = "https://qyapi.weixin.qq.com"
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if config.TokenCache == nil {
		config.TokenCache = credential.NewMemoryTokenCache()
	}
	return &WeComProvider{
		config: config,
		tokens: credential.NewManager(config.CorpID, config.Secret, config.APIBaseURL, config.HTTPClient, config.TokenCache),
	}, nil
}

func (p *WeComProvider) Descriptor() ProviderDescriptor {
	return ProviderDescriptor{ProviderID: p.config.ProviderID, Type: ProviderWeCom, DisplayName: p.config.DisplayName}
}

func (p *WeComProvider) ConfigurationMetadata() map[string]string {
	return map[string]string{
		"corp_id":  p.config.CorpID,
		"agent_id": strconv.FormatInt(p.config.AgentID, 10),
	}
}

// Begin constructs the WeCom-hosted SSO page URL. The Console only redirects
// the browser here and never renders or owns provider authentication UI.
func (p *WeComProvider) Begin(auth AuthRequest) (string, error) {
	if strings.TrimSpace(auth.State) == "" {
		return "", errors.New("WeCom provider requires a state")
	}
	query := url.Values{}
	query.Set("login_type", "CorpApp")
	query.Set("appid", p.config.CorpID)
	query.Set("agentid", strconv.FormatInt(p.config.AgentID, 10))
	query.Set("redirect_uri", p.config.RedirectURI)
	query.Set("state", auth.State)
	return strings.TrimRight(p.config.AuthBaseURL, "/") + "/wwlogin/sso/login?" + query.Encode(), nil
}

// Exchange trades the callback code for the confirmed identity.
func (p *WeComProvider) Exchange(ctx context.Context, exchange AuthExchange) (Identity, error) {
	if strings.TrimSpace(exchange.Code) == "" {
		return Identity{}, errors.New("WeCom provider requires a callback code")
	}
	accessToken, err := p.tokens.Token(ctx)
	if err != nil {
		return Identity{}, err
	}
	got, err := p.getUserInfo(ctx, accessToken, exchange.Code)
	if err == nil {
		return got, nil
	}
	// WeCom codes 40014 and 42001 require one cache invalidation and retry.
	if expired, ok := asProviderTokenExpired(err); ok && expired {
		p.tokens.Invalidate(ctx)
		refreshed, refreshErr := p.tokens.Token(ctx)
		if refreshErr != nil {
			return Identity{}, refreshErr
		}
		if got, retryErr := p.getUserInfo(ctx, refreshed, exchange.Code); retryErr == nil {
			return got, nil
		} else {
			return Identity{}, retryErr
		}
	}
	return Identity{}, err
}

func (p *WeComProvider) getUserInfo(ctx context.Context, accessToken, code string) (Identity, error) {
	endpoint := p.config.APIBaseURL + "/cgi-bin/auth/getuserinfo?" + url.Values{
		"access_token": {accessToken},
		"code":         {code},
	}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Identity{}, fmt.Errorf("build getuserinfo request: %w", err)
	}
	response, err := p.config.HTTPClient.Do(request)
	if err != nil {
		return Identity{}, fmt.Errorf("call getuserinfo: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return Identity{}, fmt.Errorf("getuserinfo HTTP %d", response.StatusCode)
	}
	var payload struct {
		Errcode int    `json:"errcode"`
		Errmsg  string `json:"errmsg"`
		UserID  string `json:"userid"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return Identity{}, fmt.Errorf("decode getuserinfo: %w", err)
	}
	if payload.Errcode != 0 {
		return Identity{}, providerTokenError{errcode: int64(payload.Errcode), message: payload.Errmsg}
	}
	if payload.UserID == "" {
		return Identity{}, errors.New("WeCom getuserinfo returned empty userid")
	}
	return Identity{
		ProviderID: p.config.ProviderID, ProviderType: ProviderWeCom,
		EnterpriseID: p.config.CorpID, SubjectID: payload.UserID, DisplayName: payload.UserID,
	}, nil
}

type providerTokenError struct {
	errcode int64
	message string
}

func (e providerTokenError) Error() string {
	return fmt.Sprintf("WeCom getuserinfo error %d: %s", e.errcode, e.message)
}

func asProviderTokenExpired(err error) (bool, bool) {
	var providerErr providerTokenError
	if errors.As(err, &providerErr) {
		return credential.IsExpiredAccessTokenError(providerErr.errcode), true
	}
	return false, false
}

var _ IdentityProvider = (*WeComProvider)(nil)
var _ IdentityProviderConfiguration = (*WeComProvider)(nil)
