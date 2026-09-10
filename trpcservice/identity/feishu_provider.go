package identity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// feishuTokenRefreshMargin renews the cached app access token before it
	// expires so an in-flight exchange never uses a stale credential.
	feishuTokenRefreshMargin = 60 * time.Second
	// feishuDefaultAppTokenTTL is used when the API omits expire.
	feishuDefaultAppTokenTTL = 2 * time.Hour
)

type FeishuConfig struct {
	ProviderID   string
	DisplayName  string
	AppID        string
	AppSecret    string
	TenantKey    string
	RedirectURI  string
	AuthorizeURL string
	APIBaseURL   string
	HTTPClient   *http.Client
	// QRAuthorizeURL is the authorization endpoint embedded in the QR code.
	// It stays separate from AuthorizeURL because the QR SDK only supports the
	// legacy login flow documented at passport.feishu.cn.
	QRAuthorizeURL string
}

type FeishuProvider struct {
	config FeishuConfig

	appTokenMu     sync.Mutex
	appToken       string
	appTokenExpiry time.Time
}

func NewFeishuProvider(config FeishuConfig) (*FeishuProvider, error) {
	config.ProviderID = strings.TrimSpace(config.ProviderID)
	config.DisplayName = strings.TrimSpace(config.DisplayName)
	config.AppID = strings.TrimSpace(config.AppID)
	config.AppSecret = strings.TrimSpace(config.AppSecret)
	config.TenantKey = strings.TrimSpace(config.TenantKey)
	config.RedirectURI = strings.TrimSpace(config.RedirectURI)
	if config.ProviderID == "" || config.AppID == "" || config.AppSecret == "" || config.RedirectURI == "" {
		return nil, errors.New("Feishu provider requires provider_id, app_id, app_secret and redirect_uri")
	}
	if config.DisplayName == "" {
		config.DisplayName = "飞书"
	}
	if config.AuthorizeURL == "" {
		config.AuthorizeURL = "https://accounts.feishu.cn/open-apis/authen/v1/authorize"
	}
	if config.APIBaseURL == "" {
		config.APIBaseURL = "https://open.feishu.cn"
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	var err error
	config.QRAuthorizeURL, err = NormalizeFeishuURL(config.QRAuthorizeURL, DefaultFeishuQRAuthorizeURL)
	if err != nil {
		return nil, err
	}
	return &FeishuProvider{config: config}, nil
}

func (p *FeishuProvider) Descriptor() ProviderDescriptor {
	return ProviderDescriptor{ProviderID: p.config.ProviderID, Type: ProviderFeishu, DisplayName: p.config.DisplayName}
}

func (p *FeishuProvider) ConfigurationMetadata() map[string]string {
	metadata := map[string]string{"app_id": p.config.AppID}
	if p.config.TenantKey != "" {
		metadata["tenant_key"] = p.config.TenantKey
	}
	return metadata
}

func (p *FeishuProvider) Begin(auth AuthRequest) (string, error) {
	if strings.TrimSpace(auth.State) == "" {
		return "", errors.New("Feishu provider requires a state")
	}
	query := url.Values{}
	query.Set("app_id", p.config.AppID)
	query.Set("redirect_uri", p.config.RedirectURI)
	query.Set("state", auth.State)
	return p.config.AuthorizeURL + "?" + query.Encode(), nil
}

// BeginQR returns the authorization URL rendered inside the embedded QR code.
// The state is the same signed value used by the redirect flow so the existing
// callback verification is reused unchanged.
func (p *FeishuProvider) BeginQR(auth AuthRequest) (string, error) {
	return FeishuQRGoto(p.config.QRAuthorizeURL, p.config.AppID, p.config.RedirectURI, auth.State)
}

func (p *FeishuProvider) Exchange(ctx context.Context, exchange AuthExchange) (Identity, error) {
	return p.exchangeV2(ctx, exchange)
}

// ExchangeQR exchanges a code produced by the embedded legacy QR SDK. It is a
// distinct seam so QR compatibility never changes the normal OAuth v2 flow.
func (p *FeishuProvider) ExchangeQR(ctx context.Context, exchange AuthExchange) (Identity, error) {
	return p.exchangeLegacy(ctx, exchange)
}

func (p *FeishuProvider) exchangeV2(ctx context.Context, exchange AuthExchange) (Identity, error) {
	code := strings.TrimSpace(exchange.Code)
	if code == "" {
		return Identity{}, errors.New("Feishu provider requires a callback code")
	}
	payload, err := json.Marshal(map[string]string{
		"grant_type": "authorization_code", "client_id": p.config.AppID, "client_secret": p.config.AppSecret,
		"code": code, "redirect_uri": p.config.RedirectURI,
	})
	if err != nil {
		return Identity{}, fmt.Errorf("encode Feishu token request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.config.APIBaseURL, "/")+"/open-apis/authen/v2/oauth/token", bytes.NewReader(payload))
	if err != nil {
		return Identity{}, fmt.Errorf("build Feishu token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return Identity{}, fmt.Errorf("exchange Feishu authorization code: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Identity{}, fmt.Errorf("Feishu token HTTP %d", resp.StatusCode)
	}
	var token struct {
		Code             int    `json:"code"`
		Message          string `json:"msg"`
		AccessToken      string `json:"access_token"`
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return Identity{}, fmt.Errorf("decode Feishu token response: %w", err)
	}
	if token.Code != 0 || token.AccessToken == "" {
		detail := token.Message
		if detail == "" {
			detail = strings.TrimSpace(token.Error + " " + token.ErrorDescription)
		}
		return Identity{}, fmt.Errorf("Feishu token exchange failed: %s", detail)
	}
	return p.userInfo(ctx, token.AccessToken)
}

func (p *FeishuProvider) userInfo(ctx context.Context, accessToken string) (Identity, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(p.config.APIBaseURL, "/")+"/open-apis/authen/v1/user_info", nil)
	if err != nil {
		return Identity{}, fmt.Errorf("build Feishu user info request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return Identity{}, fmt.Errorf("read Feishu user info: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Identity{}, fmt.Errorf("Feishu user info HTTP %d", resp.StatusCode)
	}
	var body struct {
		Code    int    `json:"code"`
		Message string `json:"msg"`
		Data    struct {
			Name            string `json:"name"`
			OpenID          string `json:"open_id"`
			UserID          string `json:"user_id"`
			Email           string `json:"email"`
			EnterpriseEmail string `json:"enterprise_email"`
			TenantKey       string `json:"tenant_key"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Identity{}, fmt.Errorf("decode Feishu user info: %w", err)
	}
	if body.Code != 0 {
		return Identity{}, fmt.Errorf("Feishu user info failed: %s", body.Message)
	}
	if p.config.TenantKey != "" && body.Data.TenantKey != p.config.TenantKey {
		return Identity{}, errors.New("Feishu account belongs to another enterprise")
	}
	subject := strings.TrimSpace(body.Data.OpenID)
	if subject == "" {
		subject = strings.TrimSpace(body.Data.UserID)
	}
	if subject == "" {
		return Identity{}, errors.New("Feishu user info returned no stable user ID")
	}
	email := strings.TrimSpace(body.Data.EnterpriseEmail)
	if email == "" {
		email = strings.TrimSpace(body.Data.Email)
	}
	return Identity{
		ProviderID: p.config.ProviderID, ProviderType: ProviderFeishu,
		EnterpriseID: p.enterpriseBoundary(), SubjectID: subject, DisplayName: strings.TrimSpace(body.Data.Name), Email: email,
	}, nil
}

func (p *FeishuProvider) enterpriseBoundary() string {
	if p.config.TenantKey != "" {
		return p.config.TenantKey
	}
	// A self-built Feishu application belongs to the deployment's enterprise.
	// When no tenant_key is pinned explicitly, app_id is the stable provider
	// boundary and avoids forcing operators to discover an extra identifier.
	return p.config.AppID
}

// exchangeLegacy completes only the QR SDK login flow using the legacy
// endpoints. Ordinary redirect login never enters this path.
func (p *FeishuProvider) exchangeLegacy(ctx context.Context, exchange AuthExchange) (Identity, error) {
	code := strings.TrimSpace(exchange.Code)
	if code == "" {
		return Identity{}, errors.New("Feishu provider requires a callback code")
	}
	appToken, err := p.appAccessToken(ctx)
	if err != nil {
		return Identity{}, err
	}
	payload, err := json.Marshal(map[string]string{
		"app_access_token": appToken, "grant_type": "authorization_code", "code": code,
	})
	if err != nil {
		return Identity{}, fmt.Errorf("encode Feishu legacy token request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.config.APIBaseURL, "/")+"/open-apis/authen/v1/access_token", bytes.NewReader(payload))
	if err != nil {
		return Identity{}, fmt.Errorf("build Feishu legacy token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return Identity{}, fmt.Errorf("exchange Feishu legacy authorization code: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Identity{}, fmt.Errorf("Feishu legacy token HTTP %d", resp.StatusCode)
	}
	var body struct {
		Code    int    `json:"code"`
		Message string `json:"msg"`
		Data    struct {
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Identity{}, fmt.Errorf("decode Feishu legacy token response: %w", err)
	}
	if body.Code != 0 || strings.TrimSpace(body.Data.AccessToken) == "" {
		return Identity{}, fmt.Errorf("Feishu legacy token exchange failed: %s", strings.TrimSpace(body.Message))
	}
	return p.userInfo(ctx, strings.TrimSpace(body.Data.AccessToken))
}

// appAccessToken returns a cached internal app access token for the legacy flow.
func (p *FeishuProvider) appAccessToken(ctx context.Context) (string, error) {
	p.appTokenMu.Lock()
	defer p.appTokenMu.Unlock()
	if p.appToken != "" && time.Now().Add(feishuTokenRefreshMargin).Before(p.appTokenExpiry) {
		return p.appToken, nil
	}
	payload, err := json.Marshal(map[string]string{"app_id": p.config.AppID, "app_secret": p.config.AppSecret})
	if err != nil {
		return "", fmt.Errorf("encode Feishu app token request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.config.APIBaseURL, "/")+"/open-apis/auth/v3/app_access_token/internal", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("build Feishu app token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request Feishu app access token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Feishu app token HTTP %d", resp.StatusCode)
	}
	var body struct {
		Code           int    `json:"code"`
		Message        string `json:"msg"`
		AppAccessToken string `json:"app_access_token"`
		Expire         int    `json:"expire"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decode Feishu app token response: %w", err)
	}
	if body.Code != 0 || strings.TrimSpace(body.AppAccessToken) == "" {
		return "", fmt.Errorf("Feishu app token request failed: %s", strings.TrimSpace(body.Message))
	}
	p.appToken = strings.TrimSpace(body.AppAccessToken)
	ttl := time.Duration(body.Expire) * time.Second
	if ttl <= 0 {
		ttl = feishuDefaultAppTokenTTL
	}
	p.appTokenExpiry = time.Now().Add(ttl)
	return p.appToken, nil
}

var _ IdentityProvider = (*FeishuProvider)(nil)
var _ IdentityProviderConfiguration = (*FeishuProvider)(nil)
var _ QRLoginProvider = (*FeishuProvider)(nil)
