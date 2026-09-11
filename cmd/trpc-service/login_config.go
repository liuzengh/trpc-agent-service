package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

func composeIdentity(ctx context.Context, getenv environment, database *sql.DB, redisClient redis.UniversalClient, httpClient *http.Client, tokenCache credential.TokenCache, secrets credential.SecretResolver) (http.Handler, identity.SessionStore, *identity.PostgresIdentityStore, error) {
	sessions, err := identity.NewRedisSessionStore(redisClient)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("construct login session store: %w", err)
	}
	users, err := identity.NewPostgresIdentityStore(database)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("construct identity store: %w", err)
	}
	feishuOptions := feishuLoginOptions{}
	feishuOptions.QRAuthorizeURL, err = identity.NormalizeFeishuURL(getenv("LOGIN_FEISHU_QR_AUTHORIZE_URL"), identity.DefaultFeishuQRAuthorizeURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("login feishu QR authorize URL: %w", err)
	}
	providers, err := composeIdentityProviders(ctx, getenv, users, httpClient, tokenCache, secrets, feishuOptions)
	if err != nil {
		return nil, nil, nil, err
	}
	ttl, err := loginSessionTTL(getenv)
	if err != nil {
		return nil, nil, nil, err
	}
	stateSecret := strings.TrimSpace(getenv("LOGIN_STATE_SECRET"))
	if stateSecret == "" {
		stateSecret, err = loginStateSecret(getenv)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if err := identity.BootstrapLocalSystemAdmin(ctx, users, identity.LocalBootstrap{
		Username:    strings.TrimSpace(getenv("SYSTEM_ADMIN_LOCAL_USERNAME")),
		Password:    getenv("SYSTEM_ADMIN_LOCAL_PASSWORD"),
		DisplayName: strings.TrimSpace(getenv("SYSTEM_ADMIN_LOCAL_DISPLAY_NAME")),
		Email:       strings.TrimSpace(getenv("SYSTEM_ADMIN_LOCAL_EMAIL")),
	}); err != nil {
		return nil, nil, nil, fmt.Errorf("bootstrap local system administrator: %w", err)
	}
	// QR login (方案 A): off by default so existing deployments keep the
	// redirect flow until an operator enables it explicitly.
	qrMode, err := web.NormalizeQRMode(getenv("LOGIN_FEISHU_QR_MODE"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("construct auth handler: %w", err)
	}
	qrSDKURL, err := identity.NormalizeFeishuURL(getenv("LOGIN_FEISHU_QR_SDK_URL"), identity.DefaultFeishuQRSDKURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("construct auth handler: %w", err)
	}
	authHandler, err := web.NewAuthHandler(web.AuthDependencies{
		Providers:                providers,
		Sessions:                 sessions,
		Users:                    users,
		Audits:                   users,
		LocalEnabled:             !strings.EqualFold(strings.TrimSpace(getenv("LOGIN_LOCAL_ENABLED")), "false"),
		LocalRegistrationEnabled: envBool(getenv, "LOGIN_LOCAL_REGISTRATION_ENABLED"),
		SessionTTL:               ttl,
		SecureCookie:             envBool(getenv, "LOGIN_SECURE_COOKIE"),
		StateSecret:              stateSecret,
		CallbackURL:              strings.TrimSpace(getenv("LOGIN_CALLBACK_URL")),
		QRMode:                   qrMode,
		QRSDKURL:                 qrSDKURL,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("construct auth handler: %w", err)
	}
	return authHandler, sessions, users, nil
}

type loginProviderFile struct {
	Providers []loginProviderConfig `json:"providers"`
}

type loginProviderConfig struct {
	ID                 string   `json:"id"`
	Type               string   `json:"type"`
	DisplayName        string   `json:"display_name"`
	CorpID             string   `json:"corp_id,omitempty"`
	AgentID            int64    `json:"agent_id,omitempty"`
	SecretRef          string   `json:"secret_ref,omitempty"`
	AppID              string   `json:"app_id,omitempty"`
	TenantKey          string   `json:"tenant_key,omitempty"`
	Issuer             string   `json:"issuer,omitempty"`
	ClientID           string   `json:"client_id,omitempty"`
	ClientSecretRef    string   `json:"client_secret_ref,omitempty"`
	Scopes             []string `json:"scopes,omitempty"`
	AuthBaseURL        string   `json:"auth_base_url,omitempty"`
	APIBaseURL         string   `json:"api_base_url,omitempty"`
	SubjectID          string   `json:"subject_id,omitempty"`
	UserDisplayName    string   `json:"user_display_name,omitempty"`
	Email              string   `json:"email,omitempty"`
	inlineSecret       string   `json:"-"`
	inlineClientSecret string   `json:"-"`
}

// feishuLoginOptions carries the deployment-wide Feishu QR settings resolved
// from the environment once and shared by every Feishu provider instance.
type feishuLoginOptions struct {
	// QRAuthorizeURL is the authorization endpoint embedded in the QR code.
	QRAuthorizeURL string
}

func composeIdentityProviders(ctx context.Context, getenv environment, store identity.LoginProviderRegistrar, httpClient *http.Client, tokenCache credential.TokenCache, secrets credential.SecretResolver, feishu feishuLoginOptions) (map[string]identity.IdentityProvider, error) {
	mode := strings.ToLower(strings.TrimSpace(getenv("LOGIN_PROVIDER")))
	hasProviderConfig := strings.TrimSpace(getenv("LOGIN_PROVIDERS_JSON")) != "" || strings.TrimSpace(getenv("LOGIN_PROVIDERS_FILE")) != ""
	if !hasProviderConfig {
		configuration, err := singleLoginProviderConfiguration(getenv, mode)
		if err != nil {
			return nil, err
		}
		return composeSelectedIdentityProviders(ctx, getenv, configuration, store, httpClient, tokenCache, secrets, feishu)
	}
	if mode != "" && mode != "mock" {
		return nil, errors.New("LOGIN_PROVIDER selects the simple single-provider mode; unset it when LOGIN_PROVIDERS_FILE or LOGIN_PROVIDERS_JSON is configured")
	}
	configuration, err := loadLoginProviderFile(getenv)
	if err != nil {
		return nil, err
	}
	selected, err := enabledLoginProviderConfigs(configuration.Providers, getenv, mode == "mock")
	if err != nil {
		return nil, err
	}
	configuration.Providers = selected
	return composeSelectedIdentityProviders(ctx, getenv, configuration, store, httpClient, tokenCache, secrets, feishu)
}

func composeSelectedIdentityProviders(ctx context.Context, getenv environment, configuration loginProviderFile, store identity.LoginProviderRegistrar, httpClient *http.Client, tokenCache credential.TokenCache, secrets credential.SecretResolver, feishu feishuLoginOptions) (map[string]identity.IdentityProvider, error) {
	providers := make(map[string]identity.IdentityProvider, len(configuration.Providers))
	callbackURL := ""
	for _, item := range configuration.Providers {
		item.ID, item.Type, item.DisplayName = strings.TrimSpace(item.ID), strings.ToLower(strings.TrimSpace(item.Type)), strings.TrimSpace(item.DisplayName)
		if item.ID == "" {
			return nil, errors.New("login provider requires id")
		}
		if _, exists := providers[item.ID]; exists {
			return nil, fmt.Errorf("duplicate login provider %q", item.ID)
		}
		var provider identity.IdentityProvider
		var enterpriseID string
		var err error
		switch item.Type {
		case string(identity.ProviderWeCom):
			if callbackURL == "" {
				callbackURL, err = loginCallbackURL(getenv)
				if err != nil {
					return nil, err
				}
			}
			secret := item.inlineSecret
			if secret == "" {
				resolved, resolveErr := secrets.Resolve(ctx, item.SecretRef)
				if resolveErr != nil {
					return nil, fmt.Errorf("resolve WeCom secret for %q: %w", item.ID, resolveErr)
				}
				secret = resolved
			}
			provider, err = identity.NewWeComProvider(identity.WeComConfig{
				ProviderID: item.ID, DisplayName: item.DisplayName,
				CorpID: item.CorpID, AgentID: item.AgentID, Secret: secret, RedirectURI: callbackURL,
				AuthBaseURL: item.AuthBaseURL, APIBaseURL: item.APIBaseURL, HTTPClient: httpClient, TokenCache: tokenCache,
			})
			enterpriseID = strings.TrimSpace(item.CorpID)
		case string(identity.ProviderFeishu):
			if callbackURL == "" {
				callbackURL, err = loginCallbackURL(getenv)
				if err != nil {
					return nil, err
				}
			}
			secret := item.inlineSecret
			if secret == "" {
				resolved, resolveErr := secrets.Resolve(ctx, item.SecretRef)
				if resolveErr != nil {
					return nil, fmt.Errorf("resolve Feishu secret for %q: %w", item.ID, resolveErr)
				}
				secret = resolved
			}
			provider, err = identity.NewFeishuProvider(identity.FeishuConfig{
				ProviderID: item.ID, DisplayName: item.DisplayName,
				AppID: item.AppID, AppSecret: secret, TenantKey: item.TenantKey, RedirectURI: callbackURL,
				AuthorizeURL: item.AuthBaseURL, APIBaseURL: item.APIBaseURL, HTTPClient: httpClient,
				QRAuthorizeURL: feishu.QRAuthorizeURL,
			})
			enterpriseID = strings.TrimSpace(item.TenantKey)
			if enterpriseID == "" {
				enterpriseID = strings.TrimSpace(item.AppID)
			}
		case string(identity.ProviderOIDC):
			if callbackURL == "" {
				callbackURL, err = loginCallbackURL(getenv)
				if err != nil {
					return nil, err
				}
			}
			secret := item.inlineClientSecret
			if secret == "" {
				resolved, resolveErr := secrets.Resolve(ctx, item.ClientSecretRef)
				if resolveErr != nil {
					return nil, fmt.Errorf("resolve OIDC secret for %q: %w", item.ID, resolveErr)
				}
				secret = resolved
			}
			provider, err = identity.NewOIDCProvider(ctx, identity.OIDCConfig{
				ProviderID: item.ID, DisplayName: item.DisplayName,
				Issuer: item.Issuer, ClientID: item.ClientID, ClientSecret: secret, RedirectURI: callbackURL,
				Scopes: item.Scopes, HTTPClient: httpClient,
			})
			enterpriseID = strings.TrimSpace(item.Issuer)
		case string(identity.ProviderMock):
			if !envBool(getenv, "LOGIN_MOCK_ENABLED") && !strings.EqualFold(strings.TrimSpace(getenv("LOGIN_PROVIDER")), "mock") {
				return nil, fmt.Errorf("login provider %q is mock but LOGIN_MOCK_ENABLED is not true", item.ID)
			}
			provider, err = identity.NewMockProvider(identity.MockConfig{
				ProviderID: item.ID, DisplayName: item.DisplayName, SubjectID: item.SubjectID,
				UserDisplayName: item.UserDisplayName, Email: item.Email,
			})
			enterpriseID = "mock:" + item.ID
		default:
			return nil, fmt.Errorf("login provider %q has unsupported type %q", item.ID, item.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("construct login provider %q: %w", item.ID, err)
		}
		if err := store.UpsertLoginProvider(ctx, provider.Descriptor(), enterpriseID); err != nil {
			return nil, err
		}
		providers[item.ID] = provider
	}
	if len(providers) == 0 {
		return nil, errors.New("login configuration requires at least one provider")
	}
	return providers, nil
}

func loginCallbackURL(getenv environment) (string, error) {
	raw := strings.TrimSpace(getenv("LOGIN_CALLBACK_URL"))
	if raw == "" {
		return "", errors.New("LOGIN_CALLBACK_URL is required for enterprise login providers")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse LOGIN_CALLBACK_URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", errors.New("LOGIN_CALLBACK_URL must be an absolute http(s) URL")
	}
	if parsed.Path != "/api/v1/auth/callback" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("LOGIN_CALLBACK_URL must point exactly to /api/v1/auth/callback without query or fragment")
	}
	return parsed.String(), nil
}

func enabledLoginProviderConfigs(configured []loginProviderConfig, getenv environment, mockShortcut bool) ([]loginProviderConfig, error) {
	if len(configured) == 0 {
		return nil, errors.New("login configuration requires at least one provider")
	}
	enterprise := make([]loginProviderConfig, 0, len(configured))
	mocks := make([]loginProviderConfig, 0, 1)
	for _, item := range configured {
		switch strings.ToLower(strings.TrimSpace(item.Type)) {
		case string(identity.ProviderMock):
			mocks = append(mocks, item)
		default:
			enterprise = append(enterprise, item)
		}
	}
	if len(mocks) > 1 {
		return nil, errors.New("login configuration may contain at most one mock provider")
	}
	if len(enterprise) == 0 && len(mocks) == 0 {
		return nil, errors.New("login configuration requires an enterprise provider or enabled mock provider")
	}

	result := append([]loginProviderConfig(nil), enterprise...)
	if envBool(getenv, "LOGIN_MOCK_ENABLED") || mockShortcut {
		if len(mocks) == 1 {
			result = append(result, mocks[0])
		} else {
			result = append(result, defaultMockLoginProviderConfig(getenv))
		}
	} else if len(mocks) == 1 {
		return nil, errors.New("mock login is configured but LOGIN_MOCK_ENABLED is not true")
	}
	if len(result) == 0 {
		return nil, errors.New("login configuration requires at least one enabled provider")
	}
	return result, nil
}

func defaultMockLoginProviderConfig(getenv environment) loginProviderConfig {
	subjectID := strings.TrimSpace(getenv("MOCK_LOGIN_SUBJECT_ID"))
	if subjectID == "" {
		subjectID = "mock-admin"
	}
	userDisplayName := strings.TrimSpace(getenv("MOCK_LOGIN_DISPLAY_NAME"))
	if userDisplayName == "" {
		userDisplayName = "Mock Admin"
	}
	return loginProviderConfig{
		ID: "mock", Type: string(identity.ProviderMock), DisplayName: "Mock 测试登录",
		SubjectID: subjectID, UserDisplayName: userDisplayName, Email: strings.TrimSpace(getenv("MOCK_LOGIN_EMAIL")),
	}
}

func singleLoginProviderConfiguration(getenv environment, mode string) (loginProviderFile, error) {
	var provider loginProviderConfig
	switch mode {
	case "mock":
		provider = defaultMockLoginProviderConfig(getenv)
	case "feishu":
		provider = loginProviderConfig{
			ID: "feishu", Type: string(identity.ProviderFeishu), DisplayName: "飞书",
			AppID:     strings.TrimSpace(getenv("FEISHU_LOGIN_APP_ID")),
			TenantKey: strings.TrimSpace(getenv("FEISHU_LOGIN_TENANT_KEY")),
			SecretRef: "env:FEISHU_LOGIN_SECRET",
		}
		if provider.AppID == "" || strings.TrimSpace(getenv("FEISHU_LOGIN_SECRET")) == "" {
			var shared struct {
				AppID     string `json:"app_id"`
				AppSecret string `json:"app_secret"`
			}
			if raw := strings.TrimSpace(getenv("FEISHU_TRAILFORGE_CONFIG_JSON")); raw != "" {
				if err := json.Unmarshal([]byte(raw), &shared); err != nil {
					return loginProviderFile{}, fmt.Errorf("decode FEISHU_TRAILFORGE_CONFIG_JSON for login reuse: %w", err)
				}
				if provider.AppID == "" {
					provider.AppID = strings.TrimSpace(shared.AppID)
				}
				provider.inlineSecret = strings.TrimSpace(shared.AppSecret)
			}
		}
	case "wecom":
		agentID, err := strconv.ParseInt(strings.TrimSpace(getenv("WECOM_LOGIN_AGENT_ID")), 10, 64)
		if err != nil || agentID <= 0 {
			return loginProviderFile{}, errors.New("WECOM_LOGIN_AGENT_ID must be a positive integer")
		}
		provider = loginProviderConfig{
			ID: "wecom", Type: string(identity.ProviderWeCom), DisplayName: "企业微信",
			CorpID: strings.TrimSpace(getenv("WECOM_LOGIN_CORP_ID")), AgentID: agentID,
			SecretRef: "env:WECOM_LOGIN_SECRET",
		}
	case "oidc":
		provider = loginProviderConfig{
			ID: "oidc", Type: string(identity.ProviderOIDC), DisplayName: "企业 SSO",
			Issuer: strings.TrimSpace(getenv("OIDC_LOGIN_ISSUER")), ClientID: strings.TrimSpace(getenv("OIDC_LOGIN_CLIENT_ID")),
			ClientSecretRef: "env:OIDC_LOGIN_CLIENT_SECRET",
		}
		if raw := strings.TrimSpace(getenv("OIDC_LOGIN_SCOPES")); raw != "" {
			for _, scope := range strings.Split(raw, ",") {
				if scope = strings.TrimSpace(scope); scope != "" {
					provider.Scopes = append(provider.Scopes, scope)
				}
			}
		}
	case "":
		return loginProviderFile{}, errors.New("LOGIN_PROVIDER or LOGIN_PROVIDERS_FILE/LOGIN_PROVIDERS_JSON is required")
	default:
		return loginProviderFile{}, fmt.Errorf("unsupported LOGIN_PROVIDER %q; use mock, feishu, wecom, or oidc", mode)
	}

	providers := []loginProviderConfig{provider}
	if mode != "mock" && envBool(getenv, "LOGIN_MOCK_ENABLED") {
		providers = append(providers, defaultMockLoginProviderConfig(getenv))
	}
	return loginProviderFile{Providers: providers}, nil
}

func loginStateSecret(getenv environment) (string, error) {
	if configured := strings.TrimSpace(getenv("LOGIN_STATE_SECRET")); configured != "" {
		return configured, nil
	}
	mode := strings.ToLower(strings.TrimSpace(getenv("LOGIN_PROVIDER")))
	if mode != "mock" && !envBool(getenv, "LOGIN_MOCK_ENABLED") {
		return "", errors.New("LOGIN_STATE_SECRET is required")
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate development login state secret: %w", err)
	}
	return hex.EncodeToString(random), nil
}

func loadLoginProviderFile(getenv environment) (loginProviderFile, error) {
	raw := strings.TrimSpace(getenv("LOGIN_PROVIDERS_JSON"))
	if path := strings.TrimSpace(getenv("LOGIN_PROVIDERS_FILE")); path != "" {
		if raw != "" {
			return loginProviderFile{}, errors.New("set only one of LOGIN_PROVIDERS_FILE or LOGIN_PROVIDERS_JSON")
		}
		bytes, err := os.ReadFile(path)
		if err != nil {
			return loginProviderFile{}, fmt.Errorf("read LOGIN_PROVIDERS_FILE: %w", err)
		}
		raw = string(bytes)
	}
	if raw == "" {
		return loginProviderFile{}, errors.New("LOGIN_PROVIDERS_FILE or LOGIN_PROVIDERS_JSON is required")
	}
	var configuration loginProviderFile
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configuration); err != nil {
		return loginProviderFile{}, fmt.Errorf("decode login provider configuration: %w", err)
	}
	return configuration, nil
}

func loginSessionTTL(getenv environment) (time.Duration, error) {
	raw := strings.TrimSpace(getenv("LOGIN_SESSION_TTL"))
	if raw == "" {
		return 8 * time.Hour, nil
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("LOGIN_SESSION_TTL must be a Go duration (e.g. 24h): %w", err)
	}
	if ttl <= 0 {
		return 0, errors.New("LOGIN_SESSION_TTL must be positive")
	}
	return ttl, nil
}
