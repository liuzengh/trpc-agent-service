package identity

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type OIDCConfig struct {
	ProviderID   string
	DisplayName  string
	Issuer       string
	ClientID     string
	ClientSecret string
	RedirectURI  string
	Scopes       []string
	HTTPClient   *http.Client
}

type OIDCProvider struct {
	descriptor ProviderDescriptor
	issuer     string
	oauth      oauth2.Config
	provider   *oidc.Provider
	verifier   *oidc.IDTokenVerifier
	httpClient *http.Client
}

func NewOIDCProvider(ctx context.Context, config OIDCConfig) (*OIDCProvider, error) {
	config.ProviderID = strings.TrimSpace(config.ProviderID)
	config.DisplayName = strings.TrimSpace(config.DisplayName)
	config.Issuer = strings.TrimSpace(config.Issuer)
	config.ClientID = strings.TrimSpace(config.ClientID)
	config.RedirectURI = strings.TrimSpace(config.RedirectURI)
	if config.ProviderID == "" || config.Issuer == "" || config.ClientID == "" || strings.TrimSpace(config.ClientSecret) == "" || config.RedirectURI == "" {
		return nil, errors.New("OIDC provider requires provider_id, issuer, client_id, client_secret and redirect_uri")
	}
	if config.DisplayName == "" {
		config.DisplayName = "企业 SSO"
	}
	if len(config.Scopes) == 0 {
		config.Scopes = []string{oidc.ScopeOpenID, "profile", "email"}
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	ctx = oidc.ClientContext(ctx, config.HTTPClient)
	discovered, err := oidc.NewProvider(ctx, config.Issuer)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC provider %q: %w", config.ProviderID, err)
	}
	oauthConfig := oauth2.Config{
		ClientID: config.ClientID, ClientSecret: config.ClientSecret, RedirectURL: config.RedirectURI,
		Endpoint: discovered.Endpoint(), Scopes: append([]string(nil), config.Scopes...),
	}
	return &OIDCProvider{
		descriptor: ProviderDescriptor{ProviderID: config.ProviderID, Type: ProviderOIDC, DisplayName: config.DisplayName},
		issuer:     config.Issuer, oauth: oauthConfig, provider: discovered,
		verifier: discovered.Verifier(&oidc.Config{ClientID: config.ClientID}), httpClient: config.HTTPClient,
	}, nil
}

func (p *OIDCProvider) Descriptor() ProviderDescriptor { return p.descriptor }

func (p *OIDCProvider) ConfigurationMetadata() map[string]string {
	return map[string]string{
		"issuer":    p.issuer,
		"client_id": p.oauth.ClientID,
	}
}

func (p *OIDCProvider) Begin(auth AuthRequest) (string, error) {
	if strings.TrimSpace(auth.State) == "" || strings.TrimSpace(auth.Nonce) == "" || strings.TrimSpace(auth.PKCEChallenge) == "" {
		return "", errors.New("OIDC authorization requires state, nonce and PKCE challenge")
	}
	return p.oauth.AuthCodeURL(
		auth.State,
		oauth2.SetAuthURLParam("nonce", auth.Nonce),
		oauth2.SetAuthURLParam("code_challenge", auth.PKCEChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	), nil
}

func (p *OIDCProvider) Exchange(ctx context.Context, exchange AuthExchange) (Identity, error) {
	if strings.TrimSpace(exchange.Code) == "" || strings.TrimSpace(exchange.Nonce) == "" || strings.TrimSpace(exchange.PKCEVerifier) == "" {
		return Identity{}, errors.New("OIDC exchange requires code, nonce and PKCE verifier")
	}
	ctx = oidc.ClientContext(ctx, p.httpClient)
	token, err := p.oauth.Exchange(ctx, exchange.Code, oauth2.VerifierOption(exchange.PKCEVerifier))
	if err != nil {
		return Identity{}, fmt.Errorf("exchange OIDC authorization code: %w", err)
	}
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok || rawIDToken == "" {
		return Identity{}, errors.New("OIDC token response omitted id_token")
	}
	idToken, err := p.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return Identity{}, fmt.Errorf("verify OIDC ID token: %w", err)
	}
	if idToken.Nonce != exchange.Nonce {
		return Identity{}, errors.New("OIDC nonce mismatch")
	}
	if strings.TrimSpace(idToken.Subject) == "" {
		return Identity{}, errors.New("OIDC ID token has no subject")
	}

	var claims struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	_ = idToken.Claims(&claims)
	if userInfo, userInfoErr := p.provider.UserInfo(ctx, oauth2.StaticTokenSource(token)); userInfoErr == nil {
		if userInfo.Subject != "" && userInfo.Subject != idToken.Subject {
			return Identity{}, errors.New("OIDC userinfo subject mismatch")
		}
		var profile struct {
			Name string `json:"name"`
		}
		_ = userInfo.Claims(&profile)
		if profile.Name != "" {
			claims.Name = profile.Name
		}
		if userInfo.Email != "" {
			claims.Email = userInfo.Email
		}
	}
	return Identity{
		ProviderID: p.descriptor.ProviderID, ProviderType: ProviderOIDC,
		EnterpriseID: p.issuer, SubjectID: idToken.Subject, DisplayName: strings.TrimSpace(claims.Name), Email: strings.TrimSpace(claims.Email),
	}, nil
}

var _ IdentityProvider = (*OIDCProvider)(nil)
var _ IdentityProviderConfiguration = (*OIDCProvider)(nil)
