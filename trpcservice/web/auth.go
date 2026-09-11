package web

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

const (
	actionLoginSuccess = "user_login_success"
	actionLoginFailure = "user_login_failure"
	actionLogout       = "user_logout"
)

type AuthDependencies struct {
	Providers                map[string]identity.IdentityProvider
	Sessions                 identity.SessionStore
	Users                    identity.AuthIdentityStore
	Audits                   identity.AuditRecorder
	LocalEnabled             bool
	LocalRegistrationEnabled bool
	SessionTTL               time.Duration
	SecureCookie             bool
	StateSecret              string
	CallbackURL              string
	// QRMode enables the embedded QR login surface: "off" (default) or
	// "sdk_redirect" (QR code inside the page, top-level redirect).
	QRMode string
	// QRSDKURL overrides the official QR SDK script location, which allows
	// self-hosting the SDK for intranet deployments.
	QRSDKURL string
}

// Supported values for AuthDependencies.QRMode.
const (
	QRModeOff         = "off"
	QRModeSDKRedirect = "sdk_redirect"
)

func NormalizeQRMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", QRModeOff:
		return QRModeOff, nil
	case QRModeSDKRedirect:
		return QRModeSDKRedirect, nil
	default:
		return "", errors.New("auth QR mode must be off or sdk_redirect")
	}
}

func NewAuthHandler(dependencies AuthDependencies) (http.Handler, error) {
	if len(dependencies.Providers) == 0 && !dependencies.LocalEnabled {
		return nil, errors.New("at least one auth provider is required")
	}
	for providerID, provider := range dependencies.Providers {
		if provider == nil {
			return nil, fmt.Errorf("auth provider %q is nil", providerID)
		}
		if provider.Descriptor().ProviderID != providerID {
			return nil, fmt.Errorf("auth provider map key %q does not match descriptor %q", providerID, provider.Descriptor().ProviderID)
		}
	}
	if dependencies.Sessions == nil {
		return nil, errors.New("auth session store is required")
	}
	if dependencies.Users == nil {
		return nil, errors.New("auth user store is required")
	}
	if dependencies.SessionTTL <= 0 {
		return nil, errors.New("auth session TTL must be positive")
	}
	if len(dependencies.Providers) > 0 && strings.TrimSpace(dependencies.StateSecret) == "" {
		return nil, errors.New("auth state secret is required")
	}
	mode, err := NormalizeQRMode(dependencies.QRMode)
	if err != nil {
		return nil, err
	}
	dependencies.QRMode = mode
	dependencies.QRSDKURL, err = identity.NormalizeFeishuURL(dependencies.QRSDKURL, identity.DefaultFeishuQRSDKURL)
	if err != nil {
		return nil, fmt.Errorf("auth QR SDK URL: %w", err)
	}
	auth := &authHandler{dependencies: dependencies}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/providers", auth.providers)
	mux.HandleFunc("/api/v1/auth/qr/begin", auth.qrBegin)
	mux.HandleFunc("/api/v1/auth/login", auth.login)
	mux.HandleFunc("/api/v1/auth/callback", auth.callback)
	mux.HandleFunc("/api/v1/auth/local/login", auth.localLogin)
	mux.HandleFunc("/api/v1/auth/local/register", auth.localRegister)
	protected := http.NewServeMux()
	protected.HandleFunc("/api/v1/auth/me", auth.me)
	protected.HandleFunc("/api/v1/auth/logout", auth.logout)
	protected.HandleFunc("/api/v1/auth/local/password", auth.changeLocalPassword)
	protected.HandleFunc("/api/v1/auth/login-identities", auth.loginIdentities)
	protected.HandleFunc("/api/v1/auth/link", auth.linkLoginIdentity)
	protected.HandleFunc("/api/v1/auth/verify-provider", auth.verifyLoginProvider)
	protected.HandleFunc("/api/v1/auth/configuration", auth.configuration)
	mux.Handle("/api/v1/auth/", identity.CSRFMiddleware(identity.SessionMiddleware(dependencies.Sessions, dependencies.Users, dependencies.Audits, protected)))
	return mux, nil
}

type authHandler struct{ dependencies AuthDependencies }

func (a *authHandler) providers(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	configured := a.providerConfigurations(false)
	providers := make([]authProviderConfiguration, 0, len(configured))
	for _, provider := range configured {
		if provider.Enabled {
			providers = append(providers, provider)
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{"providers": providers})
}

type authProviderConfiguration struct {
	Type                identity.ProviderType `json:"type"`
	DisplayName         string                `json:"display_name"`
	ProviderID          string                `json:"provider_id,omitempty"`
	Configured          bool                  `json:"configured"`
	Enabled             bool                  `json:"enabled"`
	RegistrationEnabled bool                  `json:"registration_enabled,omitempty"`
	QRSupported         bool                  `json:"qr_supported"`
	QREnabled           bool                  `json:"qr_enabled"`
	Metadata            map[string]string     `json:"metadata,omitempty"`
	LastSuccessfulLogin *time.Time            `json:"last_successful_login_at,omitempty"`
}

func (a *authHandler) configuration(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	if !requireSystemAdmin(writer, request) {
		return
	}
	providers := a.providerConfigurations(true)
	if activity, ok := a.dependencies.Users.(identity.LoginProviderActivityStore); ok {
		for index := range providers {
			provider := &providers[index]
			if provider.ProviderID == "" || provider.Type == identity.ProviderLocal {
				continue
			}
			latest, found, err := activity.LatestLoginAtForProvider(request.Context(), provider.ProviderID)
			if err != nil {
				writeAuthError(writer, http.StatusInternalServerError, "read login provider verification", err)
				return
			}
			if found {
				provider.LastSuccessfulLogin = &latest
			}
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"callback_url": strings.TrimSpace(a.dependencies.CallbackURL),
		"providers":    providers,
	})
}

func (a *authHandler) providerConfigurations(includeMetadata bool) []authProviderConfiguration {
	configured := make(map[identity.ProviderType]identity.ProviderDescriptor, len(a.dependencies.Providers))
	for _, provider := range a.dependencies.Providers {
		descriptor := provider.Descriptor()
		configured[descriptor.Type] = descriptor
	}
	supported := []struct {
		typeID      identity.ProviderType
		displayName string
	}{
		{identity.ProviderLocal, "本地账号"},
		{identity.ProviderWeCom, "企业微信"},
		{identity.ProviderFeishu, "飞书"},
		{identity.ProviderOIDC, "企业 SSO"},
		{identity.ProviderMock, "Mock 测试登录"},
	}
	providers := make([]authProviderConfiguration, 0, len(supported))
	for _, item := range supported {
		if item.typeID == identity.ProviderLocal {
			providers = append(providers, authProviderConfiguration{
				Type: identity.ProviderLocal, DisplayName: item.displayName,
				ProviderID: "local", Configured: a.dependencies.LocalEnabled, Enabled: a.dependencies.LocalEnabled,
				RegistrationEnabled: a.dependencies.LocalEnabled && a.dependencies.LocalRegistrationEnabled,
			})
			continue
		}
		descriptor, ok := configured[item.typeID]
		entry := authProviderConfiguration{
			Type: item.typeID, DisplayName: item.displayName,
			Configured: ok, Enabled: ok,
		}
		if ok {
			entry.ProviderID = descriptor.ProviderID
			if strings.TrimSpace(descriptor.DisplayName) != "" {
				entry.DisplayName = descriptor.DisplayName
			}
			if provider, exists := a.dependencies.Providers[descriptor.ProviderID]; exists {
				if _, qrCapable := provider.(identity.QRLoginProvider); qrCapable {
					entry.QRSupported = true
					entry.QREnabled = a.dependencies.QRMode != QRModeOff
				}
			}
			if includeMetadata {
				if provider, exists := a.dependencies.Providers[descriptor.ProviderID]; exists {
					if configurable, ok := provider.(identity.IdentityProviderConfiguration); ok {
						entry.Metadata = configurable.ConfigurationMetadata()
					}
				}
			}
		}
		providers = append(providers, entry)
	}
	return providers
}

func (a *authHandler) localRegister(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	if !a.dependencies.LocalEnabled || !a.dependencies.LocalRegistrationEnabled {
		http.Error(writer, "not found", http.StatusNotFound)
		return
	}
	var body struct {
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
		Email       string `json:"email"`
		Password    string `json:"password"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, "注册信息无效")
		return
	}
	hash, err := identity.HashLocalPassword(body.Password)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	user, err := a.dependencies.Users.CreateLocalUser(request.Context(), body.Username, body.DisplayName, body.Email, hash, false)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	if err := a.issueSession(writer, request, user.PlatformUserID); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "create session", err)
		return
	}
	if a.dependencies.Audits != nil {
		_ = a.dependencies.Audits.RecordAudit(request.Context(), "user_local_register", "ok", user.PlatformUserID)
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"platform_user_id": user.PlatformUserID})
}

func (a *authHandler) localLogin(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	if !a.dependencies.LocalEnabled {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	credential, user, err := a.dependencies.Users.LookupLocalCredential(request.Context(), body.Username)
	if err != nil || user.Status != "active" || !identity.VerifyLocalPassword(credential.PasswordHash, body.Password) {
		if a.dependencies.Audits != nil {
			_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginFailure, "invalid_credentials", "local")
		}
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err := a.issueSession(writer, request, user.PlatformUserID); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "create session", err)
		return
	}
	if a.dependencies.Audits != nil {
		_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginSuccess, "ok", user.PlatformUserID)
	}
	writeJSON(writer, http.StatusOK, map[string]any{"must_change_password": credential.MustChangePassword})
}

func (a *authHandler) changeLocalPassword(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	user, ok := identity.UserFromContext(request.Context())
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	credential, err := a.dependencies.Users.LocalCredentialForUser(request.Context(), user.PlatformUserID)
	if err != nil {
		badRequest(writer, "local credential is not configured")
		return
	}
	var body struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	if !identity.VerifyLocalPassword(credential.PasswordHash, body.CurrentPassword) {
		writeJSON(writer, http.StatusBadRequest, map[string]any{"error": "current password is incorrect"})
		return
	}
	if identity.VerifyLocalPassword(credential.PasswordHash, body.NewPassword) {
		writeJSON(writer, http.StatusBadRequest, map[string]any{"error": "new password must differ from the current password"})
		return
	}
	hash, err := identity.HashLocalPassword(body.NewPassword)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	if err := a.dependencies.Users.SetLocalPassword(request.Context(), user.PlatformUserID, hash, false); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "update local password", err)
		return
	}
	if err := a.dependencies.Sessions.DeleteForUser(request.Context(), user.PlatformUserID); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "revoke sessions", err)
		return
	}
	if err := a.issueSession(writer, request, user.PlatformUserID); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "create replacement session", err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (a *authHandler) issueSession(writer http.ResponseWriter, request *http.Request, platformUserID string) error {
	sessionID, err := a.dependencies.Sessions.Create(request.Context(), identity.SessionUser{PlatformUserID: platformUserID}, a.dependencies.SessionTTL)
	if err != nil {
		return err
	}
	csrfToken, err := identity.RandomState()
	if err != nil {
		_ = a.dependencies.Sessions.Delete(request.Context(), sessionID)
		return err
	}
	identity.SetSessionCookie(writer, sessionID, csrfToken, a.dependencies.SecureCookie)
	return nil
}

func (a *authHandler) login(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	providerID := strings.TrimSpace(request.URL.Query().Get("provider"))
	provider, ok := a.dependencies.Providers[providerID]
	if !ok {
		badRequest(writer, "unknown login provider")
		return
	}
	transaction, challenge, err := identity.NewLoginTransaction(providerID)
	if err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "generate login transaction", err)
		return
	}
	state := identity.SignState(a.dependencies.StateSecret, transaction.State)
	authURL, err := provider.Begin(identity.AuthRequest{State: state, Nonce: transaction.Nonce, PKCEChallenge: challenge})
	if err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "begin login", err)
		return
	}
	if err := identity.SetLoginTransactionCookie(writer, transaction, a.dependencies.StateSecret, a.dependencies.SecureCookie); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "store login transaction", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"auth_url": authURL, "provider": provider.Descriptor()})
}

// qrBegin starts an embedded QR code login. It issues the same signed state and
// login transaction cookie as the redirect flow so /api/v1/auth/callback keeps
// verifying the exchange without any change.
func (a *authHandler) qrBegin(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	if a.dependencies.QRMode == QRModeOff {
		http.Error(writer, "not found", http.StatusNotFound)
		return
	}
	providerID := strings.TrimSpace(request.URL.Query().Get("provider"))
	provider, ok := a.dependencies.Providers[providerID]
	if !ok {
		badRequest(writer, "unknown login provider")
		return
	}
	qrProvider, ok := provider.(identity.QRLoginProvider)
	if !ok {
		badRequest(writer, "login provider does not support QR login")
		return
	}
	transaction, challenge, err := identity.NewLoginTransaction(providerID)
	if err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "generate login transaction", err)
		return
	}
	transaction.Flow = identity.LoginFlowQRLegacy
	state := identity.SignState(a.dependencies.StateSecret, transaction.State)
	gotoURL, err := qrProvider.BeginQR(identity.AuthRequest{State: state, Nonce: transaction.Nonce, PKCEChallenge: challenge})
	if err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "begin QR login", err)
		return
	}
	if err := identity.SetLoginTransactionCookie(writer, transaction, a.dependencies.StateSecret, a.dependencies.SecureCookie); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "store login transaction", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"goto":       gotoURL,
		"state":      state,
		"expires_in": identity.FeishuQRTTLSeconds,
		"sdk_url":    a.dependencies.QRSDKURL,
		"provider":   provider.Descriptor(),
	})
}

func (a *authHandler) linkLoginIdentity(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	user, ok := identity.UserFromContext(request.Context())
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body struct {
		ProviderID string `json:"provider_id"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	a.beginLoginIdentityLink(writer, request, strings.TrimSpace(body.ProviderID), user.PlatformUserID, "")
}

func (a *authHandler) verifyLoginProvider(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	if !requireSystemAdmin(writer, request) {
		return
	}
	user, ok := identity.UserFromContext(request.Context())
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body struct {
		ProviderID string `json:"provider_id"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	a.beginLoginIdentityLink(writer, request, strings.TrimSpace(body.ProviderID), user.PlatformUserID, "/console/?tab=login-settings")
}

func (a *authHandler) beginLoginIdentityLink(writer http.ResponseWriter, request *http.Request, providerID, platformUserID, returnPath string) {
	provider, ok := a.dependencies.Providers[providerID]
	if !ok || provider.Descriptor().Type == identity.ProviderLocal || provider.Descriptor().Type == identity.ProviderMock {
		badRequest(writer, "login provider cannot be linked")
		return
	}
	transaction, challenge, err := identity.NewLoginLinkTransaction(providerID, platformUserID)
	if err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "generate login link transaction", err)
		return
	}
	transaction.ReturnPath = returnPath
	state := identity.SignState(a.dependencies.StateSecret, transaction.State)
	authURL, err := provider.Begin(identity.AuthRequest{State: state, Nonce: transaction.Nonce, PKCEChallenge: challenge})
	if err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "begin login identity link", err)
		return
	}
	if err := identity.SetLoginTransactionCookie(writer, transaction, a.dependencies.StateSecret, a.dependencies.SecureCookie); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "store login link transaction", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"auth_url": authURL, "provider": provider.Descriptor()})
}

func (a *authHandler) loginIdentities(writer http.ResponseWriter, request *http.Request) {
	user, ok := identity.UserFromContext(request.Context())
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch request.Method {
	case http.MethodGet:
		methods, err := a.dependencies.Users.ListLoginMethods(request.Context(), user.PlatformUserID)
		if err != nil {
			writeAuthError(writer, http.StatusInternalServerError, "list login identities", err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"login_identities": methods})
	case http.MethodDelete:
		var body struct {
			ProviderID string `json:"provider_id"`
			SubjectID  string `json:"subject_id"`
		}
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		err := a.dependencies.Users.RemoveLoginIdentity(request.Context(), user.PlatformUserID, body.ProviderID, body.SubjectID)
		if errors.Is(err, identity.ErrLastLoginIdentity) {
			writeJSON(writer, http.StatusConflict, map[string]any{"error": err.Error()})
			return
		}
		if err != nil {
			badRequest(writer, err.Error())
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodDelete)
	}
}

func (a *authHandler) callback(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	identity.ClearLoginTransactionCookie(writer, a.dependencies.SecureCookie)
	rawState, valid := identity.VerifyState(a.dependencies.StateSecret, request.URL.Query().Get("state"))
	transaction, transactionErr := identity.ReadLoginTransactionCookie(request, a.dependencies.StateSecret)
	if !valid || transactionErr != nil || transaction.State != rawState {
		if a.dependencies.Audits != nil {
			_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginFailure, "invalid_state", "login transaction verification failed")
		}
		http.Redirect(writer, request, "/console/?login_error=invalid_state", http.StatusFound)
		return
	}
	// Provider cancellation is only trusted after the signed state and browser
	// transaction cookie have been matched. This prevents a cross-site GET from
	// cancelling an unrelated login or identity-link transaction.
	if denied := strings.TrimSpace(request.URL.Query().Get("error")); denied != "" {
		if a.dependencies.Audits != nil {
			_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginFailure, "provider_denied", denied)
		}
		http.Redirect(writer, request, authFailureRedirect(transaction, "access_denied"), http.StatusFound)
		return
	}
	provider, ok := a.dependencies.Providers[transaction.ProviderID]
	if !ok {
		_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginFailure, "provider_unavailable", transaction.ProviderID)
		http.Redirect(writer, request, authFailureRedirect(transaction, "provider_unavailable"), http.StatusFound)
		return
	}
	exchange := identity.AuthExchange{
		Code: request.URL.Query().Get("code"), Nonce: transaction.Nonce, PKCEVerifier: transaction.PKCEVerifier,
	}
	var (
		external identity.Identity
		err      error
	)
	if transaction.Flow == identity.LoginFlowQRLegacy {
		qrProvider, qrOK := provider.(identity.QRLoginProvider)
		if !qrOK {
			http.Redirect(writer, request, authFailureRedirect(transaction, "provider_unavailable"), http.StatusFound)
			return
		}
		external, err = qrProvider.ExchangeQR(request.Context(), exchange)
	} else {
		external, err = provider.Exchange(request.Context(), exchange)
	}
	if err != nil {
		_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginFailure, "exchange_failed", err.Error())
		http.Redirect(writer, request, authFailureRedirect(transaction, "exchange_failed"), http.StatusFound)
		return
	}
	if transaction.LinkPlatformUserID != "" {
		if err := a.dependencies.Users.LinkLoginIdentity(request.Context(), transaction.LinkPlatformUserID, external); err != nil {
			_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginFailure, "identity_link_failed", transaction.LinkPlatformUserID)
			http.Redirect(writer, request, authFailureRedirect(transaction, "identity_link_failed"), http.StatusFound)
			return
		}
		_ = a.dependencies.Audits.RecordAudit(request.Context(), "login_identity_link", "ok", transaction.LinkPlatformUserID)
		if transaction.ReturnPath != "" {
			http.Redirect(writer, request, transaction.ReturnPath+"&provider_verified="+url.QueryEscape(transaction.ProviderID), http.StatusFound)
			return
		}
		http.Redirect(writer, request, "/console/?tab=account&identity_linked=1", http.StatusFound)
		return
	}
	user, err := a.dependencies.Users.ResolveLoginIdentity(request.Context(), external)
	if err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "resolve platform user", err)
		return
	}
	if user.Status != "active" {
		_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginFailure, "account_suspended", user.PlatformUserID)
		http.Redirect(writer, request, "/console/?login_error=account_suspended", http.StatusFound)
		return
	}
	if err := a.issueSession(writer, request, user.PlatformUserID); err != nil {
		writeAuthError(writer, http.StatusInternalServerError, "create session", err)
		return
	}
	_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLoginSuccess, "ok", user.PlatformUserID)
	http.Redirect(writer, request, "/console/", http.StatusFound)
}

func authFailureRedirect(transaction identity.LoginTransaction, reason string) string {
	if transaction.ReturnPath == "/console/?tab=login-settings" {
		return transaction.ReturnPath + "&provider_verify_error=" + url.QueryEscape(reason)
	}
	if transaction.LinkPlatformUserID != "" {
		return "/console/?tab=account&link_error=" + url.QueryEscape(reason)
	}
	return "/console/?login_error=" + url.QueryEscape(reason)
}

func (a *authHandler) me(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	user, ok := identity.UserFromContext(request.Context())
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	res := map[string]any{
		"platform_user_id": user.PlatformUserID, "display_name": user.DisplayName, "email": user.Email,
		"role": string(user.Role), "is_system_admin": user.IsSystemAdmin, "tenants": user.Tenants,
		"must_change_password": user.MustChangePassword,
	}
	activeTenant := strings.TrimSpace(request.Header.Get("X-Active-Tenant"))
	if activeTenant == "" {
		activeTenant = strings.TrimSpace(request.URL.Query().Get("tenant"))
	}
	if activeTenant != "" {
		res["active_tenant_id"] = activeTenant
		for _, membership := range user.Tenants {
			if membership.TenantID == activeTenant {
				res["active_role"] = string(membership.Role)
				break
			}
		}
	} else if len(user.Tenants) > 0 {
		res["active_tenant_id"] = user.Tenants[0].TenantID
		res["active_role"] = string(user.Tenants[0].Role)
	}
	writeJSON(writer, http.StatusOK, res)
}

func (a *authHandler) logout(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	user, _ := identity.UserFromContext(request.Context())
	if cookie, err := request.Cookie("dsh_session"); err == nil && cookie.Value != "" {
		_ = a.dependencies.Sessions.Delete(request.Context(), cookie.Value)
		if a.dependencies.Audits != nil {
			_ = a.dependencies.Audits.RecordAudit(request.Context(), actionLogout, "ok", user.PlatformUserID)
		}
	}
	writer.Header().Set("Set-Cookie", identity.ClearSessionCookie(a.dependencies.SecureCookie))
	writer.WriteHeader(http.StatusOK)
}

func writeAuthError(writer http.ResponseWriter, status int, operation string, err error) {
	writeJSON(writer, status, map[string]any{"error": fmt.Sprintf("%s: %v", operation, err)})
}
