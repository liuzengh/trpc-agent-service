package platform

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"
)

// IdentityProvider establishes a server-approved identity from a request.
type IdentityProvider interface {
	Authenticate(*http.Request, string) (identityResponse, error)
}

type identityError struct {
	code string
}

func (e *identityError) Error() string { return e.code }

type JWTIdentityConfig struct {
	Issuer     string
	Audience   string
	HMACSecret []byte
	Now        func() time.Time
}

type jwtIdentityProvider struct {
	config    JWTIdentityConfig
	directory map[string]DevelopmentIdentity
}

func LoadIdentityDirectory(path string) (map[string]DevelopmentIdentity, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	directory := map[string]DevelopmentIdentity{}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&directory); err != nil {
		return nil, err
	}
	for subject, identity := range directory {
		if strings.TrimSpace(subject) == "" || identity.ID == "" || identity.ID != subject || len(identity.Assignments) == 0 {
			return nil, errors.New("invalid identity directory")
		}
		for _, assignment := range identity.Assignments {
			if assignment.TenantID == "" || !validRole(assignment.Role) {
				return nil, errors.New("invalid identity directory")
			}
		}
	}
	return directory, nil
}

func validRole(role Role) bool {
	return role == RolePlatformAdmin || role == RoleTenantAdmin || role == RoleOperator || role == RoleViewer
}

func (p *jwtIdentityProvider) Assignments() []TenantAssignment {
	assignments := []TenantAssignment{}
	seen := map[string]bool{}
	for _, identity := range p.directory {
		for _, assignment := range identity.Assignments {
			if !seen[assignment.TenantID] {
				seen[assignment.TenantID] = true
				assignments = append(assignments, assignment)
			}
		}
	}
	return assignments
}

// NewJWTIdentityProvider returns an OIDC-compatible HS256 JWT verifier backed
// by a server-owned subject directory. The shared-secret verifier keeps local
// acceptance deterministic; hosted providers can implement IdentityProvider.
func NewJWTIdentityProvider(config JWTIdentityConfig, directory map[string]DevelopmentIdentity) IdentityProvider {
	copyDirectory := make(map[string]DevelopmentIdentity, len(directory))
	for subject, identity := range directory {
		identity.Assignments = append([]TenantAssignment(nil), identity.Assignments...)
		copyDirectory[subject] = identity
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	config.HMACSecret = append([]byte(nil), config.HMACSecret...)
	return &jwtIdentityProvider{config: config, directory: copyDirectory}
}

func (p *jwtIdentityProvider) Authenticate(r *http.Request, requestedTenant string) (identityResponse, error) {
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(authorization, "Bearer ") {
		return identityResponse{}, &identityError{code: "identity_required"}
	}
	token := strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(p.config.HMACSecret) == 0 {
		return identityResponse{}, &identityError{code: "invalid_identity_token"}
	}
	signed := parts[0] + "." + parts[1]
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return identityResponse{}, &identityError{code: "invalid_identity_token"}
	}
	mac := hmac.New(sha256.New, p.config.HMACSecret)
	_, _ = mac.Write([]byte(signed))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return identityResponse{}, &identityError{code: "invalid_identity_token"}
	}
	var header struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(headerJSON, &header) != nil || header.Algorithm != "HS256" || (header.Type != "" && header.Type != "JWT") {
		return identityResponse{}, &identityError{code: "invalid_identity_token"}
	}
	var claims struct {
		Issuer   string          `json:"iss"`
		Audience json.RawMessage `json:"aud"`
		Subject  string          `json:"sub"`
		Expires  json.Number     `json:"exp"`
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()
	if err != nil || decoder.Decode(&claims) != nil || claims.Issuer != p.config.Issuer || claims.Subject == "" || !audienceContains(claims.Audience, p.config.Audience) {
		return identityResponse{}, &identityError{code: "invalid_identity_token"}
	}
	expires, err := claims.Expires.Int64()
	if err != nil || !p.config.Now().Before(time.Unix(expires, 0)) {
		return identityResponse{}, &identityError{code: "invalid_identity_token"}
	}
	identity, ok := p.directory[claims.Subject]
	if !ok || identity.ID == "" || len(identity.Assignments) == 0 {
		return identityResponse{}, &identityError{code: "identity_not_assigned"}
	}
	if requestedTenant == "" {
		requestedTenant = strings.TrimSpace(r.Header.Get("X-Active-Tenant"))
	}
	if requestedTenant == "" {
		requestedTenant = identity.Assignments[0].TenantID
	}
	for _, assignment := range identity.Assignments {
		if assignment.TenantID == requestedTenant {
			return identityResponse{ID: identity.ID, Name: identity.Name, ActiveTenantID: assignment.TenantID, ActiveRole: assignment.Role, Assignments: append([]TenantAssignment(nil), identity.Assignments...), AuthMode: "production"}, nil
		}
	}
	return identityResponse{}, &identityError{code: "tenant_not_assigned"}
}

func audienceContains(raw json.RawMessage, expected string) bool {
	if expected == "" || len(raw) == 0 {
		return false
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single == expected
	}
	var multiple []string
	if json.Unmarshal(raw, &multiple) != nil {
		return false
	}
	for _, audience := range multiple {
		if audience == expected {
			return true
		}
	}
	return false
}

func identityPublicError(err error) (int, string, string) {
	var authErr *identityError
	if !errors.As(err, &authErr) {
		return http.StatusUnauthorized, "invalid_identity_token", "identity token is invalid"
	}
	switch authErr.code {
	case "identity_required":
		return http.StatusUnauthorized, authErr.code, "authenticated identity is required"
	case "tenant_not_assigned":
		return http.StatusForbidden, authErr.code, "tenant is not assigned to this identity"
	case "identity_not_assigned":
		return http.StatusForbidden, authErr.code, "identity has no platform assignment"
	default:
		return http.StatusUnauthorized, "invalid_identity_token", "identity token is invalid"
	}
}

const productionSessionMaxAge = 8 * time.Hour

func (h *AdminHandler) productionIdentity(r *http.Request, requestedTenant string) (identityResponse, error) {
	h.mu.Lock()
	provider := h.identityProvider
	if strings.TrimSpace(r.Header.Get("Authorization")) == "" {
		if cookie, err := r.Cookie("trpc_auth_session"); err == nil {
			if session := h.productionSessions[cookie.Value]; session != nil {
				if h.nowForIdentity().Sub(session.lastSeen) >= productionSessionMaxAge {
					delete(h.productionSessions, cookie.Value)
				} else {
					session.lastSeen = h.nowForIdentity()
					identity := session.identity
					identity.Assignments = append([]TenantAssignment(nil), identity.Assignments...)
					if requestedTenant == "" {
						h.mu.Unlock()
						return identity, nil
					}
					for _, assignment := range identity.Assignments {
						if assignment.TenantID == requestedTenant {
							identity.ActiveTenantID, identity.ActiveRole = assignment.TenantID, assignment.Role
							session.identity = identity
							h.mu.Unlock()
							return identity, nil
						}
					}
					h.mu.Unlock()
					return identityResponse{}, &identityError{code: "tenant_not_assigned"}
				}
			}
		}
	}
	h.mu.Unlock()
	if provider == nil {
		return identityResponse{}, &identityError{code: "identity_required"}
	}
	return provider.Authenticate(r, requestedTenant)
}

func (h *AdminHandler) handleProductionLogin(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	provider := h.identityProvider
	h.mu.Unlock()
	if provider == nil {
		writeError(w, http.StatusNotFound, "production_identity_disabled", "production identity is not enabled")
		return
	}
	var request struct {
		Token string `json:"token"`
	}
	if err := decodeStrict(r, &request); err != nil || len(request.Token) < 16 || len(request.Token) > 16*1024 {
		writeError(w, http.StatusBadRequest, "invalid_login", "identity token is required")
		return
	}
	authRequest := r.Clone(r.Context())
	authRequest.Header = r.Header.Clone()
	authRequest.Header.Set("Authorization", "Bearer "+request.Token)
	identity, err := provider.Authenticate(authRequest, "")
	request.Token = ""
	if err != nil {
		status, code, message := identityPublicError(err)
		writeError(w, status, code, message)
		return
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		writeError(w, http.StatusServiceUnavailable, "identity_session_unavailable", "identity session could not be created")
		return
	}
	sessionToken := hex.EncodeToString(tokenBytes)
	h.mu.Lock()
	if len(h.productionSessions) >= maxDevelopmentSessions {
		var oldestToken string
		var oldest time.Time
		for candidate, session := range h.productionSessions {
			if oldestToken == "" || session.lastSeen.Before(oldest) {
				oldestToken, oldest = candidate, session.lastSeen
			}
		}
		delete(h.productionSessions, oldestToken)
	}
	h.productionSessions[sessionToken] = &productionSession{identity: identity, lastSeen: h.nowForIdentity()}
	h.mu.Unlock()
	markAuditIdentity(w, TenantContext{TenantID: identity.ActiveTenantID, UserID: identity.ID, Role: identity.ActiveRole, Assignments: append([]TenantAssignment(nil), identity.Assignments...)})
	http.SetCookie(w, &http.Cookie{Name: "trpc_auth_session", Value: sessionToken, Path: "/", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: int(productionSessionMaxAge.Seconds())})
	writeJSON(w, http.StatusOK, identity)
}

func (h *AdminHandler) handleProductionLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("trpc_auth_session"); err == nil {
		h.mu.Lock()
		delete(h.productionSessions, cookie.Value)
		h.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: "trpc_auth_session", Value: "", Path: "/", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: -1})
	w.WriteHeader(http.StatusNoContent)
}

func (h *AdminHandler) nowForIdentity() time.Time { return time.Now().UTC() }
