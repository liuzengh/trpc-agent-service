// Package adminauth implements the small, dependency-free management-plane
// authentication and authorization surface used by the HTTP server.
package adminauth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrUnauthorized = errors.New("admin authentication failed")
	ErrForbidden    = errors.New("admin authorization denied")
)

type Action string

const (
	ActionView           Action = "view"
	ActionOperate        Action = "operate"
	ActionManageSecurity Action = "manage_security"
)

type Principal struct {
	Subject string
	Roles   []string
	Tenants []string
	Issuer  string
}

func SubjectHash(subject string) string {
	sum := sha256.Sum256([]byte(subject))
	return hex.EncodeToString(sum[:8])
}

func (p Principal) HasRole(want string) bool {
	for _, role := range p.Roles {
		if role == want || role == "admin" {
			return true
		}
	}
	return false
}

func (p Principal) Allows(action Action, tenant string) bool {
	switch action {
	case ActionManageSecurity:
		if !p.HasRole("security-admin") {
			return false
		}
	case ActionOperate:
		if !p.HasRole("operator") && !p.HasRole("security-admin") {
			return false
		}
	case ActionView:
		if !p.HasRole("viewer") && !p.HasRole("operator") && !p.HasRole("security-admin") {
			return false
		}
	default:
		return false
	}
	if tenant == "" {
		return p.allTenants()
	}
	return p.allTenants() || contains(p.Tenants, tenant)
}

// AllowsGlobal reports whether a principal may read a global, non-mutating
// management view.  The handler must still filter tenant collections and must
// not use this method for writes: a tenant-scoped viewer may inspect the
// dependency registry or receive only its own tenant rows, while global
// mutation and security operations still require an all-tenant scope.
func (p Principal) AllowsGlobal(action Action) bool {
	switch action {
	case ActionView:
		return p.HasRole("viewer") || p.HasRole("operator") || p.HasRole("security-admin")
	case ActionOperate:
		return p.allTenants() && (p.HasRole("operator") || p.HasRole("security-admin"))
	case ActionManageSecurity:
		return p.allTenants() && p.HasRole("security-admin")
	default:
		return false
	}
}

func (p Principal) allTenants() bool {
	return contains(p.Tenants, "*") || contains(p.Tenants, "all")
}

type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}

// StaticAuthenticator is useful for local acceptance fixtures. It is not
// accepted by production configuration and intentionally does not serialize
// the configured token values.
type StaticAuthenticator struct {
	mu      sync.RWMutex
	entries map[string]Principal
}

func NewStaticAuthenticator(entries map[string]Principal) *StaticAuthenticator {
	copyEntries := make(map[string]Principal, len(entries))
	for token, principal := range entries {
		copyEntries[token] = clonePrincipal(principal)
	}
	return &StaticAuthenticator{entries: copyEntries}
}

func (a *StaticAuthenticator) Authenticate(_ context.Context, token string) (Principal, error) {
	a.mu.RLock()
	principal, ok := a.entries[token]
	a.mu.RUnlock()
	if !ok || principal.Subject == "" {
		return Principal{}, ErrUnauthorized
	}
	return clonePrincipal(principal), nil
}

type OIDCOptions struct {
	Issuer       string
	Audience     string
	JWKSURL      string
	RolesClaim   string
	TenantsClaim string
	ClockSkew    time.Duration
	CacheTTL     time.Duration
	Client       *http.Client
	Now          func() time.Time
}

type OIDC struct {
	issuer       string
	audience     string
	jwksURL      string
	rolesClaim   string
	tenantsClaim string
	clockSkew    time.Duration
	cacheTTL     time.Duration
	client       *http.Client
	now          func() time.Time

	mu         sync.RWMutex
	keys       map[string]*rsa.PublicKey
	keysLoaded time.Time
}

func NewOIDC(options OIDCOptions) (*OIDC, error) {
	if options.Issuer == "" || options.Audience == "" || options.JWKSURL == "" {
		return nil, errors.New("oidc issuer, audience and jwks url are required")
	}
	if options.ClockSkew <= 0 {
		options.ClockSkew = time.Minute
	}
	if options.CacheTTL <= 0 {
		options.CacheTTL = 5 * time.Minute
	}
	if options.RolesClaim == "" {
		options.RolesClaim = "roles"
	}
	if options.TenantsClaim == "" {
		options.TenantsClaim = "tenants"
	}
	if options.Client == nil {
		options.Client = &http.Client{Timeout: 5 * time.Second}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &OIDC{
		issuer: options.Issuer, audience: options.Audience, jwksURL: options.JWKSURL,
		rolesClaim: options.RolesClaim, tenantsClaim: options.TenantsClaim,
		clockSkew: options.ClockSkew, cacheTTL: options.CacheTTL,
		client: options.Client, now: options.Now, keys: make(map[string]*rsa.PublicKey),
	}, nil
}

func (o *OIDC) Authenticate(ctx context.Context, token string) (Principal, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Principal{}, ErrUnauthorized
	}
	decode := func(part string, dst any) error {
		data, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, dst)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	var claims map[string]any
	if err := decode(parts[0], &header); err != nil {
		return Principal{}, ErrUnauthorized
	}
	if err := decode(parts[1], &claims); err != nil {
		return Principal{}, ErrUnauthorized
	}
	if header.Alg != "RS256" || header.Kid == "" {
		return Principal{}, ErrUnauthorized
	}
	key, err := o.key(ctx, header.Kid, false)
	if err != nil {
		key, err = o.key(ctx, header.Kid, true)
	}
	if err != nil {
		return Principal{}, ErrUnauthorized
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) != key.Size() {
		return Principal{}, ErrUnauthorized
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature); err != nil {
		return Principal{}, ErrUnauthorized
	}
	if !claimsValid(claims, o.issuer, o.audience, o.now(), o.clockSkew) {
		return Principal{}, ErrUnauthorized
	}
	subject, _ := claims["sub"].(string)
	if subject == "" {
		return Principal{}, ErrUnauthorized
	}
	return Principal{Subject: subject, Issuer: o.issuer,
		Roles: claimStrings(claims[o.rolesClaim]), Tenants: claimStrings(claims[o.tenantsClaim])}, nil
}

func claimsValid(claims map[string]any, issuer, audience string, now time.Time, skew time.Duration) bool {
	if got, _ := claims["iss"].(string); got != issuer {
		return false
	}
	if !audienceContains(claims["aud"], audience) {
		return false
	}
	exp, ok := numberClaim(claims["exp"])
	if !ok || now.After(time.Unix(int64(exp), 0).Add(skew)) {
		return false
	}
	if nbf, ok := numberClaim(claims["nbf"]); ok && now.Before(time.Unix(int64(nbf), 0).Add(-skew)) {
		return false
	}
	return true
}

func audienceContains(value any, want string) bool {
	if one, ok := value.(string); ok {
		return one == want
	}
	for _, item := range claimStrings(value) {
		if item == want {
			return true
		}
	}
	return false
}

func numberClaim(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case json.Number:
		value, err := number.Float64()
		return value, err == nil
	default:
		return 0, false
	}
}

func claimStrings(value any) []string {
	switch typed := value.(type) {
	case string:
		if typed == "" {
			return nil
		}
		return []string{typed}
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok && text != "" {
				out = append(out, text)
			}
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return nil
	}
}

func (o *OIDC) key(ctx context.Context, kid string, force bool) (*rsa.PublicKey, error) {
	now := o.now()
	o.mu.RLock()
	key, exists := o.keys[kid]
	loaded := o.keysLoaded
	o.mu.RUnlock()
	if exists && !force && now.Sub(loaded) < o.cacheTTL {
		return key, nil
	}
	keys, err := o.fetchKeys(ctx)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.keys = keys
	o.keysLoaded = now
	key = keys[kid]
	o.mu.Unlock()
	if key == nil {
		return nil, errors.New("jwks kid not found")
	}
	return key, nil
}

func (o *OIDC) fetchKeys(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, o.jwksURL, nil)
	if err != nil {
		return nil, err
	}
	response, err := o.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("jwks endpoint unavailable")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var document struct {
		Keys []struct {
			KTY string `json:"kty"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
			Alg string `json:"alg"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, err
	}
	keys := make(map[string]*rsa.PublicKey)
	for _, item := range document.Keys {
		if item.KTY != "RSA" || item.Kid == "" || item.N == "" || item.E == "" {
			continue
		}
		n, err := base64.RawURLEncoding.DecodeString(item.N)
		if err != nil {
			continue
		}
		e, err := base64.RawURLEncoding.DecodeString(item.E)
		if err != nil || len(e) == 0 || len(e) > 4 {
			continue
		}
		exponent := 0
		for _, part := range e {
			exponent = exponent<<8 | int(part)
		}
		if exponent < 3 || exponent%2 == 0 {
			continue
		}
		keys[item.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: exponent}
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks contains no usable rsa keys")
	}
	return keys, nil
}

func clonePrincipal(in Principal) Principal {
	in.Roles = append([]string(nil), in.Roles...)
	in.Tenants = append([]string(nil), in.Tenants...)
	return in
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
