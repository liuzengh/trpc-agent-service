package gateway

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	principalTokenVersion = 1
	principalTokenPrefix  = "gw1"
	maxPrincipalTokenSize = 8 << 10
	maxPrincipalFieldSize = 512
)

// PrincipalClaims is the service-owned wire contract for the Gateway bearer
// token. Tokens are issued by the deployment's identity boundary and are
// verified here with a key projected through the scoped SecretProvider.
// TenantVersion deliberately participates in authorization so a tenant
// configuration/status transition invalidates older tokens immediately.
type PrincipalClaims struct {
	Version       int    `json:"v"`
	TenantID      string `json:"tenant_id"`
	TenantVersion int64  `json:"tenant_version"`
	SubjectID     string `json:"sub"`
	UserID        string `json:"user_id"`
	AgentAppID    string `json:"agent_app_id"`
	SessionID     string `json:"session_id"`
	CanRead       bool   `json:"can_read"`
	CanCancel     bool   `json:"can_cancel"`
	CanRun        bool   `json:"can_run"`
	Protocol      string `json:"protocol,omitempty"`
	TraceParent   string `json:"traceparent,omitempty"`
	IssuedAt      int64  `json:"iat"`
	ExpiresAt     int64  `json:"exp"`
	TokenID       string `json:"jti"`
}

// HMACPrincipalResolver verifies signed Gateway tokens and checks their tenant
// version against the authoritative tenant repository. It intentionally does
// not accept tenant, user, or session headers as an identity source.
type HMACPrincipalResolver struct {
	mu        sync.RWMutex
	key       []byte
	tenants   tenant.Repository
	clock     func() time.Time
	clockSkew time.Duration
}

type HMACPrincipalOptions struct {
	ClockSkew time.Duration
	Clock     func() time.Time
}

func NewHMACPrincipalResolver(key []byte, tenants tenant.Repository, options HMACPrincipalOptions) (*HMACPrincipalResolver, error) {
	if len(key) < 32 || tenants == nil {
		return nil, runtime.ErrInvariantViolation
	}
	if options.ClockSkew <= 0 {
		options.ClockSkew = 30 * time.Second
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &HMACPrincipalResolver{key: append([]byte(nil), key...), tenants: tenants,
		clock: options.Clock, clockSkew: options.ClockSkew}, nil
}

// Close removes the in-memory signing key. Callers should invoke it during
// role shutdown; all returned values are independent copies.
func (r *HMACPrincipalResolver) Close() error {
	if r != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		clear(r.key)
		r.key = nil
	}
	return nil
}

func (r *HMACPrincipalResolver) Resolve(request *http.Request) (Principal, error) {
	if r == nil || r.tenants == nil || request == nil {
		return Principal{}, runtime.ErrCapabilityUnsupported
	}
	claims, err := r.claimsFromRequest(request)
	if err != nil {
		return Principal{}, err
	}
	return r.principalForClaims(request, claims)
}

func (r *HMACPrincipalResolver) principalForClaims(request *http.Request, claims PrincipalClaims) (Principal, error) {
	current, err := r.tenants.Get(request.Context(), claims.TenantID)
	if err != nil {
		return Principal{}, err
	}
	if current.Status == tenant.StatusDisabled || current.Version != claims.TenantVersion {
		return Principal{}, runtime.ErrTenantScope
	}
	program, err := current.RedactionProgram()
	if err != nil {
		return Principal{}, runtime.ErrInvariantViolation
	}
	return Principal{Authenticated: true, TenantID: claims.TenantID, TenantVersion: claims.TenantVersion,
		SubjectID: claims.SubjectID, UserID: claims.UserID, AgentAppID: claims.AgentAppID,
		SessionID: claims.SessionID, CanRead: claims.CanRead, CanCancel: claims.CanCancel,
		CanRun: claims.CanRun && current.Status == tenant.StatusActive, TraceParent: claims.TraceParent,
		RedactionProgram: program}, nil
}

func (r *HMACPrincipalResolver) ResolveProtocolInvocation(request *http.Request) (ServerInvocationContext, error) {
	claims, err := r.claimsFromRequest(request)
	if err != nil {
		return ServerInvocationContext{}, err
	}
	principal, err := r.principalForClaims(request, claims)
	if err != nil {
		return ServerInvocationContext{}, err
	}
	protocol := protocolForPath(request.URL.Path)
	if protocol == "" {
		return ServerInvocationContext{}, runtime.ErrInvalidEnvelope
	}
	if protocol == "trpc-agent" {
		appName, ok := trpcAgentRunApp(request.URL.Path)
		if !ok || appName != principal.AgentAppID {
			return ServerInvocationContext{}, runtime.ErrTenantScope
		}
	}
	if protocol == "a2a" {
		appName, ok := a2aAppForPath(request.URL.Path)
		if !ok || appName != principal.AgentAppID {
			return ServerInvocationContext{}, runtime.ErrTenantScope
		}
	}
	if (protocol == "openai" || protocol == "trpc-agent") && !principal.CanRun {
		return ServerInvocationContext{}, ErrForbidden
	}
	if claims.Protocol != "" && claims.Protocol != protocol {
		return ServerInvocationContext{}, runtime.ErrTenantScope
	}
	value := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if value == "" && protocol == "a2a" && request.Method == http.MethodGet && isA2AAgentCardPath(request.URL.Path) {
		// Agent Card discovery is a read-only standard A2A request. It does not
		// submit work, so clients must not need a submission idempotency key.
		value = "a2a-agent-card"
	}
	if value == "" || len(value) > maxPrincipalFieldSize {
		return ServerInvocationContext{}, runtime.ErrInvalidEnvelope
	} else if strings.ContainsAny(value, "\r\n") {
		return ServerInvocationContext{}, runtime.ErrInvalidEnvelope
	} else {
		return ServerInvocationContext{Tenant: tenant.Context{TenantID: principal.TenantID,
			TenantVersion: principal.TenantVersion, AgentAppID: principal.AgentAppID,
			SubjectID: principal.SubjectID, Channel: "http", TrustedSource: "authenticated_api"},
			PrincipalID: principal.SubjectID, UserID: principal.UserID, SessionID: principal.SessionID,
			Protocol: protocol, IdempotencyKey: value, TraceParent: firstNonEmpty(principal.TraceParent, request.Header.Get("traceparent")),
			CanRead: principal.CanRead, CanCancel: principal.CanCancel, CanRun: principal.CanRun,
			RedactionProgram: principal.RedactionProgram}, nil
	}
}

func protocolForPath(path string) string {
	path = strings.Trim(path, "/")
	if path == "v1/chat/completions" {
		return "openai"
	}
	if _, ok := trpcAgentRunApp(path); ok {
		return "trpc-agent"
	}
	if _, ok := a2aAppForPath(path); ok {
		return "a2a"
	}
	return ""
}

func a2aAppForPath(path string) (string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 4 || parts[0] != "a2a" || parts[1] != "v1" || parts[2] != "apps" || !safeAppPathSegment(parts[3]) {
		return "", false
	}
	if len(parts) == 4 {
		return parts[3], true
	}
	if len(parts) == 6 && parts[4] == ".well-known" && (parts[5] == "agent-card.json" || parts[5] == "agent.json") {
		return parts[3], true
	}
	return "", false
}

func isA2AAgentCardPath(path string) bool {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return len(parts) == 6 && parts[0] == "a2a" && parts[1] == "v1" && parts[2] == "apps" &&
		safeAppPathSegment(parts[3]) && parts[4] == ".well-known" &&
		(parts[5] == "agent-card.json" || parts[5] == "agent.json")
}

func trpcAgentRunApp(path string) (string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 5 || parts[0] != "trpc-agent" || parts[1] != "v1" || parts[2] != "apps" ||
		parts[4] != "runs" || !safeAppPathSegment(parts[3]) {
		return "", false
	}
	return parts[3], true
}

func safeAppPathSegment(value string) bool {
	return validPrincipalField(value) && value != "." && value != ".." && url.PathEscape(value) == value
}

func (r *HMACPrincipalResolver) claimsFromRequest(request *http.Request) (PrincipalClaims, error) {
	if request == nil {
		return PrincipalClaims{}, runtime.ErrInvalidEnvelope
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.key) < 32 {
		return PrincipalClaims{}, runtime.ErrCapabilityUnsupported
	}
	header := request.Header.Values("Authorization")
	if len(header) != 1 || !strings.HasPrefix(header[0], "Bearer ") {
		return PrincipalClaims{}, ErrUnauthenticated
	}
	token := strings.TrimSpace(strings.TrimPrefix(header[0], "Bearer "))
	if token == "" || len(token) > maxPrincipalTokenSize || strings.Count(token, ".") != 2 {
		return PrincipalClaims{}, ErrUnauthenticated
	}
	parts := strings.SplitN(token, ".", 3)
	if parts[0] != principalTokenPrefix {
		return PrincipalClaims{}, ErrUnauthenticated
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(payload) == 0 {
		return PrincipalClaims{}, ErrUnauthenticated
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return PrincipalClaims{}, ErrUnauthenticated
	}
	mac := hmac.New(sha256.New, r.key)
	_, _ = mac.Write(payload)
	expected := mac.Sum(nil)
	if len(signature) != len(expected) || subtle.ConstantTimeCompare(signature, expected) != 1 {
		return PrincipalClaims{}, ErrUnauthenticated
	}
	var claims PrincipalClaims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return PrincipalClaims{}, ErrUnauthenticated
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return PrincipalClaims{}, ErrUnauthenticated
	}
	if err := validatePrincipalClaims(claims, r.clock(), r.clockSkew); err != nil {
		return PrincipalClaims{}, err
	}
	return claims, nil
}

func validatePrincipalClaims(claims PrincipalClaims, now time.Time, skew time.Duration) error {
	if claims.Version != principalTokenVersion || claims.TenantVersion < 1 || claims.IssuedAt < 1 || claims.ExpiresAt <= claims.IssuedAt || claims.TokenID == "" {
		return ErrUnauthenticated
	}
	for _, value := range []string{claims.TenantID, claims.SubjectID, claims.UserID, claims.AgentAppID, claims.SessionID, claims.TokenID} {
		if !validPrincipalField(value) {
			return ErrUnauthenticated
		}
	}
	if claims.Protocol != "" && claims.Protocol != "openai" && claims.Protocol != "trpc-agent" && claims.Protocol != "a2a" {
		return ErrUnauthenticated
	}
	if claims.TraceParent != "" && (len(claims.TraceParent) > maxPrincipalFieldSize || strings.ContainsAny(claims.TraceParent, "\r\n")) {
		return ErrUnauthenticated
	}
	if claims.ExpiresAt-claims.IssuedAt > int64((24*time.Hour)/time.Second) {
		return ErrUnauthenticated
	}
	current := now.Unix()
	allow := int64(skew / time.Second)
	if current < claims.IssuedAt-allow || current > claims.ExpiresAt+allow {
		return ErrUnauthenticated
	}
	return nil
}

func validPrincipalField(value string) bool {
	return value != "" && len(value) <= maxPrincipalFieldSize && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}

// SignPrincipalToken is used by identity-boundary tests and deployment-side
// token issuers. It signs the exact JSON payload consumed by the resolver.
func SignPrincipalToken(key []byte, claims PrincipalClaims) (string, error) {
	if len(key) < 32 {
		return "", runtime.ErrInvariantViolation
	}
	if err := validatePrincipalClaims(claims, time.Unix(claims.IssuedAt, 0), 0); err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return principalTokenPrefix + "." + encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

var _ PrincipalResolver = (*HMACPrincipalResolver)(nil)
var _ ProtocolInvocationResolver = (*HMACPrincipalResolver)(nil)
