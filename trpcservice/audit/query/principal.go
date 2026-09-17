package query

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
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const (
	auditTokenVersion     = 1
	auditTokenPrefix      = "aq1"
	maxAuditTokenSize     = 8 << 10
	maxAuditFieldSize     = 512
	maxAuditTokenLifetime = 15 * time.Minute
)

// Claims is the service-owned wire contract for the audit query bearer token.
type Claims struct {
	Version                 int    `json:"v"`
	TenantID                string `json:"tenant_id"`
	TenantVersion           int64  `json:"tenant_version"`
	SubjectID               string `json:"sub"`
	CanReadAudit            bool   `json:"can_read_audit"`
	CanReadAuditCrossTenant bool   `json:"can_read_audit_cross_tenant"`
	IssuedAt                int64  `json:"iat"`
	ExpiresAt               int64  `json:"exp"`
	TokenID                 string `json:"jti"`
}

// PrincipalResolver resolves an authenticated audit query principal.
type PrincipalResolver interface {
	Resolve(*http.Request) (Principal, error)
}

// TenantVersionCheck optionally validates the tenant version against the
// authoritative tenant repository. When nil the check is skipped.
type TenantVersionCheck func(tenantID string, version int64) error

// HMACPrincipalResolver verifies signed audit query tokens. It never accepts a
// tenant header as an identity source.
type HMACPrincipalResolver struct {
	mu           sync.RWMutex
	key          []byte
	clock        func() time.Time
	clockSkew    time.Duration
	versionCheck TenantVersionCheck
}

type ResolverOptions struct {
	ClockSkew    time.Duration
	Clock        func() time.Time
	VersionCheck TenantVersionCheck
}

func NewHMACPrincipalResolver(key []byte, options ResolverOptions) (*HMACPrincipalResolver, error) {
	if len(key) < 32 {
		return nil, runtime.ErrInvariantViolation
	}
	if options.ClockSkew <= 0 {
		options.ClockSkew = 30 * time.Second
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &HMACPrincipalResolver{key: append([]byte(nil), key...), clock: options.Clock,
		clockSkew: options.ClockSkew, versionCheck: options.VersionCheck}, nil
}

// Close removes the in-memory signing key.
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
	if r == nil || request == nil {
		return Principal{}, runtime.ErrCapabilityUnsupported
	}
	claims, err := r.claimsFromRequest(request)
	if err != nil {
		return Principal{}, err
	}
	if r.versionCheck != nil {
		if err := r.versionCheck(claims.TenantID, claims.TenantVersion); err != nil {
			return Principal{}, ErrForbidden
		}
	}
	return Principal{Authenticated: true, SubjectID: claims.SubjectID, TenantID: claims.TenantID,
		CanReadAudit: claims.CanReadAudit, CanReadAuditCrossTenant: claims.CanReadAuditCrossTenant}, nil
}

func (r *HMACPrincipalResolver) claimsFromRequest(request *http.Request) (Claims, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.key) < 32 {
		return Claims{}, runtime.ErrCapabilityUnsupported
	}
	header := request.Header.Values("Authorization")
	if len(header) != 1 || !strings.HasPrefix(header[0], "Bearer ") {
		return Claims{}, ErrUnauthenticated
	}
	token := strings.TrimSpace(strings.TrimPrefix(header[0], "Bearer "))
	if token == "" || len(token) > maxAuditTokenSize || strings.Count(token, ".") != 2 {
		return Claims{}, ErrUnauthenticated
	}
	parts := strings.SplitN(token, ".", 3)
	if parts[0] != auditTokenPrefix {
		return Claims{}, ErrUnauthenticated
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(payload) == 0 {
		return Claims{}, ErrUnauthenticated
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Claims{}, ErrUnauthenticated
	}
	mac := hmac.New(sha256.New, r.key)
	_, _ = mac.Write(payload)
	expected := mac.Sum(nil)
	if len(signature) != len(expected) || subtle.ConstantTimeCompare(signature, expected) != 1 {
		return Claims{}, ErrUnauthenticated
	}
	var claims Claims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return Claims{}, ErrUnauthenticated
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Claims{}, ErrUnauthenticated
	}
	if err := validateClaims(claims, r.clock(), r.clockSkew); err != nil {
		return Claims{}, err
	}
	return claims, nil
}

func validateClaims(claims Claims, now time.Time, skew time.Duration) error {
	if claims.Version != auditTokenVersion || claims.TenantVersion < 1 || claims.IssuedAt < 1 ||
		claims.ExpiresAt <= claims.IssuedAt || claims.TokenID == "" {
		return ErrUnauthenticated
	}
	for _, value := range []string{claims.TenantID, claims.SubjectID, claims.TokenID} {
		if !validAuditField(value) {
			return ErrUnauthenticated
		}
	}
	if claims.ExpiresAt-claims.IssuedAt > int64(maxAuditTokenLifetime/time.Second) {
		return ErrUnauthenticated
	}
	current := now.Unix()
	allow := int64(skew / time.Second)
	if current < claims.IssuedAt-allow || current > claims.ExpiresAt+allow {
		return ErrUnauthenticated
	}
	return nil
}

func validAuditField(value string) bool {
	return value != "" && len(value) <= maxAuditFieldSize && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}

// SignToken signs the exact JSON payload consumed by the resolver. It is used
// by the deployment-side token issuer and tests.
func SignToken(key []byte, claims Claims) (string, error) {
	if len(key) < 32 {
		return "", runtime.ErrInvariantViolation
	}
	if err := validateClaims(claims, time.Unix(claims.IssuedAt, 0), 0); err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return auditTokenPrefix + "." + encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

var _ PrincipalResolver = (*HMACPrincipalResolver)(nil)
