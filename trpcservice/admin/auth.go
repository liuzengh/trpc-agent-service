package admin

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
	adminTokenVersion     = 1
	adminTokenPrefix      = "adm1"
	maxAdminTokenSize     = 8 << 10
	maxAdminFieldSize     = 512
	maxAdminTokenLifetime = 15 * time.Minute
)

// Claims is the service-owned wire contract for an Admin API bearer token.
// It deliberately has a separate format and signing key from Gateway tokens,
// so a data-plane token can never acquire configuration-management rights.
type Claims struct {
	Version           int    `json:"v"`
	TenantID          string `json:"tenant_id"`
	TenantVersion     int64  `json:"tenant_version"`
	SubjectID         string `json:"sub"`
	CanManage         bool   `json:"can_manage"`
	CanManageReleases bool   `json:"can_manage_releases,omitempty"`
	IssuedAt          int64  `json:"iat"`
	ExpiresAt         int64  `json:"exp"`
	TokenID           string `json:"jti"`
}

// TenantVersionCheck binds issued tokens to the tenant's authoritative
// version. Passing nil is useful only in isolated unit tests.
type TenantVersionCheck func(tenantID string, version int64) error

type HMACPrincipalResolver struct {
	mu           sync.RWMutex
	key          []byte
	clock        func() time.Time
	clockSkew    time.Duration
	versionCheck TenantVersionCheck
}

type HMACPrincipalOptions struct {
	ClockSkew    time.Duration
	Clock        func() time.Time
	VersionCheck TenantVersionCheck
}

func NewHMACPrincipalResolver(key []byte, options HMACPrincipalOptions) (*HMACPrincipalResolver, error) {
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
	return Principal{Authenticated: true, TenantID: claims.TenantID, SubjectID: claims.SubjectID, CanManage: claims.CanManage, CanManageReleases: claims.CanManageReleases}, nil
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
	if token == "" || len(token) > maxAdminTokenSize || strings.Count(token, ".") != 2 {
		return Claims{}, ErrUnauthenticated
	}
	parts := strings.SplitN(token, ".", 3)
	if parts[0] != adminTokenPrefix {
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
	if claims.Version != adminTokenVersion || claims.TenantVersion < 1 || !claims.CanManage || claims.IssuedAt < 1 ||
		claims.ExpiresAt <= claims.IssuedAt || claims.ExpiresAt-claims.IssuedAt > int64(maxAdminTokenLifetime/time.Second) {
		return ErrUnauthenticated
	}
	for _, value := range []string{claims.TenantID, claims.SubjectID, claims.TokenID} {
		if !validClaimField(value) {
			return ErrUnauthenticated
		}
	}
	issued, expires := time.Unix(claims.IssuedAt, 0), time.Unix(claims.ExpiresAt, 0)
	if issued.After(now.Add(skew)) || expires.Before(now.Add(-skew)) {
		return ErrUnauthenticated
	}
	return nil
}

func validClaimField(value string) bool {
	return value != "" && len(value) <= maxAdminFieldSize && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

// SignToken supports identity-boundary integration tests. Production callers
// should keep the signing key outside this process.
func SignToken(key []byte, claims Claims) (string, error) {
	if len(key) < 32 || validateClaims(claims, time.Unix(claims.IssuedAt, 0), 0) != nil {
		return "", runtime.ErrInvalidEnvelope
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return adminTokenPrefix + "." + base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

var _ PrincipalResolver = (*HMACPrincipalResolver)(nil)
