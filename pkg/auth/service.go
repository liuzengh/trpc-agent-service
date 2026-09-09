// Package auth provides replay-resistant service-to-service authentication.
package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	headerService   = "X-Service-Name"
	headerTimestamp = "X-Service-Timestamp"
	headerNonce     = "X-Service-Nonce"
	headerSignature = "X-Service-Signature"
	headerTrace     = "traceparent"
	maxSignedBody   = 2 << 20
)

// NonceStore must atomically accept a nonce once for the given TTL.
type NonceStore interface {
	UseOnce(ctx context.Context, service, nonce string, ttl time.Duration) (bool, error)
}

// RedisNonceStore is a cross-replica replay store.
type RedisNonceStore struct{ client *redis.Client }

func NewRedisNonceStore(client *redis.Client) *RedisNonceStore {
	return &RedisNonceStore{client: client}
}

func (s *RedisNonceStore) UseOnce(ctx context.Context, service, nonce string, ttl time.Duration) (bool, error) {
	if s == nil || s.client == nil {
		return false, fmt.Errorf("nonce store is not configured")
	}
	return s.client.SetNX(ctx, "service-auth:nonce:"+service+":"+nonce, "1", ttl).Result()
}

// Signer authenticates an outbound request and binds the signature to its
// method, path, trace carrier and exact body hash.
type Signer struct {
	service string
	secret  []byte
	now     func() time.Time
}

func NewSigner(service, secret string) (*Signer, error) {
	if !validServiceName(service) || len(secret) < 32 {
		return nil, fmt.Errorf("service name and a 32-character secret are required")
	}
	return &Signer{service: service, secret: []byte(secret), now: time.Now}, nil
}

func (s *Signer) Sign(req *http.Request, body []byte) error {
	if s == nil || !validServiceName(s.service) || len(s.secret) < 32 || s.now == nil {
		return fmt.Errorf("service signer is not configured")
	}
	if req == nil || req.URL == nil || req.URL.RawQuery != "" || req.URL.Fragment != "" {
		return fmt.Errorf("signed service requests must not contain query or fragment")
	}
	if len(body) > maxSignedBody {
		return fmt.Errorf("signed request body exceeds %d bytes", maxSignedBody)
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	traceParent, ok := optionalHeader(req.Header, headerTrace)
	if !ok {
		return fmt.Errorf("invalid traceparent header")
	}
	if traceParent != "" {
		req.Header.Set(headerTrace, traceParent)
	}
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return fmt.Errorf("generate service nonce: %w", err)
	}
	timestamp := strconv.FormatInt(s.now().Unix(), 10)
	nonce := hex.EncodeToString(nonceBytes)
	signature := computeSignature(s.secret, s.service, timestamp, nonce, req.Method, req.URL.EscapedPath(), traceParent, body)
	req.Header.Set(headerService, s.service)
	req.Header.Set(headerTimestamp, timestamp)
	req.Header.Set(headerNonce, nonce)
	req.Header.Set(headerSignature, signature)
	return nil
}

// Verifier validates signatures and atomically consumes nonces.
type Verifier struct {
	secret  []byte
	nonces  NonceStore
	allowed map[string]struct{}
	maxSkew time.Duration
	now     func() time.Time
}

func NewVerifier(secret string, nonces NonceStore, allowedServices ...string) (*Verifier, error) {
	if len(secret) < 32 || nonces == nil {
		return nil, fmt.Errorf("32-character service secret and nonce store are required")
	}
	allowed := make(map[string]struct{}, len(allowedServices))
	for _, service := range allowedServices {
		if !validServiceName(service) {
			return nil, fmt.Errorf("allowed service names must be non-empty ASCII tokens")
		}
		allowed[service] = struct{}{}
	}
	return &Verifier{secret: []byte(secret), nonces: nonces, allowed: allowed, maxSkew: 5 * time.Minute, now: time.Now}, nil
}

func (v *Verifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v == nil || len(v.secret) < 32 || v.nonces == nil || v.now == nil {
			http.Error(w, "authentication unavailable", http.StatusServiceUnavailable)
			return
		}
		if r == nil || r.URL == nil || r.URL.RawQuery != "" || r.URL.Fragment != "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		service, serviceOK := singleHeader(r.Header, headerService)
		timestamp, timestampOK := singleHeader(r.Header, headerTimestamp)
		nonce, nonceOK := singleHeader(r.Header, headerNonce)
		signature, signatureOK := singleHeader(r.Header, headerSignature)
		traceParent, traceOK := optionalHeader(r.Header, headerTrace)
		if _, ok := v.allowed[service]; !ok || !serviceOK || !timestampOK || !nonceOK || !signatureOK || !traceOK ||
			len(nonce) != 32 || len(signature) != sha256.Size*2 || !isHex(nonce) || !isHex(signature) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		unix, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil || !withinClockSkew(v.now(), time.Unix(unix, 0), v.maxSkew) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Body == nil {
			r.Body = http.NoBody
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxSignedBody)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		expected := computeSignature(v.secret, service, timestamp, nonce, r.Method, r.URL.EscapedPath(), traceParent, body)
		if subtle.ConstantTimeCompare([]byte(strings.ToLower(signature)), []byte(expected)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if traceParent != "" {
			r.Header.Set(headerTrace, traceParent)
		}
		used, err := v.nonces.UseOnce(r.Context(), service, nonce, 2*v.maxSkew)
		if err != nil {
			http.Error(w, "authentication unavailable", http.StatusServiceUnavailable)
			return
		}
		if !used {
			http.Error(w, "replayed request", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func singleHeader(header http.Header, name string) (string, bool) {
	values := header.Values(name)
	if len(values) != 1 || values[0] == "" || strings.ContainsAny(values[0], "\r\n\x00") {
		return "", false
	}
	return values[0], true
}

// optionalHeader accepts an absent header but rejects ambiguity or control
// characters. traceparent is part of the authentication envelope, so a
// duplicate or malformed value must fail closed; valid hex is canonicalized to
// lowercase before signing or verification.
func optionalHeader(header http.Header, name string) (string, bool) {
	values := header.Values(name)
	if len(values) == 0 {
		return "", true
	}
	if len(values) != 1 || strings.ContainsAny(values[0], "\r\n\x00") {
		return "", false
	}
	if name == headerTrace && values[0] != "" && !validTraceParent(values[0]) {
		return "", false
	}
	if name == headerTrace {
		return strings.ToLower(values[0]), true
	}
	return values[0], true
}

func validTraceParent(value string) bool {
	parts := strings.Split(value, "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return false
	}
	if !isHex(parts[0]) || !isHex(parts[1]) || !isHex(parts[2]) || !isHex(parts[3]) || strings.EqualFold(parts[0], "ff") {
		return false
	}
	if strings.Trim(parts[1], "0") == "" || strings.Trim(parts[2], "0") == "" {
		return false
	}
	return true
}

func isHex(value string) bool {
	_, err := hex.DecodeString(value)
	return err == nil
}

func validServiceName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func computeSignature(secret []byte, service, timestamp, nonce, method, path, traceParent string, body []byte) string {
	bodyHash := sha256.Sum256(body)
	canonical := strings.Join([]string{service, timestamp, nonce, method, path, traceParent, hex.EncodeToString(bodyHash[:])}, "\n")
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

// withinClockSkew compares timestamps without taking the absolute value of a
// time.Duration. time.Time.Sub saturates at the duration limits for dates far
// outside the representable range; negating the minimum duration would wrap
// it back to a negative value and could incorrectly accept an ancient signed
// request. Comparing both directions keeps the check overflow-safe.
func withinClockSkew(now, timestamp time.Time, maxSkew time.Duration) bool {
	if maxSkew < 0 {
		return false
	}
	delta := now.Sub(timestamp)
	return delta <= maxSkew && delta >= -maxSkew
}
