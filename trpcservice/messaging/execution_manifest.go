package messaging

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidManifest           = errors.New("invalid execution manifest")
	ErrManifestExpired           = errors.New("execution manifest expired")
	ErrManifestSignatureMismatch = errors.New("execution manifest signature mismatch")
	ErrManifestEnvelopeMismatch  = errors.New("execution manifest does not match envelope")
)

// ExecutionManifest is the Gateway-signed authorization for one immutable
// Kafka execution input. The Worker-side session fencing token deliberately is
// not part of this contract: that token is created only after a Worker obtains
// exclusive session execution ownership.
type ExecutionManifest struct {
	KeyID         string    `json:"kid"`
	TenantID      string    `json:"tenant_id"`
	AppCode       string    `json:"app_code"`
	SessionKey    string    `json:"session_key"`
	MessageID     string    `json:"message_id"`
	TraceID       string    `json:"trace_id"`
	ConfigVersion uint64    `json:"config_version"`
	PayloadSHA256 string    `json:"payload_sha256"`
	IssuedAt      time.Time `json:"issued_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	Signature     string    `json:"signature"`
}

func (m ExecutionManifest) canonicalString() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%d|%s|%d|%d",
		m.KeyID,
		m.TenantID,
		m.AppCode,
		m.SessionKey,
		m.MessageID,
		m.TraceID,
		m.ConfigVersion,
		m.PayloadSHA256,
		m.IssuedAt.Unix(),
		m.ExpiresAt.Unix(),
	)
}

func (m ExecutionManifest) validateFields() error {
	if strings.TrimSpace(m.KeyID) == "" || strings.TrimSpace(m.TenantID) == "" ||
		strings.TrimSpace(m.AppCode) == "" || strings.TrimSpace(m.SessionKey) == "" ||
		strings.TrimSpace(m.MessageID) == "" || strings.TrimSpace(m.TraceID) == "" ||
		m.ConfigVersion == 0 || len(m.PayloadSHA256) != sha256.Size*2 ||
		m.IssuedAt.IsZero() || m.ExpiresAt.IsZero() || !m.ExpiresAt.After(m.IssuedAt) {
		return ErrInvalidManifest
	}
	if _, err := hex.DecodeString(m.PayloadSHA256); err != nil {
		return ErrInvalidManifest
	}
	return nil
}

// ExecutionManifestCodec signs at Gateway and verifies at Worker using one
// domain-separated key. The injected clock keeps expiry tests deterministic.
type ExecutionManifestCodec struct {
	keyID string
	key   [sha256.Size]byte
	ttl   time.Duration
	now   func() time.Time
}

func NewExecutionManifestCodec(keyID string, rootSecret []byte, ttl time.Duration) (*ExecutionManifestCodec, error) {
	if strings.TrimSpace(keyID) == "" {
		return nil, errors.New("execution manifest key ID is required")
	}
	if len(rootSecret) < 32 {
		return nil, errors.New("execution manifest root secret must contain at least 32 bytes")
	}
	if ttl <= 0 {
		return nil, errors.New("execution manifest TTL must be positive")
	}
	material := append([]byte("trpc-agent-service/execution-manifest/v1\x00"), rootSecret...)
	return &ExecutionManifestCodec{
		keyID: strings.TrimSpace(keyID), key: sha256.Sum256(material), ttl: ttl, now: time.Now,
	}, nil
}

func (c *ExecutionManifestCodec) SignEnvelope(envelope *Envelope, appCode string, configVersion uint64, traceID string) error {
	if c == nil || envelope == nil {
		return errors.New("execution manifest codec and envelope are required")
	}
	now := c.now().UTC()
	digest := sha256.Sum256(envelope.Payload)
	manifest := ExecutionManifest{
		KeyID: c.keyID, TenantID: envelope.TenantID, AppCode: strings.TrimSpace(appCode),
		SessionKey: envelope.SessionKey, MessageID: envelope.EventID, TraceID: strings.TrimSpace(traceID),
		ConfigVersion: configVersion, PayloadSHA256: hex.EncodeToString(digest[:]),
		IssuedAt: now, ExpiresAt: now.Add(c.ttl),
	}
	if err := manifest.validateFields(); err != nil {
		return err
	}
	manifest.Signature = signManifest(c.key[:], manifest.canonicalString())
	envelope.Manifest = &manifest
	return nil
}

func (c *ExecutionManifestCodec) VerifyEnvelope(envelope Envelope) error {
	if c == nil || envelope.Manifest == nil {
		return ErrInvalidManifest
	}
	manifest := *envelope.Manifest
	if err := manifest.validateFields(); err != nil {
		return err
	}
	if manifest.KeyID != c.keyID || manifest.TenantID != envelope.TenantID ||
		manifest.SessionKey != envelope.SessionKey || manifest.MessageID != envelope.EventID {
		return ErrManifestEnvelopeMismatch
	}
	digest := sha256.Sum256(envelope.Payload)
	if subtle.ConstantTimeCompare([]byte(manifest.PayloadSHA256), []byte(hex.EncodeToString(digest[:]))) != 1 {
		return ErrManifestEnvelopeMismatch
	}
	now := c.now().UTC()
	if now.Before(manifest.IssuedAt.Add(-time.Minute)) {
		return ErrInvalidManifest
	}
	if now.After(manifest.ExpiresAt) {
		return ErrManifestExpired
	}
	expected := signManifest(c.key[:], manifest.canonicalString())
	if subtle.ConstantTimeCompare([]byte(manifest.Signature), []byte(expected)) != 1 {
		return ErrManifestSignatureMismatch
	}
	return nil
}

func signManifest(key []byte, canonical string) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}
