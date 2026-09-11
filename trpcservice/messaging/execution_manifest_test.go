package messaging

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func testExecutionManifestCodec(t testing.TB) *ExecutionManifestCodec {
	t.Helper()
	codec, err := NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), 5*time.Minute)
	if err != nil {
		t.Fatalf("NewExecutionManifestCodec() error = %v", err)
	}
	return codec
}

func TestExecutionManifestBindsImmutableKafkaEnvelope(t *testing.T) {
	codec := testExecutionManifestCodec(t)
	now := time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC)
	codec.now = func() time.Time { return now }
	envelope := Envelope{
		Version: CurrentEnvelopeVersion, EventID: "msg-456", TenantID: "tenant-alpha",
		SessionKey: "tenant-alpha/app/session/user-123", Type: InboundMessageType,
		Payload: []byte(`{"app_code":"app-bot","config_version":2}`),
	}
	if err := codec.SignEnvelope(&envelope, "app-bot", 2, "trace-789"); err != nil {
		t.Fatalf("SignEnvelope() error = %v", err)
	}
	if err := codec.VerifyEnvelope(envelope); err != nil {
		t.Fatalf("VerifyEnvelope() error = %v", err)
	}

	tampered := envelope
	tampered.Payload = []byte(`{"app_code":"app-bot","config_version":3}`)
	if err := codec.VerifyEnvelope(tampered); !errors.Is(err, ErrManifestEnvelopeMismatch) {
		t.Fatalf("tampered payload error = %v, want ErrManifestEnvelopeMismatch", err)
	}

	tampered = envelope
	manifest := *tampered.Manifest
	manifest.ConfigVersion = 3
	tampered.Manifest = &manifest
	if err := codec.VerifyEnvelope(tampered); !errors.Is(err, ErrManifestSignatureMismatch) {
		t.Fatalf("tampered manifest error = %v, want ErrManifestSignatureMismatch", err)
	}

	now = now.Add(6 * time.Minute)
	if err := codec.VerifyEnvelope(envelope); !errors.Is(err, ErrManifestExpired) {
		t.Fatalf("expired manifest error = %v, want ErrManifestExpired", err)
	}
}

func TestExecutionManifestCodecValidatesConstruction(t *testing.T) {
	t.Parallel()
	if _, err := NewExecutionManifestCodec(" ", []byte(strings.Repeat("k", 32)), time.Minute); err == nil {
		t.Fatal("empty key ID error = nil")
	}
	if _, err := NewExecutionManifestCodec("test", []byte("short"), time.Minute); err == nil {
		t.Fatal("short root secret error = nil")
	}
	if _, err := NewExecutionManifestCodec("test", []byte(strings.Repeat("k", 32)), 0); err == nil {
		t.Fatal("non-positive TTL error = nil")
	}
}

func TestExecutionManifestSignValidatesEnvelopeInputs(t *testing.T) {
	t.Parallel()
	codec := testExecutionManifestCodec(t)
	var nilCodec *ExecutionManifestCodec
	if err := nilCodec.SignEnvelope(&Envelope{}, "assistant", 1, "trace-1"); err == nil {
		t.Fatal("nil codec error = nil")
	}
	if err := codec.SignEnvelope(nil, "assistant", 1, "trace-1"); err == nil {
		t.Fatal("nil envelope error = nil")
	}
	base := Envelope{
		Version: CurrentEnvelopeVersion, EventID: "message-1", TenantID: "support",
		SessionKey: "support/assistant/session/session-1", Type: InboundMessageType, Payload: []byte(`{"ok":true}`),
	}
	for _, test := range []struct {
		name    string
		appCode string
		version uint64
		traceID string
	}{
		{"empty app", "", 1, "trace-1"},
		{"zero version", "assistant", 0, "trace-1"},
		{"empty trace", "assistant", 1, ""},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			envelope := base
			if err := codec.SignEnvelope(&envelope, test.appCode, test.version, test.traceID); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("SignEnvelope() error = %v", err)
			}
		})
	}
}

func TestExecutionManifestValidateFieldsRejectsMalformedDigestAndTimes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC)
	valid := ExecutionManifest{
		KeyID: "key", TenantID: "support", AppCode: "assistant",
		SessionKey: "support/assistant/session/session-1", MessageID: "message-1", TraceID: "trace-1",
		ConfigVersion: 1, PayloadSHA256: strings.Repeat("a", 64), IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := valid.validateFields(); err != nil {
		t.Fatalf("valid manifest error = %v", err)
	}
	invalidDigest := valid
	invalidDigest.PayloadSHA256 = strings.Repeat("z", 64)
	if err := invalidDigest.validateFields(); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("invalid digest error = %v", err)
	}
	invalidExpiry := valid
	invalidExpiry.ExpiresAt = now
	if err := invalidExpiry.validateFields(); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("invalid expiry error = %v", err)
	}
	missing := valid
	missing.TraceID = ""
	if err := missing.validateFields(); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("missing field error = %v", err)
	}
}

func TestExecutionManifestVerifyRejectsClockSkewAndEnvelopeIdentityChanges(t *testing.T) {
	t.Parallel()
	codec := testExecutionManifestCodec(t)
	now := time.Date(2026, 9, 11, 1, 0, 0, 0, time.UTC)
	codec.now = func() time.Time { return now }
	envelope := Envelope{
		Version: CurrentEnvelopeVersion, EventID: "message-1", TenantID: "support",
		SessionKey: "support/assistant/session/session-1", Type: InboundMessageType, Payload: []byte(`{"ok":true}`),
	}
	if err := codec.SignEnvelope(&envelope, "assistant", 1, "trace-1"); err != nil {
		t.Fatal(err)
	}
	var nilCodec *ExecutionManifestCodec
	if err := nilCodec.VerifyEnvelope(envelope); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("nil codec error = %v", err)
	}
	withoutManifest := envelope
	withoutManifest.Manifest = nil
	if err := codec.VerifyEnvelope(withoutManifest); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("missing manifest error = %v", err)
	}
	changedTenant := envelope
	changedTenant.TenantID = "other"
	if err := codec.VerifyEnvelope(changedTenant); !errors.Is(err, ErrManifestEnvelopeMismatch) {
		t.Fatalf("tenant mismatch error = %v", err)
	}

	codec.now = func() time.Time { return now.Add(-2 * time.Minute) }
	if err := codec.VerifyEnvelope(envelope); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("future-issued manifest error = %v", err)
	}
}
