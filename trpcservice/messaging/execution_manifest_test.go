package messaging

import (
	"errors"
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
