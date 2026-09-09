package persistence

import (
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestEnvelopeDigestRejectsTampering(t *testing.T) {
	task := validTask()
	fingerprint := BackendFingerprint{SchemaVersion: FingerprintSchemaVersion, Kind: tenant.StorageKindPostgres, StorageProfileID: "pg", DatabaseIdentity: "db.example:5432/app", Namespace: "public.tenant_"}
	route := Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: fingerprint}
	commit := sessionfence.TurnCommit{SessionCoord: "coord", SessionSeq: 1, AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID, FinalState: map[string][]byte{"key": []byte("value")}}
	reply := message.OutboundMessage{RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "ok"}
	envelope, err := NewEnvelope(task, route, commit, reply, time.Unix(123, 456))
	if err != nil {
		t.Fatal(err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	retry := envelope
	retry.PersistAttempt++
	if digest, err := retry.CalculateDigest(); err != nil || digest != envelope.EnvelopeDigest {
		t.Fatalf("retry bookkeeping changed envelope digest: (%s,%v)", digest, err)
	}
	envelope.Reply.Text = "tampered"
	if err := envelope.Validate(); err != ErrInvalidEnvelope {
		t.Fatalf("tampered envelope error = %v", err)
	}
}

func TestFingerprintDigestIgnoresCredentialRotation(t *testing.T) {
	first := BackendFingerprint{SchemaVersion: FingerprintSchemaVersion, Kind: tenant.StorageKindMySQL, StorageProfileID: "mysql", DatabaseIdentity: "db.example:3306/app", Namespace: "tenant_"}
	second := first
	a, err := first.Digest()
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("equivalent fingerprints differ: %s != %s", a, b)
	}
	second.Namespace = "other_"
	b, _ = second.Digest()
	if a == b {
		t.Fatal("namespace change did not alter fingerprint")
	}
}

func TestFingerprintForProfileIgnoresCredentialsAndValidatesMySQLUTC(t *testing.T) {
	profile := tenant.StorageProfile{TenantID: "tenant", ID: "mysql", Kind: tenant.StorageKindMySQL, CredentialRef: "env:MYSQL", TablePrefix: "tenant"}
	first, err := FingerprintForProfile(profile, "user:secret-one@tcp(db.example:3306)/app?parseTime=true&charset=utf8mb4&loc=UTC")
	if err != nil {
		t.Fatal(err)
	}
	second, err := FingerprintForProfile(profile, "rotated:secret-two@tcp(db.example:3306)/app?parseTime=true&charset=utf8mb4&loc=UTC&tls=preferred")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("credential rotation changed fingerprint: %#v != %#v", first, second)
	}
	if _, err := FingerprintForProfile(profile, "user:secret@tcp(db.example:3306)/app?parseTime=false&charset=utf8mb4&loc=UTC"); err == nil {
		t.Fatal("MySQL DSN without parseTime unexpectedly accepted")
	}
	if _, err := FingerprintForProfile(profile, "user:secret@tcp(db.example:3306)/app?parseTime=true&charset=utf8mb4"); err == nil {
		t.Fatal("MySQL DSN without explicit loc=UTC unexpectedly accepted")
	}
}

func TestFingerprintForPostgresIgnoresUserPasswordAndTLS(t *testing.T) {
	profile := tenant.StorageProfile{TenantID: "tenant", ID: "pg", Kind: tenant.StorageKindPostgres, CredentialRef: "env:PG", TablePrefix: "tenant", Schema: "app"}
	first, err := FingerprintForProfile(profile, "postgres://user:secret@db.example:5432/app?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	second, err := FingerprintForProfile(profile, "postgres://rotated:other@db.example:5432/app?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("credential/TLS rotation changed fingerprint: %#v != %#v", first, second)
	}
}

func validTask() message.ExecutionTask {
	task := message.ExecutionTask{
		SchemaVersion: message.TaskSchemaVersion, TaskID: "task-1", Channel: "demo", ChannelBindingID: "binding",
		ExternalAccountID: "account", TenantID: "tenant", AgentAppID: "agent", ConfigVersion: "v1",
		RunnerUserID: "user", SessionID: "session", PlatformMessageID: "message", ActorUserID: "actor",
		ConversationID: "conversation", ConversationType: message.ConversationDirect, Text: "hello",
		RequestID: "request", TraceID: "trace", ReceivedAt: time.Unix(100, 0), Attempt: 1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	return task
}
