package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ModelError marks model-side failures; its Error/Unwrap must survive
// errors.Is / errors.As unwrapping.
func TestModelErrorErrorAndUnwrap(t *testing.T) {
	base := errors.New("deadline hit")
	e := &ModelError{Err: base}
	if e.Error() != base.Error() {
		t.Fatalf("Error() = %q, want %q", e.Error(), base.Error())
	}
	if !errors.Is(e, base) {
		t.Fatal("errors.Is must unwrap ModelError to its cause")
	}
	if e.Unwrap() != base {
		t.Fatal("Unwrap must return the cause")
	}
}

func TestInfraErrorErrorAndUnwrap(t *testing.T) {
	base := errors.New("session store down")
	e := &infraError{Err: base}
	if e.Error() != base.Error() {
		t.Fatalf("Error() = %q, want %q", e.Error(), base.Error())
	}
	if !errors.Is(e, base) {
		t.Fatal("errors.Is must unwrap infraError to its cause")
	}
}

func TestKeyErrorErrorAndUnwrap(t *testing.T) {
	base := errors.New("no secret")
	e := &keyError{Err: base}
	if e.Error() != base.Error() {
		t.Fatalf("Error() = %q, want %q", e.Error(), base.Error())
	}
	if !errors.Is(e, base) {
		t.Fatal("errors.Is must unwrap keyError to its cause")
	}
}

func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("decision", "allow"); got != "decision" {
		t.Fatalf("firstNonEmpty non-empty = %q", got)
	}
	if got := firstNonEmpty("", "allow"); got != "allow" {
		t.Fatalf("firstNonEmpty fallback = %q", got)
	}
}

func TestTenantOrZero(t *testing.T) {
	if got := tenantOrZero(""); got != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("empty tenant must map to the zero UUID, got %q", got)
	}
	if got := tenantOrZero("t1"); got != "t1" {
		t.Fatalf("non-empty tenant must pass through, got %q", got)
	}
}

// signalDecision maps every interception kind to its audit decision.
func TestSignalDecisionMatrix(t *testing.T) {
	created := signalDecision(Signal{Kind: "created", ToolName: "op_a", Fresh: true})
	if created.decision != "review" || created.toolName != "op_a" {
		t.Fatalf("fresh created → review, got %+v", created)
	}
	if rehit := signalDecision(Signal{Kind: "created", ToolName: "op_a", Fresh: false}); rehit.decision != "" {
		t.Fatalf("non-fresh created must audit nothing, got %+v", rehit)
	}
	conflict := signalDecision(Signal{Kind: "conflict", ToolName: "op_b", Pending: "op_a"})
	if conflict.decision != "deny" || conflict.errorType != "approval_conflict" || conflict.toolName != "op_b" {
		t.Fatalf("conflict → deny/approval_conflict, got %+v", conflict)
	}
	timeout := signalDecision(Signal{Kind: "timeout", ToolName: "op_a"})
	if timeout.decision != "review_timeout" || timeout.toolName != "op_a" {
		t.Fatalf("timeout → review_timeout, got %+v", timeout)
	}
	if unknown := signalDecision(Signal{Kind: "bogus"}); unknown.decision != "" {
		t.Fatalf("unknown kind must audit nothing, got %+v", unknown)
	}
}

// signalReply composes the deterministic user-facing notices.
func TestSignalReplyMatrix(t *testing.T) {
	created := signalReply(Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"1"}`,
		Deadline: time.Now().Add(5 * time.Minute),
	})
	for _, want := range []string{"危险操作", "op_a", `{"x":"1"}`, "确认"} {
		if !strings.Contains(created, want) {
			t.Fatalf("created reply missing %q: %q", want, created)
		}
	}

	conflict := signalReply(Signal{Kind: "conflict", ToolName: "op_b", Pending: "op_a"})
	if !strings.Contains(conflict, "op_a") || !strings.Contains(conflict, "确认") {
		t.Fatalf("conflict reply must name the pending tool: %q", conflict)
	}

	timeout := signalReply(Signal{Kind: "timeout", ToolName: "op_a"})
	if !strings.Contains(timeout, "超时") || !strings.Contains(timeout, "op_a") {
		t.Fatalf("timeout reply must name the expired tool: %q", timeout)
	}

	if unknown := signalReply(Signal{Kind: "bogus"}); unknown != "" {
		t.Fatalf("unknown kind must yield an empty reply, got %q", unknown)
	}
}

// The zero Worker must fall back to the platform defaults.
func TestWorkerDefaults(t *testing.T) {
	w := &Worker{}
	if got := w.reapInterval(); got != 30*time.Second {
		t.Fatalf("default reap interval = %v", got)
	}
	if got := w.maxIdle(); got != 4*time.Minute {
		t.Fatalf("default max idle = %v", got)
	}
	if got := w.maxAttempts(); got != 5 {
		t.Fatalf("default max attempts = %v", got)
	}
	if got := w.lockTTL(); got != 10*time.Second {
		t.Fatalf("default lock TTL = %v", got)
	}
	if got := w.lockWait(); got != 15*time.Second {
		t.Fatalf("default lock wait = %v", got)
	}
	if got := w.drainTimeout(); got != 2*time.Minute {
		t.Fatalf("default drain timeout = %v", got)
	}
	if got := w.inStream(); got != storage.StreamInbound {
		t.Fatalf("default inbound stream = %v", got)
	}
	if got := w.outStream(); got != storage.StreamOutbound {
		t.Fatalf("default outbound stream = %v", got)
	}

	// Explicit settings win over the defaults.
	w2 := &Worker{
		ReapInterval: time.Second, MaxIdle: 2 * time.Second, MaxAttempts: 9,
		LockTTL: time.Second, LockWait: 2 * time.Second, DrainTimeout: time.Second,
		InStream: "in", OutStream: "out",
	}
	if w2.reapInterval() != time.Second || w2.maxIdle() != 2*time.Second ||
		w2.maxAttempts() != 9 || w2.lockTTL() != time.Second ||
		w2.lockWait() != 2*time.Second || w2.drainTimeout() != time.Second ||
		w2.inStream() != "in" || w2.outStream() != "out" {
		t.Fatal("explicit settings must override the defaults")
	}
}

func TestParseModelSpecInvalidJSON(t *testing.T) {
	// Invalid JSON degrades to the zero spec with a warning.
	if got := parseModelSpec(json.RawMessage(`{`)); got.Name != "" || got.BaseURL != "" {
		t.Fatalf("invalid model_config must degrade to zero, got %+v", got)
	}
	if got := parseModelSpec(nil); got.Name != "" {
		t.Fatalf("empty model_config must be zero, got %+v", got)
	}
}

func TestMergeModelAPIKeyRefOverride(t *testing.T) {
	got := mergeModel(
		ModelSpec{Name: "env", APIKeyRef: "env-ref"},
		ModelSpec{Name: "tenant", APIKeyRef: "tenant-ref"},
	)
	if got.APIKeyRef != "tenant-ref" {
		t.Fatalf("APIKeyRef override ignored: %+v", got)
	}
}

func TestMigrationFingerprint(t *testing.T) {
	if got := migrationFingerprint(nil); got != nil {
		t.Fatalf("no migration must fingerprint to nil, got %q", got)
	}
	m := &tenant.Migration{FromBackend: "redis", ToBackend: "postgres", Phase: tenant.PhaseBackfilling}
	got := string(migrationFingerprint(m))
	if got != "redis>postgres@"+string(tenant.PhaseBackfilling) {
		t.Fatalf("unexpected fingerprint %q", got)
	}
}

// DocSource exposes its metadata for tenant-scoped ingestion.
func TestDocSourceGetMetadata(t *testing.T) {
	s := &DocSource{DocName: "d", Content: "c", Metadata: map[string]any{"tenant_id": "t1"}}
	if s.Name() != "d" || s.Type() != "inline" {
		t.Fatalf("unexpected source identity: %s/%s", s.Name(), s.Type())
	}
	if got := s.GetMetadata(); got["tenant_id"] != "t1" {
		t.Fatalf("metadata mismatch: %v", got)
	}
	docs, err := s.ReadDocuments(context.Background())
	if err != nil || len(docs) != 1 {
		t.Fatalf("ReadDocuments: %v, %d docs", err, len(docs))
	}
	if docs[0].Name != "d" || docs[0].Content != "c" {
		t.Fatalf("unexpected document: %+v", docs[0])
	}
	if docs[0].ID == "" {
		t.Fatal("document ID must be the content hash")
	}
}
