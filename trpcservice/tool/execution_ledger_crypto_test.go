package tool

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestPostgresExecutionLedgerRequiresDatabaseAndStrongKeyMaterial(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte("k"), 32)
	if _, err := NewPostgresExecutionLedger(nil, key); err == nil {
		t.Fatal("NewPostgresExecutionLedger accepted nil database")
	}
	if _, err := NewPostgresExecutionLedger(&sql.DB{}, []byte("short")); err == nil {
		t.Fatal("NewPostgresExecutionLedger accepted short key material")
	}
	ledger, err := NewPostgresExecutionLedger(&sql.DB{}, key)
	if err != nil || ledger == nil {
		t.Fatalf("NewPostgresExecutionLedger() = %#v, %v", ledger, err)
	}
}

func TestToolResultEncryptionRoundTripAndTamperDetection(t *testing.T) {
	t.Parallel()
	var key [32]byte
	copy(key[:], bytes.Repeat([]byte{0x42}, len(key)))
	plaintext := []byte("{\"approved\":true,\"reference\":\"refund-42\"}")
	first, err := encryptToolResult(key, plaintext)
	if err != nil {
		t.Fatalf("encryptToolResult() error = %v", err)
	}
	second, err := encryptToolResult(key, plaintext)
	if err != nil {
		t.Fatalf("second encryptToolResult() error = %v", err)
	}
	if bytes.Equal(first, plaintext) || bytes.Equal(first, second) {
		t.Fatal("tool result encryption exposed plaintext or reused a nonce")
	}
	decrypted, err := decryptToolResult(key, first)
	if err != nil || !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decryptToolResult() = %q, %v", decrypted, err)
	}

	tampered := append([]byte(nil), first...)
	tampered[len(tampered)-1] ^= 0xff
	if _, err := decryptToolResult(key, tampered); err == nil {
		t.Fatal("tampered tool result ciphertext was accepted")
	}
	otherKey := key
	otherKey[0] ^= 0xff
	if _, err := decryptToolResult(otherKey, first); err == nil {
		t.Fatal("tool result encrypted under another key was accepted")
	}
	if _, err := decryptToolResult(key, []byte("short")); err == nil {
		t.Fatal("truncated tool result ciphertext was accepted")
	}
}

func TestExecutionRequestValidationAndImplicitToolCallID(t *testing.T) {
	t.Parallel()
	base := ExecutionRequest{
		TenantID: "tenant-a", RequestID: "request-1", ToolName: "support.lookup",
		Arguments: []byte(`{"order_id":"42"}`), TraceID: "trace-1", LeaseTTL: time.Minute,
	}
	if err := validateExecutionRequest(base); err != nil {
		t.Fatalf("validateExecutionRequest(valid) = %v", err)
	}
	for _, mutate := range []func(*ExecutionRequest){
		func(r *ExecutionRequest) { r.TenantID = "" },
		func(r *ExecutionRequest) { r.RequestID = "" },
		func(r *ExecutionRequest) { r.ToolName = "" },
		func(r *ExecutionRequest) { r.TraceID = "" },
		func(r *ExecutionRequest) { r.LeaseTTL = 0 },
	} {
		request := base
		mutate(&request)
		if err := validateExecutionRequest(request); err == nil {
			t.Fatalf("validateExecutionRequest(%#v) error = nil", request)
		}
	}
	implicit := normalizedToolCallID(base)
	if !strings.HasPrefix(implicit, "implicit-") || implicit != normalizedToolCallID(base) {
		t.Fatalf("normalizedToolCallID() = %q", implicit)
	}
	explicit := base
	explicit.ToolCallID = " call-123 "
	if got := normalizedToolCallID(explicit); got != "call-123" {
		t.Fatalf("normalizedToolCallID(explicit) = %q", got)
	}
}

func TestPostgresExecutionLedgerCompletionFailsClosedBeforeOrAtDatabase(t *testing.T) {
	t.Parallel()
	database, err := sql.Open("pgx", "postgres://unused:unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err := NewPostgresExecutionLedger(database, bytes.Repeat([]byte("k"), 32))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := ledger.Complete(ctx, "tenant-a", "key-a", func() {}); err == nil || !strings.Contains(err.Error(), "encode tool result") {
		t.Fatalf("Complete(unencodable) error = %v", err)
	}
	if err := ledger.Complete(ctx, "tenant-a", "key-a", map[string]any{"ok": true}); err == nil || !strings.Contains(err.Error(), "begin tool execution completion") {
		t.Fatalf("Complete(closed DB) error = %v", err)
	}
	if err := ledger.Fail(ctx, "", "key-a", "failure"); err == nil || !strings.Contains(err.Error(), "identity is required") {
		t.Fatalf("Fail(missing tenant) error = %v", err)
	}
	if err := ledger.MarkOutcomeUnknown(ctx, "tenant-a", "", "timeout"); err == nil || !strings.Contains(err.Error(), "identity is required") {
		t.Fatalf("MarkOutcomeUnknown(missing key) error = %v", err)
	}
	if err := ledger.Fail(ctx, "tenant-a", "key-a", "failure"); err == nil || !strings.Contains(err.Error(), "begin tool execution completion") {
		t.Fatalf("Fail(closed DB) error = %v", err)
	}
	if err := ledger.MarkOutcomeUnknown(ctx, "tenant-a", "key-a", "timeout"); err == nil || !strings.Contains(err.Error(), "begin tool execution completion") {
		t.Fatalf("MarkOutcomeUnknown(closed DB) error = %v", err)
	}
}
