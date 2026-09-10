package log

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/trace"
)

func decodeRecord(t *testing.T, buffer *bytes.Buffer) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &result); err != nil {
		t.Fatalf("decode JSON log: %v output=%q", err, buffer.String())
	}
	return result
}

func TestLoggerEmitsStructuredRedactedRecord(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Writer: &output, Level: LevelInfo, MaskingLevel: MaskBasic, Role: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	secret := "canary-secret-value"
	logger.Error(context.Background(), "delivery failed authorization=Bearer "+secret,
		String("tenant_id", "tenant-a"), String("authorization", "Bearer "+secret),
		Error(errors.New("postgres://app:"+secret+"@db.example/test")))
	if strings.Contains(output.String(), secret) {
		t.Fatalf("secret leaked in log output: %s", output.String())
	}
	record := decodeRecord(t, &output)
	if record["level"] != "ERROR" || record["role"] != "worker" || record["tenant_id"] != "tenant-a" {
		t.Fatalf("unexpected structured record: %#v", record)
	}
	if record["authorization"] != "[REDACTED]" {
		t.Fatalf("authorization was not redacted: %#v", record)
	}
}

func TestLoggerHonorsLevelAndAddsTraceCoordinates(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Writer: &output, Level: LevelInfo, MaskingLevel: MaskNone, Role: "gateway"})
	if err != nil {
		t.Fatal(err)
	}
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}))
	logger.Debug(ctx, "not emitted")
	logger.Info(ctx, "accepted", String("request_id", "request-a"))
	record := decodeRecord(t, &output)
	if record["trace_id"] != traceID.String() || record["span_id"] != spanID.String() || record["request_id"] != "request-a" {
		t.Fatalf("missing trace coordinates: %#v", record)
	}
}

func TestStrictMaskingAndInvalidConfiguration(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Writer: &output, Level: LevelInfo, MaskingLevel: MaskStrict, Role: "channel"})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info(context.Background(), "callback", String("external_user_id", "user-secret"), String("safe_count", "3"))
	record := decodeRecord(t, &output)
	if record["external_user_id"] != "[REDACTED]" || record["safe_count"] != "3" {
		t.Fatalf("strict masking mismatch: %#v", record)
	}
	for _, config := range []Config{
		{Writer: &output, Level: "verbose", MaskingLevel: MaskBasic, Role: "worker"},
		{Writer: &output, Level: LevelInfo, MaskingLevel: "unsafe", Role: "worker"},
		{Writer: &output, Level: LevelInfo, MaskingLevel: MaskBasic, Role: "bad\nrole"},
	} {
		if _, err := New(config); err == nil {
			t.Fatalf("expected invalid configuration rejection: %#v", config)
		}
	}
}

func TestLoggerUsesCompiledCustomRedactionRules(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{
		Writer: &output, Level: LevelInfo, MaskingLevel: MaskBasic, Role: "worker",
		RedactionRules: []redaction.Rule{{ID: "case-reference", KeyFragments: []string{"case_ref"}, TextPattern: `CASE-[0-9]{6}`}},
	})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info(context.Background(), "processing CASE-123456", String("case-ref", "CASE-654321"))
	if strings.Contains(output.String(), "CASE-") {
		t.Fatalf("custom rule leaked a reference: %s", output.String())
	}
	record := decodeRecord(t, &output)
	if record["case-ref"] != redaction.Replacement {
		t.Fatalf("custom key rule was not applied: %#v", record)
	}
}

func TestNewForTenantUsesVersionedPolicy(t *testing.T) {
	var output bytes.Buffer
	logger, err := NewForTenant(Config{Writer: &output, Level: LevelInfo, Role: "worker"}, tenant.Tenant{
		LogMaskingLevel: tenant.MaskingBasic,
		RedactionRules:  []redaction.Rule{{ID: "case", TextPattern: `CASE-[0-9]{6}`}},
	})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info(context.Background(), "CASE-123456")
	if strings.Contains(output.String(), "CASE-123456") {
		t.Fatalf("tenant policy was not used: %s", output.String())
	}
}

func TestLoggerUsesTenantPolicyBoundToContext(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Writer: &output, Level: LevelInfo, Role: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := (tenant.Tenant{LogMaskingLevel: tenant.MaskingBasic, RedactionRules: []redaction.Rule{{ID: "case", TextPattern: `CASE-[0-9]{6}`}}}).ContextWithRedaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	logger.Info(ctx, "CASE-123456")
	if strings.Contains(output.String(), "CASE-123456") {
		t.Fatalf("context policy was not used: %s", output.String())
	}
}

func TestNewFromEnvRejectsInvalidValues(t *testing.T) {
	var output bytes.Buffer
	if _, err := NewFromEnv(func(string) string { return "verbose" }, &output, "worker"); err == nil {
		t.Fatal("expected invalid level")
	}
	if _, err := NewFromEnv(func(name string) string {
		if name == "TRPC_LOG_MASKING_LEVEL" {
			return "unsafe"
		}
		return ""
	}, &output, "worker"); err == nil {
		t.Fatal("expected invalid masking level")
	}
}
