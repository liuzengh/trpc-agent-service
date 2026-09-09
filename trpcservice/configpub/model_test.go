package configpub

import (
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func validDocument() Document {
	return Document{
		SchemaVersion: 1,
		BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "none"},
		Agent:         AgentDocument{AgentAppID: "agent-1", ModelConfigRef: "env", ToolPolicyRef: "default"},
		Bindings: []BindingDocument{
			{BindingID: "binding-1", Channel: tenant.ChannelLark, ExternalAppID: "app-1", SecretRef: "env://LARK_SECRET"},
		},
	}
}

func TestDocumentValidateAcceptsWellFormed(t *testing.T) {
	doc := validDocument()
	if err := doc.validateShape(); err != nil {
		t.Fatalf("well-formed document rejected: %v", err)
	}
	raw, err := EncodeDocument(doc)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decoded, err := DecodeDocument(raw)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	fingerprint, err := Fingerprint(decoded)
	if err != nil {
		t.Fatalf("fingerprint failed: %v", err)
	}
	if !strings.HasPrefix(fingerprint, "sha256:") || len(fingerprint) != len("sha256:")+16 {
		t.Fatalf("fingerprint is not bounded: %q", fingerprint)
	}
	again, _ := Fingerprint(doc)
	if fingerprint != again {
		t.Fatalf("fingerprint is not deterministic")
	}
}

func TestDocumentValidationCategories(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*Document)
		category string
	}{
		{"unsupported schema version", func(d *Document) { d.SchemaVersion = 99 }, CategoryIncompatibleVer},
		{"empty backend", func(d *Document) { d.BackendPolicy.Memory = "" }, CategoryMissingSetting},
		{"unsupported session backend", func(d *Document) { d.BackendPolicy.Session = "oracle" }, CategoryUnsupportedBackend},
		{"vector backend enabled", func(d *Document) { d.BackendPolicy.Vector = "milvus" }, CategoryUnsupportedBackend},
		{"invalid agent id", func(d *Document) { d.Agent.AgentAppID = "bad id!" }, CategorySchemaInvalid},
		{"plaintext secret", func(d *Document) { d.Bindings[0].SecretRef = "super-secret-value" }, CategoryInvalidSecretRef},
		{"unsupported secret scheme", func(d *Document) { d.Bindings[0].SecretRef = "vault://x" }, CategoryInvalidSecretRef},
		{"duplicate binding", func(d *Document) { d.Bindings = append(d.Bindings, d.Bindings[0]) }, CategorySchemaInvalid},
		{"unknown channel", func(d *Document) { d.Bindings[0].Channel = "wechat" }, CategorySchemaInvalid},
		{"oversized ref", func(d *Document) { d.Agent.ModelConfigRef = strings.Repeat("x", 300) }, CategorySchemaInvalid},
		{"message target with id", func(d *Document) { d.Bindings[0].TargetType = "message"; d.Bindings[0].TargetID = "x" }, CategorySchemaInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := validDocument()
			tc.mutate(&doc)
			err := doc.validateShape()
			if err == nil {
				t.Fatalf("expected rejection")
			}
			if got := CategoryOf(err); got != tc.category {
				t.Fatalf("category = %q, want %q", got, tc.category)
			}
		})
	}
}

func TestStatusRuntimeReadable(t *testing.T) {
	for status, want := range map[Status]bool{
		StatusDraft: false, StatusRejected: false,
		StatusPublished: true, StatusSuperseded: true, StatusRecalled: true,
	} {
		if status.RuntimeReadable() != want {
			t.Fatalf("status %s readable = %v", status, want)
		}
	}
}

func TestAssignmentBucketDeterministicAndBounded(t *testing.T) {
	first, err := AssignmentBucket("tenant-alpha")
	if err != nil {
		t.Fatalf("bucket failed: %v", err)
	}
	for i := 0; i < 100; i++ {
		bucket, err := AssignmentBucket("tenant-alpha")
		if err != nil || bucket != first {
			t.Fatalf("assignment not deterministic: %d vs %d (err=%v)", bucket, first, err)
		}
	}
	for i := 0; i < 500; i++ {
		bucket, err := AssignmentBucket("tenant-" + strings.Repeat("x", 1) + string(rune('a'+i%26)) + "-" + time.Now().Format("150405") + "-" + strings.Repeat("y", i%7))
		_ = bucket
		if err != nil {
			t.Fatalf("bucket rejected valid id: %v", err)
		}
	}
	if _, err := AssignmentBucket(""); err == nil {
		t.Fatalf("empty tenant id must fail closed")
	}
	if _, err := AssignmentBucket(strings.Repeat("x", 200)); err == nil {
		t.Fatalf("oversized tenant id must fail closed")
	}
	if _, err := AssignmentBucket("bad id!"); err == nil {
		t.Fatalf("invalid tenant id must fail closed")
	}
}

func TestRolloutAssignBoundaries(t *testing.T) {
	state := RolloutState{ActiveVersion: 5, BaselineVersion: 4, Percentage: 30}
	if version, ok := state.Assign(29); !ok || version != 5 {
		t.Fatalf("bucket 29 should get active: %d %v", version, ok)
	}
	if version, ok := state.Assign(30); !ok || version != 4 {
		t.Fatalf("bucket 30 should get baseline: %d %v", version, ok)
	}
	full := RolloutState{ActiveVersion: 5, BaselineVersion: 4, Percentage: 100}
	if version, ok := full.Assign(99); !ok || version != 5 {
		t.Fatalf("percentage 100 must always serve active")
	}
	zero := RolloutState{ActiveVersion: 5, BaselineVersion: 4, Percentage: 0}
	if version, ok := zero.Assign(0); !ok || version != 4 {
		t.Fatalf("percentage 0 must always serve baseline")
	}
	noBaseline := RolloutState{ActiveVersion: 5, Percentage: 30}
	// Defense in depth: publishing a partial rollout without a baseline is
	// rejected at the repository, and assignment fails closed for every
	// bucket if such unsafe state ever existed.
	if _, ok := noBaseline.Assign(50); ok {
		t.Fatalf("out-of-bucket tenant without baseline must fail closed")
	}
	if _, ok := noBaseline.Assign(10); ok {
		t.Fatalf("in-bucket tenant without baseline must fail closed")
	}
	if _, ok := state.Assign(100); ok {
		t.Fatalf("out-of-range bucket must fail closed")
	}
	if _, ok := state.Assign(-1); ok {
		t.Fatalf("negative bucket must fail closed")
	}
	if _, ok := (RolloutState{Percentage: 100}).Assign(0); ok {
		t.Fatalf("missing active revision must fail closed")
	}
}

func TestFingerprintChangesWithContent(t *testing.T) {
	doc := validDocument()
	a, _ := Fingerprint(doc)
	doc.Agent.ToolPolicyRef = "strict"
	b, _ := Fingerprint(doc)
	if a == b {
		t.Fatalf("fingerprint must change with content")
	}
}

func TestValidationErrorCategoryExtraction(t *testing.T) {
	err := validationError(CategoryCrossTenantRef)
	if CategoryOf(err) != CategoryCrossTenantRef {
		t.Fatalf("category extraction failed")
	}
	if CategoryOf(nil) != "" {
		t.Fatalf("nil error must have empty category")
	}
}
