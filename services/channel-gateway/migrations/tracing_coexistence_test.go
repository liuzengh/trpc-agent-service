package migrations_test

import (
	"crypto/sha256"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
	"testing"
)

// These migrations have already been published independently. Full filenames,
// not numeric prefixes, identify immutable ledger entries; do not renumber them.
func TestPublishedTracingAndAuthorizationMigrationsCoexist(t *testing.T) {
	for name, want := range map[string]string{
		"0012_access_policy_projection.sql": "fd041197c717180401e39ebfc16c79f4da2ac414500b313441097cbed27a6682",
		"0012_admission_trace.sql":          "821821cd649a57a49e586609924af6f2209703f8cc55480660157b09b4a42264",
		"0013_policy_source_identity.sql":   "97d86d2cc626e69ac824436632c41309457e45550adb06065bdfc3fac4389f64",
		"0013_delivery_trace.sql":           "0cfe84eaef559ac9fb48e4ac4756a4c287a9e330c97a234b7979340ac1b3bd66",
	} {
		body, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(body)); got != want {
			t.Fatalf("published migration %s changed: %s", name, got)
		}
	}
}

func TestLegacyFactsIgnoreOnlyNullTraceMetadata(t *testing.T) {
	for _, table := range []string{"gateway_outbox", "gateway_delivery_intents"} {
		before, err := legacyFactsWithoutNullTrace(table, `[{"payload":"original"}]`)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			after string
			same  bool
		}{
			{`[{"payload":"original","traceparent":null,"tracestate":null}]`, true},
			{`[{"payload":"original","traceparent":"backfilled","tracestate":null}]`, false},
			{`[{"payload":"original","traceparent":null,"tracestate":"backfilled"}]`, false},
			{`[{"payload":"changed","traceparent":null,"tracestate":null}]`, false},
			{`[{"payload":"original","unexpected":null}]`, false},
		} {
			after, err := legacyFactsWithoutNullTrace(table, tc.after)
			if err != nil || (after == before) != tc.same {
				t.Fatalf("table=%s input=%s same=%v err=%v", table, tc.after, after == before, err)
			}
		}
	}
}
