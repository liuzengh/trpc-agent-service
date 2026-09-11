package trpcagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Manifest execution.max_output_tokens is a positive int64; the separate
// node generation field's 262144 schema maximum is not its policy maximum.
// The manifest reader validates the node contract before constructing Request.
func TestExecutorPreservesPublishedOutputLimitWithoutPrivateCeiling(t *testing.T) {
	for _, override := range []int64{0, 18000} {
		name := "published-default"
		if override != 0 {
			name = "node-override"
		}
		t.Run(name, func(t *testing.T) {
			requests := make(chan map[string]any, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var value map[string]any
				if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
					t.Error(err)
				}
				requests <- value
				writeAnswer(w, "published limit preserved")
			}))
			defer server.Close()
			req := testRequest(server.URL)
			req.MaxOutputTokens = 300000
			want := req.MaxOutputTokens
			if override != 0 {
				req.Model.MaxOutputTokens = &override
				want = override
			}
			result, err := testExecutor().Execute(context.Background(), req)
			if err != nil {
				t.Fatalf("published execution limit=%d node=%d incorrectly rejected before SDK: %v", req.MaxOutputTokens, override, err)
			}
			if result.FinalText != "published limit preserved" || len(result.Snapshot) == 0 {
				t.Fatalf("valid final not preserved: %+v", result)
			}
			if len(requests) != 1 {
				t.Fatalf("SDK requests=%d, want one", len(requests))
			}
			actual := <-requests
			if actual["max_completion_tokens"] != float64(want) {
				t.Fatalf("published limit silently changed: actual=%v want=%d", actual["max_completion_tokens"], want)
			}
			t.Logf("PUBLISHED_OUTPUT_LIMIT=PASS execution=300000 node=%d sdk_max_completion_tokens=%d sdk_calls=1", override, want)
		})
	}
}
