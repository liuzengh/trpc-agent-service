package executionhttp

import (
	"context"
	"encoding/json"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFinalVerifierExactProofAndHTTPFailure(t *testing.T) {
	in := executionv1.FinalRequest{IntentID: "intent", Digest: "sha256:" + strings.Repeat("a", 64), AdmissionID: "admission", RunID: "run", AttemptID: "attempt", CompletionID: "completion", ExecutionGeneration: 1, Sequence: 1}
	for _, mode := range []string{"ok", "mismatch", "denied", "redirect", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != executionv1.FinalVerifyPath {
					t.Error("wrong endpoint")
				}
				if mode == "denied" {
					w.WriteHeader(403)
					return
				}
				if mode == "redirect" {
					w.Header().Set("Location", "https://other.invalid")
					w.WriteHeader(302)
					return
				}
				if mode == "malformed" {
					w.Write([]byte(`{}`))
					return
				}
				p := executionv1.FinalResponse{FinalRequest: in, TenantID: "tenant", ManifestDigest: in.Digest}
				if mode == "mismatch" {
					p.CompletionID = "other"
				}
				json.NewEncoder(w).Encode(p)
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = time.Second
			v, e := New(client, server.URL)
			if e != nil {
				t.Fatal(e)
			}
			_, e = v.VerifyFinal(context.Background(), in)
			if (e == nil) != (mode == "ok") || calls != 1 {
				t.Fatal(mode, e, calls)
			}
		})
	}
}
