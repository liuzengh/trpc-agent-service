package executionhttp

import (
	"context"
	"encoding/json"
	"errors"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExecutionVerifierClassifiesTransportAndOwnerDenial(t *testing.T) {
	for _, status := range []int{200, 403, 401, 404, 503, 302} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != executionv1.AttemptVerifyPath {
					t.Error("wrong path")
				}
				w.Header().Set("Location", "https://leak.invalid/")
				w.WriteHeader(status)
				if status == 200 {
					json.NewEncoder(w).Encode(executionv1.AttemptResponse{TenantID: "tenant", ProfileID: "profile", ProfileRevisionNumber: 2, RunID: "run", AttemptID: "attempt", WorkerID: "worker", LeaseEpoch: 1, ExpiresAt: time.Now().Add(time.Minute), ManifestID: "manifest", ManifestDigest: "sha256:" + strings.Repeat("a", 64), AllowedUses: []executionv1.CredentialUse{}})
				}
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = time.Second
			v, err := New(client, server.URL)
			if err != nil {
				t.Fatal(err)
			}
			result, err := v.VerifyAttempt(context.Background(), application.ExecutionAuthorizationRequest{WorkloadIdentity: "worker", ExecutionToken: "opaque", ManifestID: "manifest", ManifestDigest: "sha256:" + strings.Repeat("a", 64)})
			if calls != 1 {
				t.Fatalf("HTTP retries %d", calls)
			}
			switch status {
			case 200:
				if err != nil || result.ProfileRevisionNumber != 2 {
					t.Fatalf("result %#v %v", result, err)
				}
			case 403:
				if !errors.Is(err, application.ErrExecutionUnauthorized) {
					t.Fatal(err)
				}
			default:
				if !errors.Is(err, application.ErrExecutionDependencyUnavailable) {
					t.Fatal(err)
				}
			}
		})
	}
}
