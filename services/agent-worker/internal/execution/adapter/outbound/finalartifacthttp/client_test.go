package finalartifacthttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func credentialFixture() (Request, []domain.CredentialUse, batch) {
	digest := domain.Digest([]byte("manifest"))
	audience := domain.Digest([]byte("artifact-backend"))
	r := Request{Final: proof.FinalRequest{IntentID: "intent", Digest: domain.Digest([]byte("Final bytes, not Manifest")), AdmissionID: "admission", RunID: "run", AttemptID: "completed-attempt", CompletionID: "completion", ExecutionGeneration: 2, Sequence: 1}, TenantID: "tenant", ManifestID: "manifest", ManifestDigest: digest, DeploymentID: "deployment", DeploymentRevisionID: "revision", ProfileID: "profile", ProfileRevisionNumber: 3}
	uses := []domain.CredentialUse{{CredentialID: "crd_access", Purpose: "access_key_id", AudienceDigest: audience}, {CredentialID: "crd_secret", Purpose: "secret_access_key", AudienceDigest: audience}}
	b := batch{TenantID: r.TenantID, ProfileID: r.ProfileID, ProfileRevision: 3, RunID: "run", AttemptID: "completed-attempt", WorkerID: "worker-one", LeaseEpoch: 0, ManifestID: "manifest", ManifestDigest: digest, Credentials: []credential{{CredentialID: "crd_access", Purpose: "access_key_id", AudienceDigest: audience, Revision: 2, Value: "private-access"}, {CredentialID: "crd_secret", Purpose: "secret_access_key", AudienceDigest: audience, Revision: 7, Value: "private-secret"}}}
	return r, uses, b
}
func TestCompletedArtifactClientExactBatchWithoutLease(t *testing.T) {
	r, uses, b := credentialFixture()
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		if req.URL.Path != ResolvePath || req.Method != "POST" || req.Header.Get("Content-Type") != "application/json" {
			t.Error("wrong route")
		}
		raw, _ := io.ReadAll(req.Body)
		defer clear(raw)
		var got Request
		if json.Unmarshal(raw, &got) != nil || got != r {
			t.Error("changed committed identity")
		}
		if bytes.Contains(raw, []byte("execution_token")) || bytes.Contains(raw, []byte("lease_epoch")) || bytes.Contains(raw, []byte("uses")) {
			t.Error("active or caller-selected authorization")
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		b.Credentials[0], b.Credentials[1] = b.Credentials[1], b.Credentials[0]
		json.NewEncoder(w).Encode(b)
	}))
	defer server.Close()
	client, e := New(Options{BaseURL: server.URL, WorkerID: "worker-one", Client: server.Client(), Timeout: time.Second, MaxResponseBytes: 4096})
	if e != nil {
		t.Fatal(e)
	}
	got, e := client.Resolve(context.Background(), r, uses)
	if e != nil || got != (artifactstore.Credentials{AccessKeyID: "private-access", SecretAccessKey: "private-secret"}) || calls.Load() != 1 {
		t.Fatal("completed exact credentials", e, calls.Load())
	}
}
func TestCompletedArtifactClientRejectsWrongOrAmbiguousBatch(t *testing.T) {
	for _, mode := range []string{"tenant", "profile", "profile revision", "run", "attempt", "worker", "manifest", "manifest digest", "active lease", "missing", "extra", "duplicate use", "purpose", "audience", "credential revision", "empty", "newline", "unknown", "duplicate field", "case field", "null", "nested duplicate", "nested null", "missing epoch", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			r, uses, b := credentialFixture()
			switch mode {
			case "tenant":
				b.TenantID = "other"
			case "profile":
				b.ProfileID = "other"
			case "profile revision":
				b.ProfileRevision++
			case "run":
				b.RunID = "other"
			case "attempt":
				b.AttemptID = "other"
			case "worker":
				b.WorkerID = "other"
			case "manifest":
				b.ManifestID = "other"
			case "manifest digest":
				b.ManifestDigest = domain.Digest([]byte("other"))
			case "active lease":
				b.LeaseEpoch = 1
			case "missing":
				b.Credentials = b.Credentials[:1]
			case "extra":
				b.Credentials = append(b.Credentials, b.Credentials[0])
			case "duplicate use":
				b.Credentials[1] = b.Credentials[0]
			case "purpose":
				b.Credentials[0].Purpose = "api_key"
			case "audience":
				b.Credentials[0].AudienceDigest = domain.Digest([]byte("other"))
			case "credential revision":
				b.Credentials[0].Revision = 0
			case "empty":
				b.Credentials[0].Value = " "
			case "newline":
				b.Credentials[0].Value = "private\nsecret"
			}
			raw, _ := json.Marshal(b)
			text := string(raw)
			switch mode {
			case "unknown":
				text = strings.Replace(text, "{", `{"secret":"injected",`, 1)
			case "duplicate field":
				text = strings.Replace(text, "{", `{"tenant_id":"tenant",`, 1)
			case "case field":
				text = strings.Replace(text, `"tenant_id"`, `"Tenant_ID"`, 1)
			case "null":
				text = strings.Replace(text, `"lease_epoch":0`, `"lease_epoch":null`, 1)
			case "nested duplicate":
				text = strings.Replace(text, `"purpose":"access_key_id"`, `"purpose":"access_key_id","purpose":"access_key_id"`, 1)
			case "nested null":
				text = strings.Replace(text, `"credential_revision":2`, `"credential_revision":null`, 1)
			case "missing epoch":
				text = strings.Replace(text, `"lease_epoch":0,`, "", 1)
			case "oversize":
				text += strings.Repeat(" ", 8192)
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, text)
			}))
			defer server.Close()
			client, _ := New(Options{BaseURL: server.URL, WorkerID: "worker-one", Client: server.Client(), Timeout: time.Second, MaxResponseBytes: 4096})
			got, e := client.Resolve(context.Background(), r, uses)
			if !errors.Is(e, ErrDenied) || got != (artifactstore.Credentials{}) {
				t.Fatal("invalid batch exposed credentials", e)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type closeBuffer struct {
	*bytes.Reader
	closed bool
}

func (b *closeBuffer) Close() error { b.closed = true; return nil }
func TestCompletedArtifactClientFailureClosesResponseWithoutSecretErrors(t *testing.T) {
	for _, status := range []int{200, 403, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			r, uses, _ := credentialFixture()
			body := &closeBuffer{Reader: bytes.NewReader([]byte("private diagnostic"))}
			borrowed := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
			})}
			client, _ := New(Options{BaseURL: "https://control.invalid", WorkerID: "worker-one", Client: borrowed, Timeout: time.Second, MaxResponseBytes: 4096})
			out, e := client.Resolve(context.Background(), r, uses)
			if e == nil || !body.closed || out != (artifactstore.Credentials{}) || strings.Contains(e.Error(), "private") {
				t.Fatal("failure lifecycle", e, body.closed)
			}
			want := ErrDenied
			if status == 429 || status >= 500 {
				want = ErrUnavailable
			}
			if !errors.Is(e, want) {
				t.Fatal(e)
			}
		})
	}
}
func TestCompletedArtifactClientNeverFollowsRedirect(t *testing.T) {
	var calls atomic.Int64
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, server.URL+"/redirect", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	r, uses, _ := credentialFixture()
	borrowed := server.Client()
	client, _ := New(Options{BaseURL: server.URL, WorkerID: "worker-one", Client: borrowed, Timeout: time.Second, MaxResponseBytes: 4096})
	if _, e := client.Resolve(context.Background(), r, uses); !errors.Is(e, ErrDenied) || calls.Load() != 1 || borrowed.CheckRedirect != nil {
		t.Fatal("redirect/borrowed client changed", e, calls.Load())
	}
}
