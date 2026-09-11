package runtimehttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	runtimeprofile "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile"
	runtimehttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/inbound/runtimehttp"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const resolvePath = "/internal/v1/runtime-profiles/credentials/resolve"

var digest = "sha256:" + strings.Repeat("a", 64)
var resolveBody = `{"execution_token":"test-execution-token","manifest_id":"manifest-1","manifest_digest":"` + digest + `","uses":[{"credential_id":"crd_0123456789abcdef0123456789abcdef","purpose":"api_key","audience_digest":"` + digest + `"}]}`

func TestRuntimeCredentialHTTPRequiresTrustedContextNotHeaders(t *testing.T) {
	resolver := &resolverStub{}
	router := newRouter(resolver, "")
	req := httptest.NewRequest(http.MethodPost, resolvePath, strings.NewReader(resolveBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Worker-ID", "worker-spoof")
	req.Header.Set("Authorization", "Bearer worker-spoof")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 401 || resolver.calls != 0 {
		t.Fatal("headers impersonated a trusted Worker context")
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("authentication failure must be non-cacheable")
	}
}

func TestRuntimeCredentialHTTPReturnsCompleteBatchAndClearsBuffers(t *testing.T) {
	secret := []byte("private-runtime-credential")
	batch := application.CredentialBatch{TenantID: "tenant-trusted", ProfileID: "profile-trusted", RunID: "run-1", AttemptID: "attempt-1", WorkerID: "worker-trusted", LeaseEpoch: 7, ManifestID: "manifest-1", ManifestDigest: digest, Credentials: []application.ResolvedCredential{{Use: application.CredentialUse{CredentialID: "crd_0123456789abcdef0123456789abcdef", Purpose: "api_key", AudienceDigest: digest}, CredentialRevision: 4, Value: secret}}}
	resolver := &resolverStub{batch: batch}
	router := newRouter(resolver, "worker-trusted")
	w := post(router, resolveBody, "application/json")
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("resolution status=%d", w.Code)
	}
	if resolver.command.Authorization.WorkloadIdentity != "worker-trusted" || resolver.command.Authorization.ExecutionToken != "test-execution-token" || len(resolver.command.Uses) != 1 {
		t.Fatal("request authority was not derived from trusted context")
	}
	var got struct {
		TenantID       string `json:"tenant_id"`
		ProfileID      string `json:"profile_id"`
		RunID          string `json:"run_id"`
		AttemptID      string `json:"attempt_id"`
		WorkerID       string `json:"worker_id"`
		LeaseEpoch     int64  `json:"lease_epoch"`
		ManifestID     string `json:"manifest_id"`
		ManifestDigest string `json:"manifest_digest"`
		Credentials    []struct {
			ID       string `json:"credential_id"`
			Purpose  string `json:"purpose"`
			Audience string `json:"audience_digest"`
			Revision int64  `json:"credential_revision"`
			Value    string `json:"value"`
		} `json:"credentials"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.TenantID != batch.TenantID || got.ProfileID != batch.ProfileID || got.RunID != batch.RunID || got.AttemptID != batch.AttemptID || got.WorkerID != batch.WorkerID || got.LeaseEpoch != 7 || got.ManifestID != batch.ManifestID || got.ManifestDigest != digest || len(got.Credentials) != 1 {
		t.Fatal("batch metadata missing")
	}
	c := got.Credentials[0]
	if c.ID != batch.Credentials[0].Use.CredentialID || c.Purpose != "api_key" || c.Audience != digest || c.Revision != 4 || c.Value != "private-runtime-credential" {
		t.Fatal("internal credential wire representation is incomplete")
	}
	if !bytes.Equal(secret, make([]byte, len(secret))) || resolver.batch.Credentials[0].Value != nil {
		t.Fatal("resolved batch secret buffers were not cleared after encoding")
	}
}

func TestRuntimeCredentialHTTPFailuresNeverExposePartialBatch(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
	}{{application.ErrExecutionDependencyUnavailable, 503}, {application.ErrExecutionUnauthorized, 403}, {domain.ErrCredentialUnavailable, 409}, {domain.ErrCredentialAssociation, 409}, {errors.New("private-error-canary"), 500}} {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			secret := []byte("private-partial-canary")
			resolver := &resolverStub{err: tt.err, batch: application.CredentialBatch{TenantID: "partial-tenant", Credentials: []application.ResolvedCredential{{Value: secret}}}}
			w := post(newRouter(resolver, "trusted-worker"), resolveBody, "application/json")
			if w.Code != tt.status || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d", w.Code)
			}
			for _, fragment := range []string{"private-", "partial-tenant", "credentials", "execution_token"} {
				if strings.Contains(w.Body.String(), fragment) {
					t.Fatal("failure exposed partial batch or error details")
				}
			}
			if !bytes.Equal(secret, make([]byte, len(secret))) || resolver.batch.Credentials[0].Value != nil {
				t.Fatal("failed batch retained plaintext")
			}
		})
	}
}

func TestRuntimeCredentialHTTPStrictInputAndBodyLimit(t *testing.T) {
	resolver := &resolverStub{}
	router := newRouter(resolver, "worker-trusted")
	bodies := []string{"null", resolveBody + ` {}`, strings.Replace(resolveBody, `"manifest_id":"manifest-1"`, `"manifest_id":"manifest-1","manifest_id":"manifest-2"`, 1), strings.Replace(resolveBody, `"execution_token"`, `"Execution_Token"`, 1), resolveBody + strings.Repeat(" ", domain.MaxDocumentBytes)}
	for _, field := range []string{"tenant_id", "profile_id", "worker_id", "workload_identity", "run_id", "attempt_id"} {
		bodies = append(bodies, strings.Replace(resolveBody, "{", `{"`+field+`":"claimed",`, 1))
	}
	for _, body := range bodies {
		w := post(router, body, "application/json")
		if w.Code != 400 || w.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("invalid input status=%d", w.Code)
		}
	}
	for _, ct := range []string{"", "text/plain", "application/json-invalid"} {
		if w := post(router, resolveBody, ct); w.Code != 400 {
			t.Errorf("invalid MIME accepted: %q", ct)
		}
	}
	if resolver.calls != 0 {
		t.Fatal("invalid request reached resolver")
	}
}

func TestModuleRegistersRuntimeRouteOnlyWithPairedTrustedDependencies(t *testing.T) {
	for _, tt := range []struct {
		name               string
		auth, verify       bool
		wantErr, wantRoute bool
	}{{"disabled", false, false, false, false}, {"missing verifier", true, false, true, false}, {"missing authentication", false, true, true, false}, {"enabled", true, true, false, true}} {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			router := gin.New()
			deps := runtimeprofile.Dependencies{DB: dbStub{}, Routes: router, Authenticate: func(c *gin.Context) { c.Next() }, TenantAccess: accessStub{}, OwnerAccess: accessStub{}, CredentialKey: bytes.Repeat([]byte{1}, 32)}
			if tt.auth {
				deps.AuthenticateWorker = func(c *gin.Context) {
					c.Request = c.Request.WithContext(runtimehttp.WithWorkerIdentity(c.Request.Context(), "verified-worker"))
					c.Next()
				}
			}
			if tt.verify {
				deps.ExecutionVerifier = verifierStub{}
			}
			_, err := runtimeprofile.NewModule(deps)
			if (err != nil) != tt.wantErr {
				t.Fatalf("module pairing error=%v", err)
			}
			found := false
			for _, route := range router.Routes() {
				if route.Method == http.MethodPost && route.Path == resolvePath {
					found = true
				}
			}
			if found != tt.wantRoute {
				t.Fatalf("runtime route present=%v", found)
			}
		})
	}
}

type resolverStub struct {
	calls   int
	command application.ResolveAttemptCommand
	batch   application.CredentialBatch
	err     error
}

func (s *resolverStub) ResolveForAttempt(_ context.Context, c application.ResolveAttemptCommand) (application.CredentialBatch, error) {
	s.calls++
	s.command = c
	return s.batch, s.err
}
func newRouter(resolver *resolverStub, worker string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if worker != "" {
		r.Use(func(c *gin.Context) {
			c.Request = c.Request.WithContext(runtimehttp.WithWorkerIdentity(c.Request.Context(), worker))
			c.Next()
		})
	}
	runtimehttp.NewHandler(resolver).Register(r)
	return r
}
func post(router http.Handler, body, contentType string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, resolvePath, strings.NewReader(body))
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("X-Worker-ID", "untrusted-header")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

type accessStub struct{}

func (accessStub) IsActiveMember(context.Context, string, string) (bool, error) { return true, nil }
func (accessStub) IsActiveOwner(context.Context, string, string) (bool, error)  { return true, nil }

type verifierStub struct{}

func (verifierStub) VerifyAttempt(context.Context, application.ExecutionAuthorizationRequest) (application.ExecutionAuthorization, error) {
	return application.ExecutionAuthorization{}, application.ErrExecutionUnauthorized
}

type dbStub struct{}

func (dbStub) Begin(context.Context) (pgx.Tx, error)            { return nil, errors.New("unused") }
func (dbStub) QueryRow(context.Context, string, ...any) pgx.Row { return nil }
func (dbStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("unused")
}

func TestModuleInternalAuthenticationFailuresAreNotCacheable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	_, err := runtimeprofile.NewModule(runtimeprofile.Dependencies{
		DB: dbStub{}, Routes: router,
		Authenticate: func(c *gin.Context) { c.Next() },
		TenantAccess: accessStub{}, OwnerAccess: accessStub{},
		CredentialKey: bytes.Repeat([]byte{1}, 32), ExecutionVerifier: verifierStub{},
		AuthenticateWorker: func(c *gin.Context) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "workload authentication required"})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	w := post(router, resolveBody, "application/json")
	if w.Code != http.StatusUnauthorized || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("workload authentication response is cacheable: status=%d cache-control=%q", w.Code, w.Header().Get("Cache-Control"))
	}
}
