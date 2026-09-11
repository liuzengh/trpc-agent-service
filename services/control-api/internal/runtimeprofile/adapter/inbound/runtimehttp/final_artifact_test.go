package runtimehttp_test

import (
	"context"
	"github.com/gin-gonic/gin"
	runtimehttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/inbound/runtimehttp"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type finalResolverStub struct {
	calls  int
	worker string
}

func (s *finalResolverStub) ResolveForAttempt(context.Context, application.ResolveAttemptCommand) (application.CredentialBatch, error) {
	panic("old Attempt path called")
}
func (s *finalResolverStub) ResolveForFinalArtifact(_ context.Context, w string, in application.ResolveFinalArtifactInput) (application.CredentialBatch, error) {
	s.calls++
	s.worker = w
	return application.CredentialBatch{TenantID: in.TenantID, WorkerID: w}, nil
}
func TestFinalArtifactHTTPRequiresWorkloadAndClosedBody(t *testing.T) {
	body := `{"final":{"intent_id":"i","digest":"` + digest + `","admission_id":"a","run_id":"r","attempt_id":"at","completion_id":"c","execution_generation":1,"sequence":1},"tenant_id":"t","manifest_id":"m","manifest_digest":"` + digest + `","deployment_id":"d","deployment_revision_id":"dr","profile_id":"p","profile_revision_number":1}`
	for _, mode := range []string{"ok", "noauth", "uses"} {
		t.Run(mode, func(t *testing.T) {
			s := &finalResolverStub{}
			router := gin.New()
			router.Use(func(c *gin.Context) {
				if mode != "noauth" {
					c.Request = c.Request.WithContext(runtimehttp.WithWorkerIdentity(c.Request.Context(), "trusted"))
				}
				c.Next()
			})
			runtimehttp.NewHandler(s).Register(router)
			raw := body
			if mode == "uses" {
				raw = strings.Replace(raw, `"final":`, `"uses":[],"final":`, 1)
			}
			req := httptest.NewRequest("POST", runtimehttp.FinalArtifactResolvePath, strings.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Worker-ID", "spoof")
			out := httptest.NewRecorder()
			router.ServeHTTP(out, req)
			want := http.StatusOK
			if mode == "noauth" {
				want = 401
			}
			if mode == "uses" {
				want = 400
			}
			if out.Code != want || out.Header().Get("Cache-Control") != "no-store" {
				t.Fatal(out.Code, out.Body.String())
			}
			if mode == "ok" {
				if s.calls != 1 || s.worker != "trusted" {
					t.Fatal("bad identity")
				}
			} else if s.calls != 0 {
				t.Fatal("invalid request resolved")
			}
		})
	}
}
