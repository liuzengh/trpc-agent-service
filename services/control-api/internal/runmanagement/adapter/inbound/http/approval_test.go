package httpadapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

type approvalServiceStub struct {
	calls   int
	request approvalv1.DecisionRequest
}

func (*approvalServiceStub) ListRuns(context.Context, string, string, int, int) (managementv1.RunPage, error) {
	return managementv1.RunPage{}, nil
}
func (*approvalServiceStub) GetRun(context.Context, string, string, string) (managementv1.RunDetail, error) {
	return managementv1.RunDetail{}, nil
}
func (*approvalServiceStub) ListAudit(context.Context, string, string, int, int) (managementv1.AuditPage, error) {
	return managementv1.AuditPage{}, nil
}
func (*approvalServiceStub) ListApprovals(context.Context, string, string, int, int) (approvalv1.Page, error) {
	return approvalv1.Page{Operations: []approvalv1.Operation{}, Limit: 25}, nil
}
func (s *approvalServiceStub) DecideApproval(_ context.Context, tenant, user, operation string, request approvalv1.DecisionRequest) (approvalv1.DecisionResponse, error) {
	s.calls++
	s.request = request
	return approvalv1.DecisionResponse{Outcome: "DECIDED", Operation: approvalv1.Operation{TenantID: tenant, OperationID: operation, DecidedBy: user}}, nil
}

func approvalRouter(service Service) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		identity := identityapp.IdentityContext{UserID: "owner"}
		c.Request = c.Request.WithContext(identityapp.WithIdentity(c.Request.Context(), identity))
		c.Next()
	})
	New(service).Register(router)
	return router
}

func TestApprovalDecisionRequiresClosedJSONAndForwardsDigest(t *testing.T) {
	service := &approvalServiceStub{}
	router := approvalRouter(service)
	digest := "sha256:" + strings.Repeat("a", 64)
	body := `{"action":"approve","expected_arguments_digest":"` + digest + `"}`

	missingType := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/tool-approvals/tap_1/decision", strings.NewReader(body))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, missingType)
	if response.Code != http.StatusBadRequest || service.calls != 0 {
		t.Fatal(response.Code, response.Body.String(), service.calls)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/tool-approvals/tap_1/decision", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || service.calls != 1 || service.request.ExpectedArgumentsDigest != digest {
		t.Fatal(response.Code, response.Body.String(), service.calls, service.request)
	}
}
