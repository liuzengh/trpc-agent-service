package httpadapter

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"net/http"
	"testing"
)

type fakeKnowledgeService struct {
	*fakeDeploymentService
	last application.KnowledgeImportCommand
	err  error
}

func (f *fakeKnowledgeService) ImportKnowledge(_ context.Context, c application.KnowledgeImportCommand) (application.KnowledgeResult, error) {
	f.last = c
	return application.KnowledgeResult{Documents: 2}, f.err
}
func TestKnowledgeImportHTTPClosedBody(t *testing.T) {
	f := &fakeKnowledgeService{fakeDeploymentService: &fakeDeploymentService{}}
	router := newTestRouter(f)
	path := "/v1/tenants/" + testTenantID + "/deployments/" + testDeploymentID + "/revisions/1/knowledge/docs/import"
	id := &identityapp.IdentityContext{UserID: testUserID}
	w := performRequest(router, http.MethodPost, path, `{"name":"note","text":"hello"}`, id, nil)
	if w.Code != 200 || f.last.Resource != "docs" || f.last.Text != "hello" {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, body := range []string{`{"name":"a","text":"b","scope":"evil"}`, `{"name":"a","text":"b","text":"c"}`, `{"text":"a"}`} {
		w = performRequest(router, http.MethodPost, path, body, id, nil)
		if w.Code != 400 {
			t.Fatal("body accepted", w.Code)
		}
	}
	w = performRequest(router, http.MethodPost, path+"?scope=x", `{"name":"a","text":"b"}`, id, nil)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	f.err = application.ErrKnowledgeForbidden
	w = performRequest(router, http.MethodPost, path, `{"name":"a","text":"b"}`, id, nil)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
}
