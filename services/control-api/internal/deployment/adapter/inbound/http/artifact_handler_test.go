package httpadapter

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"net/http"
	"testing"
)

type fakeArtifactService struct {
	*fakeDeploymentService
	last application.ArtifactCommand
	err  error
}

func (f *fakeArtifactService) AccessArtifact(_ context.Context, c application.ArtifactCommand) (application.ArtifactResult, error) {
	f.last = c
	return application.ArtifactResult{Name: c.Name, Version: 0, MimeType: "text/plain", Content: []byte("hello"), SizeBytes: 5}, f.err
}
func TestArtifactHTTPBytesAndClosedQuery(t *testing.T) {
	f := &fakeArtifactService{fakeDeploymentService: &fakeDeploymentService{}}
	router := newTestRouter(f)
	path := "/v1/tenants/" + testTenantID + "/deployments/" + testDeploymentID + "/revisions/1/artifacts/note.txt?run_id=run1"
	identity := &identityapp.IdentityContext{UserID: testUserID}
	w := performRequestWithContentType(router, http.MethodPut, path, "hello", identity, nil, "text/plain")
	if w.Code != 200 || f.last.Operation != "save" {
		t.Fatal(w.Code, w.Body.String())
	}

	if f.last.Name != "note.txt" || f.last.RunID != "run1" || f.last.MIMEType != "text/plain" {
		t.Fatal(f.last)
	}
	w = performRequest(router, http.MethodGet, path+"&version=0", "", identity, nil)
	if w.Code != 200 || w.Body.String() != "hello" || w.Header().Get("X-Artifact-Version") != "0" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, suffix := range []string{"&user_id=evil", "&run_id=second", "&version=-1"} {
		w = performRequest(router, http.MethodGet, path+suffix, "", identity, nil)
		if w.Code != 400 {
			t.Fatal("query", suffix, w.Code)
		}
	}
	f.err = application.ErrArtifactForbidden
	w = performRequest(router, http.MethodGet, path, "", identity, nil)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	w = performRequest(router, http.MethodGet, path, "", nil, nil)
	if w.Code != 401 {
		t.Fatal("unauthenticated", w.Code)
	}
}
