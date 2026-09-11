package httpadapter

import (
	"context"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"net/http/httptest"
	"testing"
)

type artifactStub struct {
	calls int
	err   error
}

func (a *artifactStub) Artifact(context.Context, proof.ArtifactRequest) (proof.ArtifactResponse, error) {
	a.calls++
	return proof.ArtifactResponse{Name: "x"}, a.err
}
func TestArtifactHTTPRequiresControlIdentityAndStrictBody(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ids    []string
		body   string
		status int
	}{
		{"no TLS", nil, `{}`, 401}, {"gateway", []string{gatewayID}, `{}`, 403}, {"multiple", []string{controlID, gatewayID}, `{}`, 403}, {"unknown field", []string{controlID}, `{"session_id":"forged"}`, 400}, {"trailing body", []string{controlID}, `{} {}`, 400}, {"control", []string{controlID}, `{}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &artifactStub{}
			h, err := New(&attemptStub{}, &finalStub{}, Options{ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}, Artifacts: a})
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, request(proof.ArtifactPath, []byte(tc.body), tc.ids...))
			if w.Code != tc.status || (tc.status != 200 && a.calls != 0) {
				t.Fatalf("%d calls=%d", w.Code, a.calls)
			}
		})
	}
}
