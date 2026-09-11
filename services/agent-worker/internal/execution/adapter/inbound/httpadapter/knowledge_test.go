package httpadapter

import (
	"context"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"net/http/httptest"
	"testing"
)

type knowledgeStub struct{ calls int }

func (k *knowledgeStub) ImportKnowledge(context.Context, proof.KnowledgeRequest) (proof.KnowledgeResponse, error) {
	k.calls++
	return proof.KnowledgeResponse{Documents: 1}, nil
}
func TestKnowledgeHTTPControlIdentityAndNoScopeOverride(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  []string
		body string
		want int
	}{
		{"no TLS", nil, `{}`, 401}, {"gateway", []string{gatewayID}, `{}`, 403}, {"forged namespace", []string{controlID}, `{"scope":"other"}`, 400}, {"forged backend", []string{controlID}, `{"endpoint":"http://other"}`, 400}, {"control", []string{controlID}, `{}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &knowledgeStub{}
			h, err := New(&attemptStub{}, &finalStub{}, Options{ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}, Knowledge: k})
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, request(proof.KnowledgePath, []byte(tc.body), tc.ids...))
			if w.Code != tc.want || (tc.want != 200 && k.calls != 0) {
				t.Fatal(w.Code, k.calls)
			}
		})
	}
}
