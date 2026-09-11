package httpadapter

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
)

type backendMigrationStub struct {
	calls int
	err   error
}

func (s *backendMigrationStub) Execute(context.Context, proof.BackendMigrationRequest) (proof.BackendMigrationResponse, error) {
	s.calls++
	return proof.BackendMigrationResponse{MemoryScopesCopied: 3}, s.err
}

func TestBackendMigrationHTTPIsControlOnlyAndMapsBusy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ids    []string
		err    error
		status int
	}{
		{"missing TLS", nil, nil, 401}, {"gateway denied", []string{gatewayID}, nil, 403}, {"busy", []string{controlID}, proof.ErrBackendMigrationBusy, 409}, {"success", []string{controlID}, nil, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &backendMigrationStub{err: tc.err}
			h, err := New(&attemptStub{}, &finalStub{}, Options{ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}, BackendMigrations: stub})
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, request(proof.BackendMigrationPath, []byte(`{}`), tc.ids...))
			if w.Code != tc.status || (tc.status != 200 && tc.status != 409 && stub.calls != 0) {
				t.Fatal(w.Code, stub.calls, w.Body.String())
			}
			if tc.status == 200 {
				var out proof.BackendMigrationResponse
				if json.Unmarshal(w.Body.Bytes(), &out) != nil || out.MemoryScopesCopied != 3 {
					t.Fatal(w.Body.String())
				}
			}
		})
	}
}
