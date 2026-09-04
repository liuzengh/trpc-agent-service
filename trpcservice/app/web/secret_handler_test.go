package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/secret"
)

func newSecretServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	NewSecretAPI(secret.NewMemStore()).Register(mux)
	return httptest.NewServer(mux)
}

func TestSecretAPILifecycleNoPlaintextExposed(t *testing.T) {
	srv := newSecretServer(t)
	defer srv.Close()

	// put a value
	resp, err := http.Post(srv.URL+"/secrets", "application/json",
		bytes.NewBufferString(`{"key":"endpoint:gpt","value":"sk-very-secret"}`))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("put status = %d, want 201", resp.StatusCode)
	}
	resp.Body.Close()

	// list returns metadata only — plaintext must not appear
	resp, err = http.Get(srv.URL + "/secrets")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var items []secret.Secret
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	resp.Body.Close()
	if len(items) != 1 || items[0].Key != "endpoint:gpt" {
		t.Fatalf("list = %+v, want [endpoint:gpt]", items)
	}
	raw, _ := json.Marshal(items)
	if bytes.Contains(raw, []byte("sk-very-secret")) {
		t.Error("list response must not expose the plaintext value")
	}

	// validation
	resp, _ = http.Post(srv.URL+"/secrets", "application/json",
		bytes.NewBufferString(`{"key":"","value":"x"}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty key status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// delete then empty
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/secrets/endpoint:gpt", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(srv.URL + "/secrets")
	var after []secret.Secret
	_ = json.NewDecoder(resp.Body).Decode(&after)
	resp.Body.Close()
	if len(after) != 0 {
		t.Errorf("after delete = %+v, want empty", after)
	}
}

// TestSecretAPIStoreIsWritable confirms the store actually received the value
// (so the API can be used to wire credential refs at runtime).
func TestSecretAPIStoreIsWritable(t *testing.T) {
	store := secret.NewMemStore()
	mux := http.NewServeMux()
	NewSecretAPI(store).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	http.Post(srv.URL+"/secrets", "application/json",
		bytes.NewBufferString(`{"key":"wecom:corp","value":"corp-secret"}`))

	got, err := store.Get(context.Background(), "wecom:corp")
	if err != nil || got != "corp-secret" {
		t.Errorf("store.Get = (%q, %v), want corp-secret", got, err)
	}
}
