package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func newChannelServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	NewChannelAPI(channels.NewMemBindingStore()).Register(mux)
	return httptest.NewServer(mux)
}

func TestChannelBindingsAPI(t *testing.T) {
	srv := newChannelServer(t)
	defer srv.Close()

	// create
	body := `{"tenant_id":"t1","agent_id":"a1","channel":"wecom","account_id":"wx-1","credential_ref":"sec://wx-1"}`
	resp, err := http.Post(srv.URL+"/channels", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	resp.Body.Close()

	// duplicate channel+account -> 409
	resp, _ = http.Post(srv.URL+"/channels", "application/json", strings.NewReader(body))
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("dup status = %d, want 409", resp.StatusCode)
	}
	resp.Body.Close()

	// invalid channel -> 400
	bad := `{"tenant_id":"t1","agent_id":"a1","channel":"telegram","account_id":"x"}`
	resp, _ = http.Post(srv.URL+"/channels", "application/json", strings.NewReader(bad))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad channel status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// missing fields -> 400
	bad = `{"tenant_id":"t1"}`
	resp, _ = http.Post(srv.URL+"/channels", "application/json", strings.NewReader(bad))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing fields status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// list shows the binding (credential ref is a reference, not a token)
	resp, err = http.Get(srv.URL + "/channels?tenant_id=t1")
	if err != nil {
		t.Fatal(err)
	}
	var got []channels.ChannelBinding
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(got) != 1 || got[0].CredentialRef != "sec://wx-1" || got[0].Channel != "wecom" {
		t.Errorf("list = %+v", got)
	}

	// delete then 404 on a second delete
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/channels/"+got[0].BindingID, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/channels/nope", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("delete missing status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}
