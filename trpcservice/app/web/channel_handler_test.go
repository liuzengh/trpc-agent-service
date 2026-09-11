package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

func newChannelServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	NewChannelAPI(channels.NewMemBindingStore()).Register(mux)
	return httptest.NewServer(asClaims(mux))
}

func TestChannelBindingsAPI(t *testing.T) {
	srv := newChannelServer(t)
	defer srv.Close()

	t1 := clientAs(adminClaims("t1"))

	// create
	body := `{"tenant_id":"t1","agent_id":"a1","channel":"wecom","account_id":"wx-1","credential_ref":"sec://wx-1"}`
	resp := postAs(t, t1, srv.URL+"/channels", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	resp.Body.Close()

	// duplicate channel+account -> 409
	resp = postAs(t, t1, srv.URL+"/channels", body)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("dup status = %d, want 409", resp.StatusCode)
	}
	resp.Body.Close()

	// invalid channel -> 400
	bad := `{"tenant_id":"t1","agent_id":"a1","channel":"telegram","account_id":"x"}`
	resp = postAs(t, t1, srv.URL+"/channels", bad)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad channel status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// missing fields -> 400
	bad = `{"tenant_id":"t1"}`
	resp = postAs(t, t1, srv.URL+"/channels", bad)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing fields status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// list shows the binding (credential ref is a reference, not a token)
	resp = getAs(t, t1, srv.URL+"/channels?tenant_id=t1")
	var got []channels.ChannelBinding
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(got) != 1 || got[0].CredentialRef != "sec://wx-1" || got[0].Channel != "wecom" {
		t.Errorf("list = %+v", got)
	}

	// delete then 404 on a second delete
	resp = doAs(t, t1, http.MethodDelete, srv.URL+"/channels/"+got[0].BindingID, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doAs(t, t1, http.MethodDelete, srv.URL+"/channels/nope", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("delete missing status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestChannelBindingsAreTenantScoped covers the tenant boundary on bindings: a
// foreign tenant sees none of them and cannot delete one by id, while the write
// is pinned to the caller's own tenant.
func TestChannelBindingsAreTenantScoped(t *testing.T) {
	srv := newChannelServer(t)
	defer srv.Close()

	t1 := clientAs(adminClaims("t1"))
	t2 := clientAs(adminClaims("t2"))

	// t1 asks to bind into t2; the tenant is pinned back to t1.
	body := `{"tenant_id":"t2","agent_id":"a1","channel":"wecom","account_id":"wx-1"}`
	resp := postAs(t, t1, srv.URL+"/channels", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	resp.Body.Close()

	resp = getAs(t, t1, srv.URL+"/channels")
	var mine []channels.ChannelBinding
	if err := json.NewDecoder(resp.Body).Decode(&mine); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(mine) != 1 || mine[0].TenantID != "t1" {
		t.Fatalf("t1 bindings = %+v, want one owned by t1", mine)
	}

	// t2 sees nothing and cannot touch t1's binding.
	resp = getAs(t, t2, srv.URL+"/channels")
	var theirs []channels.ChannelBinding
	if err := json.NewDecoder(resp.Body).Decode(&theirs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(theirs) != 0 {
		t.Errorf("t2 bindings = %+v, want none", theirs)
	}

	resp = doAs(t, t2, http.MethodDelete, srv.URL+"/channels/"+mine[0].BindingID, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("foreign delete status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// t1's binding survived.
	resp = getAs(t, t1, srv.URL+"/channels")
	if err := json.NewDecoder(resp.Body).Decode(&mine); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(mine) != 1 {
		t.Errorf("t1 binding was removed by a foreign delete: %+v", mine)
	}
}
