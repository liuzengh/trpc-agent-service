package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"
)

func newMemberMux(mgr *member.Manager) *http.ServeMux {
	mux := http.NewServeMux()
	NewMemberAPI(mgr).Register(mux)
	return mux
}

// postMembers issues an authenticated POST /members with the given JSON body.
func postMembers(mux *http.ServeMux, claims *auth.Claims, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/members", strings.NewReader(body))
	if claims != nil {
		req = req.WithContext(context.WithValue(req.Context(), AuthUserKey, claims))
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestOwnerCreatesMemberInAnyTenant(t *testing.T) {
	mgr := member.NewManager()
	mux := newMemberMux(mgr)
	owner := &auth.Claims{TenantID: "platform", UserID: "root", Role: member.RoleOwner}

	rec := postMembers(mux, owner, `{"user_id":"alice","password":"secret","role":"member","tenant_id":"t2"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s, want 201", rec.Code, rec.Body.String())
	}
	got, err := mgr.GetByUserID(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != "t2" {
		t.Errorf("member tenant = %q, want t2 (owner chose it)", got.TenantID)
	}
}

func TestAdminCreatesMemberInOwnTenantOnly(t *testing.T) {
	mgr := member.NewManager()
	mux := newMemberMux(mgr)
	admin := &auth.Claims{TenantID: "t1", UserID: "admin", Role: member.RoleAdmin}

	// The admin tries to place a member in another tenant; the backend pins it
	// to the admin's own tenant regardless.
	rec := postMembers(mux, admin, `{"user_id":"bob","password":"secret","role":"member","tenant_id":"t2"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s, want 201", rec.Code, rec.Body.String())
	}
	got, err := mgr.GetByUserID(context.Background(), "bob")
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != "t1" {
		t.Errorf("member tenant = %q, want t1 (admin's own tenant)", got.TenantID)
	}
}

func TestMemberCannotCreateMembers(t *testing.T) {
	mgr := member.NewManager()
	mux := newMemberMux(mgr)
	plain := &auth.Claims{TenantID: "t1", UserID: "alice", Role: member.RoleMember}
	rec := postMembers(mux, plain, `{"user_id":"eve","password":"secret","role":"member"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestAdminCannotCreateOwner(t *testing.T) {
	mgr := member.NewManager()
	mux := newMemberMux(mgr)
	admin := &auth.Claims{TenantID: "t1", UserID: "admin", Role: member.RoleAdmin}
	rec := postMembers(mux, admin, `{"user_id":"root2","password":"secret","role":"owner"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (admin cannot create an owner)", rec.Code)
	}
}

func TestMemberListIsScopedToRole(t *testing.T) {
	mgr := member.NewManager()
	// Seed two tenants' members.
	for _, m := range []*member.Member{
		{TenantID: "t1", UserID: "a1", Role: member.RoleMember, Password: "x"},
		{TenantID: "t1", UserID: "admin1", Role: member.RoleAdmin, Password: "x"},
		{TenantID: "t2", UserID: "a2", Role: member.RoleMember, Password: "x"},
	} {
		if err := mgr.Create(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	mux := newMemberMux(mgr)

	list := func(claims *auth.Claims) []*member.Member {
		req := httptest.NewRequest(http.MethodGet, "/members", nil)
		req = req.WithContext(context.WithValue(req.Context(), AuthUserKey, claims))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("list status = %d, body = %s", rec.Code, rec.Body.String())
		}
		var out []*member.Member
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	// The owner sees every tenant.
	owner := &auth.Claims{TenantID: "platform", UserID: "root", Role: member.RoleOwner}
	if got := list(owner); len(got) != 3 {
		t.Errorf("owner sees %d members, want 3", len(got))
	}

	// An admin only sees their own tenant, whatever the query asks for.
	admin := &auth.Claims{TenantID: "t1", UserID: "admin1", Role: member.RoleAdmin}
	if got := list(admin); len(got) != 2 {
		t.Errorf("admin sees %d members, want 2 (own tenant only)", len(got))
	}
}
