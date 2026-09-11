package domain

import "testing"

func TestSessionScopeIsExplicitAndRevisionBound(t *testing.T) {
	r := Requested{Route: Route{TenantID: "t", Provider: "telegram", AccountID: "a", BindingID: "b", DeploymentRevisionID: "r", Generation: 1}, Input: Input{ConversationID: "42", SenderID: "u"}}
	base := r.SessionID()
	r.Route.Generation++
	if r.SessionID() != base {
		t.Fatal("route refresh changed history namespace")
	}
	for _, change := range []func(*Requested){func(r *Requested) { r.Input.SenderID = "other" }, func(r *Requested) { r.Route.TenantID = "other" }, func(r *Requested) { r.Route.AccountID = "other" }, func(r *Requested) { r.Route.BindingID = "other" }, func(r *Requested) { r.Route.DeploymentRevisionID = "other" }, func(r *Requested) { r.Input.ConversationID = "other" }, func(r *Requested) { r.Input.ThreadID = "42" }} {
		x := r
		change(&x)
		if x.SessionID() == base {
			t.Fatal("scope collision")
		}
	}
	r.Route.DeploymentRevisionID = "r2"
	r.Route.DeploymentRevisionID = "r"
	if r.SessionID() != base {
		t.Fatal("switch-back did not restore scope")
	}
}
