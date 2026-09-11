package domain

import "testing"

func TestSocialIdentityIsObservedAccountNotConversation(t *testing.T) {
	r := Requested{Route: Route{TenantID: "tenant", Provider: "telegram", AccountID: "bot", BindingID: "binding", DeploymentRevisionID: "rev"}, Input: Input{SenderID: "42", ConversationID: "group"}}
	identity, session := r.SocialIdentityID(), r.SessionID()
	otherConversation := r
	otherConversation.Input.ConversationID = "other"
	if otherConversation.SocialIdentityID() != identity || otherConversation.SessionID() == session {
		t.Fatal("identity and conversation conflated")
	}
	for _, change := range []func(*Requested){func(r *Requested) { r.Route.TenantID = "other" }, func(r *Requested) { r.Route.Provider = "wecom" }, func(r *Requested) { r.Route.AccountID = "other" }, func(r *Requested) { r.Input.SenderID = "other" }} {
		other := r
		change(&other)
		if other.SocialIdentityID() == identity || other.SessionID() == session {
			t.Fatal("identity scope collision")
		}
	}
	repeat := r
	repeat.Input.Text = "next round"
	repeat.EventID = "new-event"
	repeat.RunID = "new-run"
	repeat.Route.Generation++
	if repeat.SocialIdentityID() != identity || repeat.SessionID() != session {
		t.Fatal("next message changed identity/session")
	}
}
