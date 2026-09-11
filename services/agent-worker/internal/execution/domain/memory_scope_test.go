package domain

import "testing"

func TestMemoryScopeUsesStableAgentAndObservedIdentity(t *testing.T) {
	r := Requested{Route: Route{TenantID: "tenant", Provider: "telegram", AccountID: "bot", BindingID: "binding", DeploymentRevisionID: "revision"}, Input: Input{SenderID: "sender", ConversationID: "conversation"}}
	base, err := r.MemoryScopeID("agent")
	if err != nil || base == "" {
		t.Fatalf("scope: %q, %v", base, err)
	}
	for _, mutate := range []func(*Requested){
		func(r *Requested) { r.Input.ConversationID = "other" },
		func(r *Requested) { r.Input.ThreadID = "other" },
		func(r *Requested) { r.Route.DeploymentRevisionID = "next" },
		func(r *Requested) { r.Route.BindingID = "next" },
		func(r *Requested) { r.Route.ManifestRef = "next"; r.Route.Generation++ },
		func(r *Requested) { r.RunID = "next"; r.Input.Text = "next" },
	} {
		x := r
		mutate(&x)
		if got, err := x.MemoryScopeID("agent"); err != nil || got != base {
			t.Fatalf("execution change reset memory: %q, %v", got, err)
		}
	}
	for _, mutate := range []func(*Requested){
		func(r *Requested) { r.Route.TenantID = "other" },
		func(r *Requested) { r.Route.AccountID = "other" },
		func(r *Requested) { r.Route.Provider = "wecom" },
		func(r *Requested) { r.Input.SenderID = "other" },
	} {
		x := r
		mutate(&x)
		if got, err := x.MemoryScopeID("agent"); err != nil || got == base {
			t.Fatalf("identity collision: %q, %v", got, err)
		}
	}
	if got, _ := r.MemoryScopeID("other-agent"); got == base {
		t.Fatal("Agent isolation lost")
	}
	if got, _ := r.MemoryScopeID(" agent "); got == base {
		t.Fatal("identity silently normalized")
	}
}

func TestMemoryScopeRejectsIncompleteIdentity(t *testing.T) {
	r := Requested{Route: Route{TenantID: "tenant", Provider: "telegram", AccountID: "bot"}, Input: Input{SenderID: "sender"}}
	for _, mutate := range []func(*Requested){
		func(r *Requested) { r.Route.TenantID = " " },
		func(r *Requested) { r.Route.AccountID = "" },
		func(r *Requested) { r.Route.Provider = "unknown" },
		func(r *Requested) { r.Input.SenderID = "" },
	} {
		x := r
		mutate(&x)
		if got, err := x.MemoryScopeID("agent"); err != ErrInvalid || got != "" {
			t.Fatalf("invalid identity accepted: %q, %v", got, err)
		}
	}
	if got, err := r.MemoryScopeID(" "); err != ErrInvalid || got != "" {
		t.Fatalf("missing agent accepted: %q, %v", got, err)
	}
}

func TestMemoryScopeEncodingPreservesComponentBoundaries(t *testing.T) {
	a := Requested{Route: Route{TenantID: "tenant", Provider: "telegram", AccountID: "bot:a"}, Input: Input{SenderID: "b"}}
	b := a
	b.Route.AccountID, b.Input.SenderID = "bot", "a:b"
	left, err := a.MemoryScopeID("agent")
	if err != nil {
		t.Fatal(err)
	}
	right, err := b.MemoryScopeID("agent")
	if err != nil || left == right {
		t.Fatalf("ambiguous component encoding: %q %q %v", left, right, err)
	}
}
