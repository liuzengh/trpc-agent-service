package main

import "testing"

// TestPlanForRoleCapabilities pins the node topology: each role enables exactly
// the capabilities it is named for. The regression this guards is a role that
// registers an API whose loop it never runs (the previous behaviour: `-role
// gateway` did not start the gateway, and `-role admin` lacked the chat API).
func TestPlanForRoleCapabilities(t *testing.T) {
	cases := []struct {
		role  string
		plan  rolePlan
		valid bool
	}{
		{roleAll, rolePlan{AdminAPI: true, ChatAPI: true, WorkerLoop: true, IMGatway: true}, true},
		{roleAdmin, rolePlan{AdminAPI: true}, true},
		{roleWorker, rolePlan{ChatAPI: true, WorkerLoop: true}, true},
		{roleGateway, rolePlan{IMGatway: true}, true},
		{"", rolePlan{}, false},
		{"Gateway", rolePlan{}, false}, // role names are case-sensitive
		{"bogus", rolePlan{}, false},
	}
	for _, c := range cases {
		got, err := planFor(c.role)
		if c.valid {
			if err != nil {
				t.Errorf("planFor(%q) unexpected error: %v", c.role, err)
				continue
			}
			if got != c.plan {
				t.Errorf("planFor(%q) = %+v, want %+v", c.role, got, c.plan)
			}
			continue
		}
		if err == nil {
			t.Errorf("planFor(%q) = %+v, want an error for an unknown role", c.role, got)
		}
	}
}

// TestWorkerRoleOwnsTheChatSurface documents the coupling the plan encodes: the
// worker consumes what /chat publishes, so they travel together.
func TestWorkerRoleOwnsTheChatSurface(t *testing.T) {
	w, err := planFor(roleWorker)
	if err != nil {
		t.Fatal(err)
	}
	if !w.ChatAPI || !w.WorkerLoop {
		t.Errorf("worker plan = %+v, want the chat API and the worker loop together", w)
	}
	if w.AdminAPI {
		t.Error("worker must not expose the management REST surface")
	}
	// A gateway node holds IM connections only: it must not expose REST or
	// consume inbound messages.
	g, err := planFor(roleGateway)
	if err != nil {
		t.Fatal(err)
	}
	if g.AdminAPI || g.ChatAPI || g.WorkerLoop {
		t.Errorf("gateway plan = %+v, want IM gateway only", g)
	}
	if !g.IMGatway {
		t.Error("gateway role must start the IM gateway")
	}
}
