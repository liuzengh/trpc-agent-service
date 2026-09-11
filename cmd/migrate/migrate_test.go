package main

import (
	"context"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestParseSpec(t *testing.T) {
	cases := []struct {
		in      string
		backend string
		dsn     string
		wantErr bool
	}{
		{"inmemory", "inmemory", "", false},
		{"redis://localhost:6379/0", "redis", "redis://localhost:6379/0", false},
		{"mysql://u:p@tcp(h:3306)/db?parseTime=true", "mysql", "u:p@tcp(h:3306)/db?parseTime=true", false},
		{"mysql:u:p@tcp(h:3306)/db", "mysql", "u:p@tcp(h:3306)/db", false},
		{"", "", "", true},
		{"postgres://x", "", "", true},
	}
	for _, c := range cases {
		sp, err := parseSpec(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseSpec(%q) expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSpec(%q): %v", c.in, err)
			continue
		}
		if string(sp.backend) != c.backend || sp.dsn != c.dsn {
			t.Errorf("parseSpec(%q) = (%s,%q), want (%s,%q)", c.in, sp.backend, sp.dsn, c.backend, c.dsn)
		}
	}
}

// TestRoundTripInMemory migrates a seeded tenant/user from one in-memory
// backend pair to another and verifies session state+events and memory
// entries survive, proving the generic copy path used by all backends.
func TestRoundTripInMemory(t *testing.T) {
	ctx := context.Background()
	const tenant, user = "t1", "u1"

	// Seed source.
	srcSvc := sessioninmemory.NewSessionService()
	sKey := mkSessKey(tenant, user, "s1")
	created, err := srcSvc.CreateSession(ctx, sKey, map[string][]byte{"topic": []byte("agents")})
	if err != nil {
		t.Fatalf("seed create: %v", err)
	}
	ts := time.Now()
	for i := 0; i < 3; i++ {
		evt := event.New("inv", "user")
		evt.Timestamp = ts.Add(time.Duration(i) * time.Millisecond)
		evt.Response = &model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleUser, Content: "hello"},
		}}}
		if err := srcSvc.AppendEvent(ctx, created, evt); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}
	srcMem := memoryinmemory.NewMemoryService()
	memKey := memory.UserKey{AppName: tenant, UserID: user}
	if err := srcMem.AddMemory(ctx, memKey, "user likes Go", []string{"go"}, memory.WithMetadata(&memory.Metadata{Kind: memory.KindFact})); err != nil {
		t.Fatalf("seed memory: %v", err)
	}

	// Copy onto a fresh (empty) backend pair via the shared copy path.
	dstSvc := sessioninmemory.NewSessionService()
	dstMem := memoryinmemory.NewMemoryService()
	ns, ne, nm, err := copyUser(ctx, srcSvc, dstSvc, srcMem, dstMem, tenant, user)
	if err != nil {
		t.Fatalf("copyUser: %v", err)
	}
	if ns != 1 || ne != 3 || nm != 1 {
		t.Fatalf("copy counts = (%d,%d,%d), want (1,3,1)", ns, ne, nm)
	}

	// Verify target.
	got, err := dstSvc.GetSession(ctx, sKey)
	if err != nil {
		t.Fatalf("target get session: %v", err)
	}
	if got == nil {
		t.Fatal("target session missing")
	}
	if string(got.State["topic"]) != "agents" {
		t.Errorf("state lost: %v", got.State)
	}
	if len(got.Events) != 3 {
		t.Errorf("events lost: got %d, want 3", len(got.Events))
	}

	entries, err := dstMem.ReadMemories(ctx, memKey, 10)
	if err != nil {
		t.Fatalf("target read memories: %v", err)
	}
	if len(entries) != 1 || entries[0].Memory.Memory != "user likes Go" {
		t.Errorf("memory lost: got %+v", entries)
	}
	if entries[0].Memory.Kind != memory.KindFact {
		t.Errorf("memory kind lost: got %q", entries[0].Memory.Kind)
	}
}

// TestRunRequiresTenant covers the CLI validation path.
func TestRunRequiresTenant(t *testing.T) {
	if err := run(context.Background(), options{srcSession: "inmemory", dstSession: "inmemory"}); err == nil {
		t.Fatal("expected error for missing -tenant")
	}
}

func mkSessKey(app, user, sess string) session.Key {
	return session.Key{AppName: app, UserID: user, SessionID: sess}
}
