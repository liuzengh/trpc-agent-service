package agent

import (
	"context"
	"testing"
)

// grayFixture publishes two versions with distinguishable prompts.
func grayFixture(t *testing.T) (*Manager, string) {
	t.Helper()
	ctx := context.Background()
	m := NewManager(nil)
	if err := m.Create(ctx, Agent{ID: "a-gray", TenantID: "t1", Name: "helper"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Publish(ctx, "a-gray", RuntimeProfile{SystemPrompt: "v1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Publish(ctx, "a-gray", RuntimeProfile{SystemPrompt: "v2"}); err != nil {
		t.Fatal(err)
	}
	return m, "a-gray"
}

// TestGrayReleaseSplitsTrafficBySession pins the canary contract: the release
// share of sessions runs the alternate version and the rest stay on the current
// one, with the choice fixed per session so a conversation never flips version
// mid-flight.
func TestGrayReleaseSplitsTrafficBySession(t *testing.T) {
	ctx := context.Background()
	m, id := grayFixture(t)

	if err := m.SetGray(ctx, id, &GrayRelease{Version: 1, Percent: 50}); err != nil {
		t.Fatalf("SetGray: %v", err)
	}

	gray, base := 0, 0
	for i := 0; i < 400; i++ {
		session := "session-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i%10))
		p, version, err := m.ResolveForSession(ctx, id, session)
		if err != nil {
			t.Fatalf("ResolveForSession(%q): %v", session, err)
		}
		switch version {
		case 1:
			gray++
			if p.SystemPrompt != "v1" {
				t.Fatalf("gray session got prompt %q, want v1", p.SystemPrompt)
			}
		case 2:
			base++
			if p.SystemPrompt != "v2" {
				t.Fatalf("baseline session got prompt %q, want v2", p.SystemPrompt)
			}
		default:
			t.Fatalf("unexpected version %d", version)
		}
		// Stability: the same session always resolves the same way.
		if _, again, _ := m.ResolveForSession(ctx, id, session); again != version {
			t.Fatalf("session %q flipped from version %d to %d", session, version, again)
		}
	}
	if gray == 0 || base == 0 {
		t.Fatalf("split produced gray=%d base=%d, want both sides populated", gray, base)
	}
	// A 50% split over 400 sessions cannot be far off; a broken bucketing
	// (e.g. always the same side) shows up immediately.
	if gray < 100 || gray > 300 {
		t.Errorf("gray share = %d/400, want roughly half", gray)
	}
}

// TestGrayReleaseClearedSendsEverythingToCurrent is the rollback guarantee.
func TestGrayReleaseClearedSendsEverythingToCurrent(t *testing.T) {
	ctx := context.Background()
	m, id := grayFixture(t)
	if err := m.SetGray(ctx, id, &GrayRelease{Version: 1, Percent: 100}); err != nil {
		t.Fatal(err)
	}
	if _, version, _ := m.ResolveForSession(ctx, id, "s1"); version != 1 {
		t.Fatalf("version = %d, want 1 while the release is active", version)
	}
	if err := m.ClearGray(ctx, id); err != nil {
		t.Fatal(err)
	}
	for _, session := range []string{"s1", "s2", "s3"} {
		if p, version, err := m.ResolveForSession(ctx, id, session); err != nil || version != 2 {
			t.Fatalf("after clearing: version = %d err = %v, want 2 (current)", version, err)
		} else if p.SystemPrompt != "v2" {
			t.Fatalf("after clearing: prompt = %q, want v2", p.SystemPrompt)
		}
	}
}

// TestGrayReleaseRejectsBadTargets keeps a typo from silently doing nothing: an
// unpublished version, the current version, or an out-of-range percentage.
func TestGrayReleaseRejectsBadTargets(t *testing.T) {
	ctx := context.Background()
	m, id := grayFixture(t)

	if err := m.SetGray(ctx, id, &GrayRelease{Version: 9, Percent: 10}); err == nil {
		t.Error("an unpublished gray version must be rejected")
	}
	if err := m.SetGray(ctx, id, &GrayRelease{Version: 2, Percent: 10}); err == nil {
		t.Error("the current version must not be used as its own gray target")
	}
	for _, percent := range []int{0, -1, 101} {
		if err := m.SetGray(ctx, id, &GrayRelease{Version: 1, Percent: percent}); err == nil {
			t.Errorf("percent %d must be rejected", percent)
		}
	}
	if err := m.SetGray(ctx, "missing", &GrayRelease{Version: 1, Percent: 10}); err == nil {
		t.Error("an unknown agent must be rejected")
	}
}

// TestResolveForSessionFallsBackWhenTheGrayVersionVanishes protects the live
// path: a hand-edited or deleted version must not fail the user's turn.
func TestResolveForSessionFallsBackWhenTheGrayVersionVanishes(t *testing.T) {
	ctx := context.Background()
	m, id := grayFixture(t)
	if err := m.SetGray(ctx, id, &GrayRelease{Version: 1, Percent: 100}); err != nil {
		t.Fatal(err)
	}
	// Simulate the version disappearing (rollback to a state where the row is
	// gone) by resolving through a store whose version lookup fails.
	m.store = &grayGoneStore{Store: m.store}
	p, version, err := m.ResolveForSession(ctx, id, "s1")
	if err != nil {
		t.Fatalf("ResolveForSession: %v", err)
	}
	if version != 2 || p.SystemPrompt != "v2" {
		t.Errorf("fallback = v%d %q, want v2 v2", version, p.SystemPrompt)
	}
}

// grayGoneStore fails every version lookup, standing in for a vanished row.
type grayGoneStore struct{ Store }

func (s *grayGoneStore) ResolveVersion(ctx context.Context, id string, version int) (RuntimeProfile, error) {
	return RuntimeProfile{}, ErrNotFound
}
