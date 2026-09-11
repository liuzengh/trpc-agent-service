package wecommcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRecoveryBoundsAndDeduplicationSurvive(t *testing.T) {
	b := fixtureBinding()
	b.Status = "disabled"
	b.Version = 2
	cfg, _ := ParseBinding(b)
	now := time.Now().UTC().Truncate(time.Second)
	start := now.Add(-2 * time.Hour)
	cfg.StartAt = start.Format(time.RFC3339)
	b.Config, _ = json.Marshal(cfg)
	s := NewMemoryStore()
	key := PollKey{b.TenantID, b.ID, endpointHash("group-1")}
	cp, _ := s.Checkpoint(context.Background(), key, ConfigFingerprint(b, cfg), start)
	if err := s.Advance(context.Background(), key, cp, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	_ = s.MarkSeen(context.Background(), key, "already-processed")
	if _, err := s.RecoverCheckpoint(context.Background(), b, key.ChatHash, 2, "resume", now.Add(-time.Minute), false); err == nil {
		t.Fatal("gap silently skipped")
	}
	next, err := s.RecoverCheckpoint(context.Background(), b, key.ChatHash, 2, "resume", now.Add(-time.Minute), true)
	if err != nil || next.Version != 3 || !next.Floor.Equal(now.Add(-time.Minute)) {
		t.Fatal("resume floor not recorded")
	}
	if _, err := s.RecoverCheckpoint(context.Background(), b, key.ChatHash, 2, "resume", now.Add(-time.Minute), true); err == nil {
		t.Fatal("stale recovery version accepted")
	}
	next, err = s.RecoverCheckpoint(context.Background(), b, key.ChatHash, 3, "rewind", start.Add(time.Minute), false)
	if err != nil || next.Version != 4 {
		t.Fatal("explicit rewind rejected")
	}
	if seen, err := s.Seen(context.Background(), key, "already-processed"); err != nil || !seen {
		t.Fatal("rewind cleared idempotency")
	}
	for _, changed := range []string{"active", "wrong_group", "too_old", "future", "changed_config"} {
		input := b
		chat := key.ChatHash
		from := start.Add(time.Minute)
		switch changed {
		case "active":
			input.Status = "active"
		case "wrong_group":
			chat = "other"
		case "too_old":
			from = now.Add(-8 * 24 * time.Hour)
		case "future":
			from = now.Add(time.Hour)
		case "changed_config":
			cfg.MentionPrefix = "@changed"
			input.Config, _ = json.Marshal(cfg)
		}
		if _, err := s.RecoverCheckpoint(context.Background(), input, chat, 4, "rewind", from, false); err == nil {
			t.Fatalf("unsafe recovery accepted: %s", changed)
		}
	}
}
