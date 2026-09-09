package reliable

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestInboxTransitions(t *testing.T) {
	valid := [][2]InboxStatus{
		{InboxReceived, InboxProcessing},
		{InboxProcessing, InboxCompleted},
		{InboxProcessing, InboxRetryWait},
		{InboxProcessing, InboxWaitingApproval},
		{InboxProcessing, InboxDeadLetter},
		{InboxRetryWait, InboxProcessing},
		{InboxWaitingApproval, InboxProcessing},
		{InboxDeadLetter, InboxReceived},
	}
	for _, transition := range valid {
		if err := ValidateInboxTransition(transition[0], transition[1]); err != nil {
			t.Fatalf("expected %s -> %s to be valid: %v", transition[0], transition[1], err)
		}
	}
	if err := ValidateInboxTransition(InboxCompleted, InboxProcessing); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected terminal-state transition to fail, got %v", err)
	}
}

func TestOutboxTransitions(t *testing.T) {
	if err := ValidateOutboxTransition(OutboxPending, OutboxDelivering); err != nil {
		t.Fatal(err)
	}
	if err := ValidateOutboxTransition(OutboxDelivering, OutboxDispatchStarted); err != nil {
		t.Fatal(err)
	}
	if err := ValidateOutboxTransition(OutboxDelivering, OutboxDelivered); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected pre-dispatch delivery completion to fail, got %v", err)
	}
	if err := ValidateOutboxTransition(OutboxDispatchStarted, OutboxWaitingReconciliation); err != nil {
		t.Fatal(err)
	}
	if err := ValidateOutboxTransition(OutboxDelivered, OutboxDelivering); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected terminal-state transition to fail, got %v", err)
	}
}

func TestMemoryReplayOutboxForTenantRejectsCrossTenantID(t *testing.T) {
	store := NewMemoryStore()
	store.outbox[42] = &OutboxMessage{ID: 42, TenantID: "tenant-a", Status: OutboxDeadLetter}
	if err := store.ReplayOutboxForTenant(context.Background(), "tenant-b", 42, "operator", "scope check"); err == nil {
		t.Fatal("cross-tenant replay unexpectedly succeeded")
	}
	if got := store.outbox[42].Status; got != OutboxDeadLetter {
		t.Fatalf("cross-tenant replay changed status to %s", got)
	}
	if err := store.ReplayOutboxForTenant(context.Background(), "tenant-a", 42, "operator", "scope check"); err != nil {
		t.Fatalf("same-tenant replay failed: %v", err)
	}
}

func TestValidateLeaseOwner(t *testing.T) {
	cases := []struct {
		name  string
		owner string
		valid bool
	}{
		{name: "empty", owner: ""},
		{name: "invalid utf8", owner: string([]byte{0xff})},
		{name: "nul", owner: "worker\x00id"},
		{name: "newline", owner: "worker\nid"},
		{name: "format", owner: "worker\u202eid"},
		{name: "exact limit", owner: strings.Repeat("x", 256), valid: true},
		{name: "over limit", owner: strings.Repeat("x", 257)},
		{name: "multibyte within limit", owner: strings.Repeat("界", 84), valid: true},
		{name: "multibyte over limit", owner: strings.Repeat("界", 86)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLeaseOwner(tc.owner)
			if tc.valid && err != nil {
				t.Fatalf("ValidateLeaseOwner(%q): %v", tc.owner, err)
			}
			if !tc.valid && err == nil {
				t.Fatalf("ValidateLeaseOwner(%q) accepted invalid owner", tc.owner)
			}
		})
	}
}

func TestBackoffIsBounded(t *testing.T) {
	base := time.Second
	maximum := 30 * time.Second
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{5, 16 * time.Second},
		{10, 30 * time.Second},
	}
	for _, tc := range cases {
		if got := Backoff(tc.attempt, base, maximum, 0); got != tc.want {
			t.Fatalf("attempt %d: got %s want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestDeterministicJitter(t *testing.T) {
	first := DeterministicJitter(42, time.Second)
	if first != DeterministicJitter(42, time.Second) {
		t.Fatal("jitter changed for the same message")
	}
	if first < 0 || first >= time.Second {
		t.Fatalf("jitter out of range: %s", first)
	}
}

func TestErrorTextIsPostgresSafeAndUTF8Bounded(t *testing.T) {
	input := strings.Repeat("界", 1000) + "\x00" + string([]byte{0xff})
	got := errorText(errors.New(input))
	if len(got) > maxStoredErrorBytes {
		t.Fatalf("stored error is %d bytes, want <= %d", len(got), maxStoredErrorBytes)
	}
	if !utf8.ValidString(got) {
		t.Fatal("stored error is invalid UTF-8")
	}
	if strings.IndexByte(got, 0) >= 0 {
		t.Fatal("stored error contains a PostgreSQL-incompatible NUL byte")
	}
}

func TestErrorTextRedactsCredentialAssignments(t *testing.T) {
	got := errorText(errors.New("provider failed api-key=sk-live secret:abc password = hunter2"))
	for _, secret := range []string{"sk-live", "abc", "hunter2"} {
		if strings.Contains(got, secret) {
			t.Fatalf("stored error contains credential %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "api-key=<redacted>") ||
		!strings.Contains(got, "secret=<redacted>") ||
		!strings.Contains(got, "password=<redacted>") {
		t.Fatalf("credential markers missing: %s", got)
	}
}

func TestErrorTextRedactsURLUserinfoAndBearerTokens(t *testing.T) {
	got := errorText(errors.New("dial postgres://db-user:db-secret@private-db.internal/app with Authorization: Bearer live-secret-token"))
	for _, secret := range []string{"db-user:db-secret", "db-secret", "live-secret-token"} {
		if strings.Contains(got, secret) {
			t.Fatalf("stored error contains credential %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "postgres://<redacted>@private-db.internal/app") ||
		!strings.Contains(got, "Bearer <redacted>") {
		t.Fatalf("redacted diagnostic markers missing: %s", got)
	}
}
