package sessionturn

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestPostgresSummaryValidatesCoverageAndSelectsNewestCompatibleRange(t *testing.T) {
	store := newPostgresIntegrationStore(t, true)
	service, err := NewSessionServiceForTenant(store, "tenant-summary")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	ctx := context.Background()
	key := testKey("summary-lifecycle")
	sess, err := service.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []*event.Event{
		validSessionTurnEvent("summary-event-1", model.RoleUser, "one"),
		validSessionTurnEvent("summary-event-2", model.RoleAssistant, "two"),
	} {
		if err := service.AppendEvent(ctx, sess, item); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := store.Load(ctx, key)
	if err != nil || snapshot == nil {
		t.Fatalf("summary snapshot = %#v, %v", snapshot, err)
	}
	write := func(through int64, text string) SummaryWrite {
		return SummaryWrite{
			TenantID: "tenant-summary", Key: key,
			CoveredFromSequence: 1, CoveredThroughSequence: through,
			LastEventID: snapshot.Events[through-1].ID, SessionVersion: snapshot.Version,
			SummaryVersion: 1, BoundaryVersion: session.SummaryBoundaryVersion,
			GeneratorVersion: "test-generator", SourceSHA256: summarySourceHashSnapshot(snapshot.Events, 1, through, ""),
			SummaryText: text, Events: snapshot.Events,
		}
	}
	if err := store.PutSummary(ctx, write(1, "one-summary")); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSummary(ctx, write(2, "two-summary")); err != nil {
		t.Fatal(err)
	}
	got, ok := store.SummaryForTenant(ctx, "tenant-summary", key, snapshot, "")
	if !ok || got.Summary != "two-summary" {
		t.Fatalf("newest compatible summary = %#v, ok=%v", got, ok)
	}
	invalid := write(2, "bad-source")
	invalid.SourceSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := store.PutSummary(ctx, invalid); !errors.Is(err, ErrSummaryBoundary) {
		t.Fatalf("source hash mismatch = %v, want ErrSummaryBoundary", err)
	}
	invalid = write(1, "bad-last-event")
	invalid.LastEventID = "not-the-covered-event"
	if err := store.PutSummary(ctx, invalid); !errors.Is(err, ErrSummaryBoundary) {
		t.Fatalf("last event mismatch = %v, want ErrSummaryBoundary", err)
	}
	invalid = write(1, "bad-boundary")
	invalid.BoundaryVersion = session.SummaryBoundaryVersion + 1
	if err := store.PutSummary(ctx, invalid); !errors.Is(err, ErrSummaryBoundary) {
		t.Fatalf("boundary version mismatch = %v, want ErrSummaryBoundary", err)
	}
}
