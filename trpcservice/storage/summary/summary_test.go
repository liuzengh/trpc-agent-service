package summary_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/summary"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/summary/inmemory"
)

func TestSupersededSummaryCanBeCompactedOnlyAfterHigherWatermark(t *testing.T) {
	store := inmemory.New()
	now := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	old := summary.Body{Key: summary.Key{TenantID: "t", AgentAppID: "a", SessionID: "s", SummaryID: "one"}, ContentRef: "summary://one", Content: []byte("old")}
	next := summary.Body{Key: summary.Key{TenantID: "t", AgentAppID: "a", SessionID: "s", SummaryID: "two"}, ContentRef: "summary://two", Content: []byte("new")}
	if _, err := store.Put(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	store.SetWatermark(old.Key, 10)
	store.SetWatermark(next.Key, 20)
	if err := store.Supersede(context.Background(), old.Key, next.SummaryID, now); err != nil {
		t.Fatal(err)
	}
	if got, err := (summary.Compactor{Store: store, Owner: "worker", Now: func() time.Time { return now }}).RunOnce(context.Background()); err != nil || got != 1 {
		t.Fatalf("compact got=%d err=%v", got, err)
	}
	if _, err := store.Get(context.Background(), old.Key); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("old err=%v", err)
	}
	if got, err := store.Get(context.Background(), next.Key); err != nil || string(got.Content) != "new" {
		t.Fatalf("next=%+v err=%v", got, err)
	}
}

func TestSupersedeRejectsStaleReplacement(t *testing.T) {
	store := inmemory.New()
	first := summary.Body{Key: summary.Key{TenantID: "t", AgentAppID: "a", SessionID: "s", SummaryID: "one"}, ContentRef: "one", Content: []byte("one")}
	second := summary.Body{Key: summary.Key{TenantID: "t", AgentAppID: "a", SessionID: "s", SummaryID: "two"}, ContentRef: "two", Content: []byte("two")}
	_, _ = store.Put(context.Background(), first)
	_, _ = store.Put(context.Background(), second)
	store.SetWatermark(first.Key, 2)
	store.SetWatermark(second.Key, 2)
	if err := store.Supersede(context.Background(), first.Key, second.SummaryID, time.Now()); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("err=%v", err)
	}
}
