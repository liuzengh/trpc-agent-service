package inmemory

import (
	"context"
	"errors"
	"testing"
	"time"
)

//nolint:gocyclo // Exercises the complete list contract in one focused fixture.
func TestListScopesFiltersAndPaginatesAppsAndRevisions(t *testing.T) {
	repository := NewRepository()
	first := createApp(t, repository, tenantOne, "first")
	second := createApp(t, repository, tenantOne, "second")
	_ = createApp(t, repository, tenantTwo, "second")

	page, next, err := repository.List(context.Background(), tenantOne, "", "draft", "", 1)
	if err != nil || len(page) != 1 || next == "" {
		t.Fatalf("first app page = items=%+v next=%q err=%v", page, next, err)
	}
	page, next, err = repository.List(context.Background(), tenantOne, "", "draft", next, 1)
	if err != nil || len(page) != 1 || next != "" {
		t.Fatalf("second app page = items=%+v next=%q err=%v", page, next, err)
	}
	filtered, _, err := repository.List(context.Background(), tenantOne, "second", "", "", 50)
	if err != nil || len(filtered) != 1 || filtered[0].AppID != second.AppID || filtered[0].AppID == first.AppID {
		t.Fatalf("filtered app list = items=%+v err=%v", filtered, err)
	}

	firstDraft := createDraft(t, repository, first, draftConfiguration("first"))
	secondDraft := createDraft(t, repository, first, draftConfiguration("second"))
	revisions, next, err := repository.ListRevisions(context.Background(), tenantOne, first.AppID, "", "draft", "", 1)
	if err != nil || len(revisions) != 1 || next == "" || revisions[0].Revision != firstDraft.Revision {
		t.Fatalf("first revision page = items=%+v next=%q err=%v", revisions, next, err)
	}
	revisions, next, err = repository.ListRevisions(context.Background(), tenantOne, first.AppID, "", "draft", next, 1)
	if err != nil || len(revisions) != 1 || next != "" || revisions[0].Revision != secondDraft.Revision {
		t.Fatalf("second revision page = items=%+v next=%q err=%v", revisions, next, err)
	}
	filteredRevisions, _, err := repository.ListRevisions(context.Background(), tenantOne, first.AppID, "second", "", "", 50)
	if err != nil || len(filteredRevisions) != 1 || filteredRevisions[0].Revision != secondDraft.Revision {
		t.Fatalf("filtered revisions = items=%+v err=%v", filteredRevisions, err)
	}
}

//nolint:gocyclo // Exercises cancellation, cursor, and paging boundaries together.
func TestListBoundaryReturnsContextCursorAndEmptyPageErrors(t *testing.T) {
	repository := NewRepository()
	app := createApp(t, repository, tenantOne, "boundary")
	firstDraft := createDraft(t, repository, app, draftConfiguration("boundary"))
	if _, _, err := repository.List(context.Background(), tenantOne, "", "active", "", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.ListRevisions(context.Background(), tenantOne, app.AppID, "", "published", "", 1); err != nil {
		t.Fatal(err)
	}
	for _, call := range []func(context.Context) error{
		func(ctx context.Context) error {
			_, _, err := repository.List(ctx, tenantOne, "", "", "bad", 1)
			return err
		},
		func(ctx context.Context) error {
			_, _, err := repository.ListRevisions(ctx, tenantOne, app.AppID, "", "", "bad", 1)
			return err
		},
	} {
		if err := call(context.Background()); err == nil {
			t.Fatal("invalid cursor was accepted")
		}
	}
	if items, next, err := repository.List(context.Background(), tenantOne, "", "", "0", 0); err != nil || len(items) != 1 || next != "" {
		t.Fatalf("default app limit = items=%+v next=%q err=%v", items, next, err)
	}
	if items, next, err := repository.List(context.Background(), tenantOne, "", "", "99", 201); err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("past-end app page = items=%+v next=%q err=%v", items, next, err)
	}
	if items, next, err := repository.List(context.Background(), tenantOne, "", "", "", 201); err != nil || len(items) != 1 || next != "" {
		t.Fatalf("maximum app limit = items=%+v next=%q err=%v", items, next, err)
	}
	if items, next, err := repository.ListRevisions(context.Background(), tenantOne, app.AppID, "", "", "0", 0); err != nil || len(items) != 1 || next != "" || items[0].Revision != firstDraft.Revision {
		t.Fatalf("default revision limit = items=%+v next=%q err=%v", items, next, err)
	}

	if err := repository.mu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := repository.List(ctx, tenantOne, "", "", "", 1)
		done <- err
	}()
	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting List error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("List did not observe cancellation while waiting for lock")
	}
	repository.mu.unlock()
	if err := repository.mu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	done = make(chan error, 1)
	go func() {
		_, _, err := repository.ListRevisions(ctx, tenantOne, app.AppID, "", "", "", 1)
		done <- err
	}()
	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting ListRevisions error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ListRevisions did not observe cancellation while waiting for lock")
	}
	repository.mu.unlock()

	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if _, _, err := repository.List(ctx, tenantOne, "", "", "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled List error = %v", err)
	}
	if _, _, err := repository.ListRevisions(ctx, tenantOne, app.AppID, "", "", "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ListRevisions error = %v", err)
	}
}
