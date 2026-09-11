package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestPublicationReturnsPersistedRepresentationAgainstPostgreSQL(t *testing.T) {
	store, _ := credentialPostgres(t)
	ctx := context.Background()
	if err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
		draft, err := tx.GetDraft(ctx)
		if err != nil {
			return err
		}
		draft.Revision = 2
		return tx.SaveDraft(ctx, draft, 1)
	}); err != nil {
		t.Fatal(err)
	}
	candidate := domain.ProfileRevision{
		ID: "persisted-publication", TenantID: "tenant-a", ProfileID: "profile-a",
		SourceDraftRevision: 2, SchemaVersion: "v1", Spec: json.RawMessage(`{"z":2,"a":1}`),
		SpecDigest: "sha256:" + strings.Repeat("a", 64), PublishedBy: "actor",
		PublishedAt: time.Date(2026, 9, 4, 12, 0, 0, 123456789, time.UTC),
	}
	first, created, err := store.PublishProfileRevision(ctx, candidate, 2)
	if err != nil || !created {
		t.Fatalf("first publication: created=%v err=%v", created, err)
	}
	fetched, err := store.GetProfileRevision(ctx, "tenant-a", "profile-a", first.RevisionNumber)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, fetched) {
		t.Fatalf("first publication differs from persisted representation: first=%#v persisted=%#v", first, fetched)
	}
	retry, created, err := store.PublishProfileRevision(ctx, candidate, 2)
	if err != nil || created {
		t.Fatalf("idempotent publication: created=%v err=%v", created, err)
	}
	if !reflect.DeepEqual(first, retry) {
		t.Fatalf("idempotent publication changed fields: first=%#v retry=%#v", first, retry)
	}
}

func TestCredentialSaveAndPublicationConcurrentAgainstPostgreSQL(t *testing.T) {
	store, _ := credentialPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for expected := int64(1); expected <= 12; expected++ {
		start := make(chan struct{})
		saved := make(chan error, 1)
		published := make(chan error, 1)
		go func() {
			<-start
			saved <- store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
				draft, err := tx.GetDraft(ctx)
				if err != nil {
					return err
				}
				draft.Revision = expected + 1
				return tx.SaveDraft(ctx, draft, expected)
			})
		}()
		go func() {
			<-start
			candidate := domain.ProfileRevision{
				ID: fmt.Sprintf("publication-%d", expected), TenantID: "tenant-a", ProfileID: "profile-a",
				SourceDraftRevision: expected, SchemaVersion: "v1", Spec: json.RawMessage(`{}`),
				SpecDigest: "sha256:" + strings.Repeat("a", 64), PublishedBy: "actor", PublishedAt: time.Now().UTC(),
			}
			_, _, err := store.PublishProfileRevision(ctx, candidate, expected)
			published <- err
		}()
		close(start)
		if err := <-saved; err != nil {
			t.Fatalf("concurrent draft save: %v", err)
		}
		if err := <-published; err != nil && !errors.Is(err, application.ErrDraftRevisionConflict) {
			t.Fatalf("concurrent publication must succeed or lose CAS, not deadlock: %v", err)
		}
	}
}
