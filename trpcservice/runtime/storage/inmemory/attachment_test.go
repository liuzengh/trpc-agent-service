package inmemory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestAttachmentStoreScopesBindsAndCleansUp(t *testing.T) {
	store := New()
	t.Cleanup(func() { _ = store.Close() })
	data := []byte("document")
	digest := sha256.Sum256(data)
	stored, err := store.PutAttachment(context.Background(), "tenant-a", attachment.Upload{ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data))}, strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("PutAttachment = %v", err)
	}
	reference := attachment.Reference{ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if stored != reference {
		t.Fatalf("stored reference = %+v", stored)
	}
	if err := store.BindAttachments(context.Background(), "tenant-a", "event-1", []attachment.Reference{reference}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("binding unknown event = %v", err)
	}
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-1", nil); err != nil {
		t.Fatalf("CreateSession = %v", err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-1", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1"}); err != nil {
		t.Fatalf("RecordMessage = %v", err)
	}
	if err := store.BindAttachments(context.Background(), "tenant-a", "event-1", []attachment.Reference{reference}); err != nil {
		t.Fatalf("BindAttachments = %v", err)
	}
	content, err := store.Load(context.Background(), "tenant-a", "event-1", reference)
	if err != nil || string(content.Data) != string(data) {
		t.Fatalf("Load = %q, %v", content.Data, err)
	}
	if _, err := store.Load(context.Background(), "tenant-b", "event-1", reference); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant Load = %v", err)
	}
	reference.Size++
	if _, err := store.Load(context.Background(), "tenant-a", "event-1", reference); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("size mismatch Load = %v", err)
	}
	if removed, err := store.CleanupAttachments(context.Background(), "tenant-a", time.Now().UTC().Add(attachment.DefaultRetention)); err != nil || removed != 0 {
		t.Fatalf("bound cleanup = %d, %v", removed, err)
	}
	if err := store.DeleteSession(context.Background(), "tenant-a", "session-1"); err != nil {
		t.Fatalf("DeleteSession = %v", err)
	}
	if removed, err := store.CleanupAttachments(context.Background(), "tenant-a", time.Now().UTC().Add(attachment.DefaultRetention+time.Second)); err != nil || removed != 1 {
		t.Fatalf("orphaned cleanup = %d, %v", removed, err)
	}
}

func TestDeleteSessionDoesNotUnbindAnotherTenantAttachment(t *testing.T) {
	store := New()
	t.Cleanup(func() { _ = store.Close() })
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if _, err := store.CreateSession(context.Background(), tenantID, "session-1", nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: tenantID, EventID: "event-shared", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1"}); err != nil {
			t.Fatal(err)
		}
	}
	data := []byte("tenant-b-data")
	digest := sha256.Sum256(data)
	reference, err := store.PutAttachment(context.Background(), "tenant-b", attachment.Upload{ID: "attachment-b", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data))}, strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	if reference.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("reference digest = %q", reference.SHA256)
	}
	if err := store.BindAttachments(context.Background(), "tenant-b", "event-shared", []attachment.Reference{reference}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSession(context.Background(), "tenant-a", "session-1"); err != nil {
		t.Fatal(err)
	}
	if content, err := store.Load(context.Background(), "tenant-b", "event-shared", reference); err != nil || string(content.Data) != string(data) {
		t.Fatalf("tenant-b attachment after tenant-a delete = %q, %v", content.Data, err)
	}
}

func TestAttachmentStoreRejectsInvalidInputsAndConflicts(t *testing.T) {
	store := New()
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	data := []byte("document")
	upload := attachment.Upload{ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data))}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.PutAttachment(canceled, "tenant-a", upload, strings.NewReader(string(data))); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled PutAttachment = %v", err)
	}
	if _, err := store.PutAttachment(ctx, "", upload, strings.NewReader(string(data))); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid tenant PutAttachment = %v", err)
	}
	if _, err := store.PutAttachment(ctx, "tenant-a", upload, nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil content PutAttachment = %v", err)
	}
	if _, err := store.PutAttachment(ctx, "tenant-a", attachment.Upload{ID: "bad", Kind: attachment.KindImage, MIMEType: "application/pdf", Size: 1}, strings.NewReader("x")); !errors.Is(err, attachment.ErrInvalid) {
		t.Fatalf("invalid upload PutAttachment = %v", err)
	}
	if _, err := store.PutAttachment(ctx, "tenant-a", upload, failingAttachmentReader{}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("reader failure PutAttachment = %v", err)
	}
	if _, err := store.PutAttachment(ctx, "tenant-a", upload, strings.NewReader("short")); !errors.Is(err, attachment.ErrInvalid) {
		t.Fatalf("size mismatch PutAttachment = %v", err)
	}

	reference, err := store.PutAttachment(ctx, "tenant-a", upload, strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	conflict := upload
	conflict.Size = int64(len("documenz"))
	if _, err := store.PutAttachment(ctx, "tenant-a", conflict, strings.NewReader("documenz")); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("conflicting attachment PutAttachment = %v", err)
	}

	store.mu.Lock()
	delete(store.attachments, key("tenant-a", reference.ID))
	store.mu.Unlock()
	if _, err := store.PutAttachment(ctx, "tenant-a", conflict, strings.NewReader("documenz")); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("object conflict PutAttachment = %v", err)
	}
}

type failingAttachmentReader struct{}

func (failingAttachmentReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func TestAttachmentStoreBindFailures(t *testing.T) {
	store, reference := newAttachmentStoreBindingFixture(t)
	ctx := context.Background()
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	if err := store.BindAttachments(canceled, "tenant-a", "event-1", []attachment.Reference{reference}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled BindAttachments = %v", err)
	}
	if err := store.BindAttachments(ctx, "", "event-1", []attachment.Reference{reference}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid tenant BindAttachments = %v", err)
	}
	if err := store.BindAttachments(ctx, "tenant-a", "", []attachment.Reference{reference}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid event BindAttachments = %v", err)
	}
	badReference := reference
	badReference.ID = "bad\nid"
	if err := store.BindAttachments(ctx, "tenant-a", "event-1", []attachment.Reference{badReference}); !errors.Is(err, attachment.ErrInvalid) {
		t.Fatalf("invalid reference BindAttachments = %v", err)
	}
	missing := reference
	missing.ID = "attachment-missing"
	if err := store.BindAttachments(ctx, "tenant-a", "event-1", []attachment.Reference{missing}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing attachment BindAttachments = %v", err)
	}
	if err := store.BindAttachments(ctx, "tenant-a", "event-1", []attachment.Reference{reference}); err != nil {
		t.Fatal(err)
	}
	if err := store.BindAttachments(ctx, "tenant-a", "event-2", []attachment.Reference{reference}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("rebind conflict = %v", err)
	}
}

func TestAttachmentStoreLoadFailures(t *testing.T) {
	store, reference := newAttachmentStoreBindingFixture(t)
	ctx := context.Background()
	if err := store.BindAttachments(ctx, "tenant-a", "event-1", []attachment.Reference{reference}); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := store.Load(canceled, "tenant-a", "event-1", reference); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Load = %v", err)
	}
	if _, err := store.Load(ctx, "", "event-1", reference); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid tenant Load = %v", err)
	}
	if _, err := store.Load(ctx, "tenant-a", "", reference); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid event Load = %v", err)
	}
	badReference := reference
	badReference.ID = "bad\nid"
	if _, err := store.Load(ctx, "tenant-a", "event-1", badReference); !errors.Is(err, attachment.ErrInvalid) {
		t.Fatalf("invalid reference Load = %v", err)
	}
	if _, err := store.Load(ctx, "tenant-a", "event-2", reference); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("wrong event Load = %v", err)
	}
}

func TestAttachmentStoreCleanupFailures(t *testing.T) {
	store, _ := newAttachmentStoreBindingFixture(t)
	ctx := context.Background()
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	if removed, err := store.CleanupAttachments(canceled, "tenant-a", time.Now()); !errors.Is(err, context.Canceled) || removed != 0 {
		t.Fatalf("canceled CleanupAttachments = %d, %v", removed, err)
	}
	if removed, err := store.CleanupAttachments(ctx, "", time.Now()); !errors.Is(err, runtimestorage.ErrInvalid) || removed != 0 {
		t.Fatalf("invalid tenant CleanupAttachments = %d, %v", removed, err)
	}
	if removed, err := store.CleanupAttachments(ctx, "tenant-a", time.Time{}); !errors.Is(err, runtimestorage.ErrInvalid) || removed != 0 {
		t.Fatalf("zero before CleanupAttachments = %d, %v", removed, err)
	}
}

func newAttachmentStoreBindingFixture(t *testing.T) (*Store, attachment.Reference) {
	t.Helper()
	store := New()
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	data := []byte("document")
	reference, err := store.PutAttachment(ctx, "tenant-a", attachment.Upload{ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data))}, strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session-1", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-1", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-2", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-2"}); err != nil {
		t.Fatal(err)
	}
	return store, reference
}
