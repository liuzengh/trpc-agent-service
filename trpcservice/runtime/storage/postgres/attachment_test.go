package postgres_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
)

var runtimeAttachmentColumns = []string{"tenant_id", "attachment_id", "kind", "mime_type", "name", "size", "sha256", "provider", "provider_id", "event_id", "expires_at"}

func TestPostgresAttachmentLifecycleContracts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := runtimepostgres.New(db)
	data := []byte("document")
	reference := attachmentReference(data)
	when := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,event_id,expires_at FROM public.runtime_attachment WHERE tenant_id=$1 AND attachment_id=$2 FOR UPDATE")).
		WithArgs("tenant-a", reference.ID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_object (tenant_id,object_key,content_type,content,size,etag) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,object_key) DO NOTHING")).
		WithArgs("tenant-a", reference.ID, reference.MIMEType, data, reference.Size, reference.SHA256).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT content_type,size,etag FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2")).
		WithArgs("tenant-a", reference.ID).WillReturnRows(sqlmock.NewRows([]string{"content_type", "size", "etag"}).AddRow(reference.MIMEType, reference.Size, reference.SHA256))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_attachment (tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (tenant_id,attachment_id) DO NOTHING")).
		WithArgs("tenant-a", reference.ID, reference.Kind, reference.MIMEType, reference.Name, reference.Size, reference.SHA256, reference.Provider, reference.ProviderID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	stored, err := store.PutAttachment(context.Background(), "tenant-a", attachment.Upload{ID: reference.ID, Kind: reference.Kind, MIMEType: reference.MIMEType, Size: reference.Size}, strings.NewReader(string(data)))
	if err != nil || stored != reference {
		t.Fatalf("PutAttachment = %+v, %v", stored, err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS(SELECT 1 FROM public.message_event WHERE tenant_id=$1 AND event_id=$2)")).
		WithArgs("tenant-a", "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,event_id,expires_at FROM public.runtime_attachment WHERE tenant_id=$1 AND attachment_id=$2 FOR UPDATE")).
		WithArgs("tenant-a", reference.ID).WillReturnRows(runtimeAttachmentRow(reference, nil, when.Add(time.Hour)))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE public.runtime_attachment SET event_id=$3 WHERE tenant_id=$1 AND attachment_id=$2 AND (event_id IS NULL OR event_id=$3)")).
		WithArgs("tenant-a", reference.ID, "event-a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.BindAttachments(context.Background(), "tenant-a", "event-a", []attachment.Reference{reference}); err != nil {
		t.Fatalf("BindAttachments = %v; expectations = %v", err, mock.ExpectationsWereMet())
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,event_id,expires_at FROM public.runtime_attachment WHERE tenant_id=$1 AND attachment_id=$2")).
		WithArgs("tenant-a", reference.ID).WillReturnRows(runtimeAttachmentRow(reference, "event-a", when.Add(time.Hour)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT content FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2")).
		WithArgs("tenant-a", reference.ID).WillReturnRows(sqlmock.NewRows([]string{"content"}).AddRow(data))
	content, err := store.Load(context.Background(), "tenant-a", "event-a", reference)
	if err != nil || string(content.Data) != string(data) {
		t.Fatalf("Load = %q, %v", content.Data, err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("DELETE FROM public.runtime_attachment AS a WHERE a.tenant_id=$1 AND a.expires_at <= $2 AND (a.event_id IS NULL OR EXISTS (SELECT 1 FROM public.message_event AS e WHERE e.tenant_id=a.tenant_id AND e.event_id=a.event_id AND e.status IN ('completed','failed'))) RETURNING a.attachment_id")).
		WithArgs("tenant-a", when).WillReturnRows(sqlmock.NewRows([]string{"attachment_id"}).AddRow(reference.ID))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2")).WithArgs("tenant-a", reference.ID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	removed, err := store.CleanupAttachments(context.Background(), "tenant-a", when)
	if err != nil || removed != 1 {
		t.Fatalf("CleanupAttachments = %d, %v", removed, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAttachmentConcurrentPutKeepsExactReference(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	data := []byte("document")
	reference := attachmentReference(data)
	when := time.Now().UTC().Add(time.Hour)
	mock.ExpectBegin()
	mock.ExpectQuery("FROM public.runtime_attachment WHERE tenant_id=\\$1 AND attachment_id=\\$2 FOR UPDATE").WithArgs("tenant-a", reference.ID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public.runtime_object").WithArgs("tenant-a", reference.ID, reference.MIMEType, data, reference.Size, reference.SHA256).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT content_type,size,etag FROM public.runtime_object").WithArgs("tenant-a", reference.ID).WillReturnRows(sqlmock.NewRows([]string{"content_type", "size", "etag"}).AddRow(reference.MIMEType, reference.Size, reference.SHA256))
	mock.ExpectExec("INSERT INTO public.runtime_attachment").WithArgs("tenant-a", reference.ID, reference.Kind, reference.MIMEType, reference.Name, reference.Size, reference.SHA256, reference.Provider, reference.ProviderID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("FROM public.runtime_attachment WHERE tenant_id=\\$1 AND attachment_id=\\$2 FOR UPDATE").WithArgs("tenant-a", reference.ID).WillReturnRows(runtimeAttachmentRow(reference, nil, when))
	mock.ExpectCommit()
	stored, err := runtimepostgres.New(db).PutAttachment(context.Background(), "tenant-a", attachment.Upload{ID: reference.ID, Kind: reference.Kind, MIMEType: reference.MIMEType, Size: reference.Size}, strings.NewReader(string(data)))
	if err != nil || stored != reference {
		t.Fatalf("PutAttachment concurrent result = %+v, %v; expectations = %v", stored, err, mock.ExpectationsWereMet())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAttachmentPutValidationAndPrepareErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := runtimepostgres.New(db)
	data := []byte("document")
	reference := attachmentReference(data)
	upload := attachmentUpload(reference)

	for _, test := range []struct {
		name    string
		tenant  string
		upload  attachment.Upload
		content io.Reader
		want    error
	}{
		{name: "invalid tenant", tenant: "", upload: upload, content: strings.NewReader(string(data)), want: runtimestorage.ErrInvalid},
		{name: "nil content", tenant: "tenant-a", upload: upload, want: runtimestorage.ErrInvalid},
		{name: "invalid upload", tenant: "tenant-a", upload: attachment.Upload{ID: "", Kind: reference.Kind, MIMEType: reference.MIMEType, Size: reference.Size}, content: strings.NewReader(string(data)), want: attachment.ErrInvalid},
		{name: "reader error", tenant: "tenant-a", upload: upload, content: errReader{}, want: runtimestorage.ErrStorage},
		{name: "size mismatch", tenant: "tenant-a", upload: attachment.Upload{ID: reference.ID, Kind: reference.Kind, MIMEType: reference.MIMEType, Size: reference.Size + 1}, content: strings.NewReader(string(data)), want: attachment.ErrInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := store.PutAttachment(context.Background(), test.tenant, test.upload, test.content)
			if !errors.Is(err, test.want) {
				t.Fatalf("PutAttachment error = %v, want %v", err, test.want)
			}
		})
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAttachmentPutFailureBoundaries(t *testing.T) {
	data := []byte("document")
	reference := attachmentReference(data)
	expiresAt := time.Now().UTC().Add(time.Hour)
	for _, test := range []struct {
		name    string
		prepare func(sqlmock.Sqlmock)
		want    error
	}{
		{
			name: "begin failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "existing reference commits",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentLookup(mock, reference.ID, true).WillReturnRows(runtimeAttachmentRow(reference, nil, expiresAt))
				mock.ExpectCommit()
			},
		},
		{
			name: "existing reference conflict",
			prepare: func(mock sqlmock.Sqlmock) {
				conflict := reference
				conflict.Name = "other.pdf"
				mock.ExpectBegin()
				expectAttachmentLookup(mock, reference.ID, true).WillReturnRows(runtimeAttachmentRow(conflict, nil, expiresAt))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrConflict,
		},
		{
			name: "initial lookup failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentLookup(mock, reference.ID, true).WillReturnError(errors.New("lookup failed"))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "object insert failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentLookup(mock, reference.ID, true).WillReturnError(sql.ErrNoRows)
				expectObjectInsert(mock, reference, data).WillReturnError(errors.New("object insert failed"))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "object metadata conflict",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentLookup(mock, reference.ID, true).WillReturnError(sql.ErrNoRows)
				expectObjectInsert(mock, reference, data).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("SELECT content_type,size,etag FROM public.runtime_object").
					WithArgs("tenant-a", reference.ID).WillReturnRows(sqlmock.NewRows([]string{"content_type", "size", "etag"}).AddRow("application/json", reference.Size, reference.SHA256))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrConflict,
		},
		{
			name: "metadata rows affected failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentLookup(mock, reference.ID, true).WillReturnError(sql.ErrNoRows)
				expectObjectPersisted(mock, reference, data)
				expectMetadataInsert(mock, reference).WillReturnResult(sqlmock.NewErrorResult(errors.New("rows failed")))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "metadata race conflict",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentLookup(mock, reference.ID, true).WillReturnError(sql.ErrNoRows)
				expectObjectPersisted(mock, reference, data)
				expectMetadataInsert(mock, reference).WillReturnResult(sqlmock.NewResult(0, 0))
				expectAttachmentLookup(mock, reference.ID, true).WillReturnError(sql.ErrNoRows)
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrConflict,
		},
		{
			name: "commit failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentLookup(mock, reference.ID, true).WillReturnError(sql.ErrNoRows)
				expectObjectPersisted(mock, reference, data)
				expectMetadataInsert(mock, reference).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
			},
			want: runtimestorage.ErrStorage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			test.prepare(mock)
			got, err := runtimepostgres.New(db).PutAttachment(context.Background(), "tenant-a", attachmentUpload(reference), strings.NewReader(string(data)))
			if test.want == nil {
				if err != nil || got != reference {
					t.Fatalf("PutAttachment = %+v, %v", got, err)
				}
			} else if !errors.Is(err, test.want) {
				t.Fatalf("PutAttachment error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresAttachmentBindFailureBoundaries(t *testing.T) {
	data := []byte("document")
	reference := attachmentReference(data)
	expiresAt := time.Now().UTC().Add(time.Hour)
	for _, test := range []struct {
		name       string
		references []attachment.Reference
		prepare    func(sqlmock.Sqlmock)
		want       error
	}{
		{
			name:       "begin failure",
			references: []attachment.Reference{reference},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name:       "missing event",
			references: []attachment.Reference{reference},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectMessageEvent(mock, "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrNotFound,
		},
		{
			name:       "event lookup failure",
			references: []attachment.Reference{reference},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectMessageEvent(mock, "event-a").WillReturnError(errors.New("event lookup failed"))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "invalid reference",
			references: []attachment.Reference{{
				ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "image/png", Size: reference.Size, SHA256: reference.SHA256,
			}},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectMessageEvent(mock, "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				mock.ExpectRollback()
			},
			want: attachment.ErrInvalid,
		},
		{
			name:       "missing attachment",
			references: []attachment.Reference{reference},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectMessageEvent(mock, "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				expectAttachmentLookup(mock, reference.ID, true).WillReturnError(sql.ErrNoRows)
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrNotFound,
		},
		{
			name:       "expired attachment",
			references: []attachment.Reference{reference},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectMessageEvent(mock, "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				expectAttachmentLookup(mock, reference.ID, true).WillReturnRows(runtimeAttachmentRow(reference, nil, time.Now().UTC().Add(-time.Hour)))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrConflict,
		},
		{
			name:       "already bound elsewhere",
			references: []attachment.Reference{reference},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectMessageEvent(mock, "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				expectAttachmentLookup(mock, reference.ID, true).WillReturnRows(runtimeAttachmentRow(reference, "event-b", expiresAt))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrConflict,
		},
		{
			name:       "update failure",
			references: []attachment.Reference{reference},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectMessageEvent(mock, "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				expectAttachmentLookup(mock, reference.ID, true).WillReturnRows(runtimeAttachmentRow(reference, nil, expiresAt))
				expectAttachmentBind(mock, reference.ID, "event-a").WillReturnError(errors.New("bind failed"))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name:       "commit failure",
			references: []attachment.Reference{reference},
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectMessageEvent(mock, "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				expectAttachmentLookup(mock, reference.ID, true).WillReturnRows(runtimeAttachmentRow(reference, nil, expiresAt))
				expectAttachmentBind(mock, reference.ID, "event-a").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
			},
			want: runtimestorage.ErrStorage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			test.prepare(mock)
			err = runtimepostgres.New(db).BindAttachments(context.Background(), "tenant-a", "event-a", test.references)
			if !errors.Is(err, test.want) {
				t.Fatalf("BindAttachments error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresAttachmentLoadFailureBoundaries(t *testing.T) {
	data := []byte("document")
	reference := attachmentReference(data)
	expiresAt := time.Now().UTC().Add(time.Hour)
	for _, test := range []struct {
		name      string
		reference attachment.Reference
		tenant    string
		eventID   string
		prepare   func(sqlmock.Sqlmock)
		want      error
	}{
		{name: "invalid tenant", tenant: "", eventID: "event-a", reference: reference, want: runtimestorage.ErrInvalid},
		{name: "invalid event", tenant: "tenant-a", eventID: "", reference: reference, want: runtimestorage.ErrInvalid},
		{name: "invalid reference", tenant: "tenant-a", eventID: "event-a", reference: attachment.Reference{ID: "bad", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: 1, SHA256: "bad"}, want: attachment.ErrInvalid},
		{
			name: "lookup failure", tenant: "tenant-a", eventID: "event-a", reference: reference,
			prepare: func(mock sqlmock.Sqlmock) {
				expectAttachmentLookup(mock, reference.ID, false).WillReturnError(errors.New("lookup failed"))
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "missing attachment", tenant: "tenant-a", eventID: "event-a", reference: reference,
			prepare: func(mock sqlmock.Sqlmock) {
				expectAttachmentLookup(mock, reference.ID, false).WillReturnError(sql.ErrNoRows)
			},
			want: runtimestorage.ErrNotFound,
		},
		{
			name: "unbound attachment", tenant: "tenant-a", eventID: "event-a", reference: reference,
			prepare: func(mock sqlmock.Sqlmock) {
				expectAttachmentLookup(mock, reference.ID, false).WillReturnRows(runtimeAttachmentRow(reference, nil, expiresAt))
			},
			want: runtimestorage.ErrNotFound,
		},
		{
			name: "wrong event", tenant: "tenant-a", eventID: "event-a", reference: reference,
			prepare: func(mock sqlmock.Sqlmock) {
				expectAttachmentLookup(mock, reference.ID, false).WillReturnRows(runtimeAttachmentRow(reference, "event-b", expiresAt))
			},
			want: runtimestorage.ErrNotFound,
		},
		{
			name: "expired", tenant: "tenant-a", eventID: "event-a", reference: reference,
			prepare: func(mock sqlmock.Sqlmock) {
				expectAttachmentLookup(mock, reference.ID, false).WillReturnRows(runtimeAttachmentRow(reference, "event-a", time.Now().UTC().Add(-time.Hour)))
			},
			want: runtimestorage.ErrNotFound,
		},
		{
			name: "object read failure", tenant: "tenant-a", eventID: "event-a", reference: reference,
			prepare: func(mock sqlmock.Sqlmock) {
				expectAttachmentLookup(mock, reference.ID, false).WillReturnRows(runtimeAttachmentRow(reference, "event-a", expiresAt))
				mock.ExpectQuery("SELECT content FROM public.runtime_object").WithArgs("tenant-a", reference.ID).WillReturnError(errors.New("object read failed"))
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "content digest mismatch", tenant: "tenant-a", eventID: "event-a", reference: reference,
			prepare: func(mock sqlmock.Sqlmock) {
				expectAttachmentLookup(mock, reference.ID, false).WillReturnRows(runtimeAttachmentRow(reference, "event-a", expiresAt))
				mock.ExpectQuery("SELECT content FROM public.runtime_object").WithArgs("tenant-a", reference.ID).WillReturnRows(sqlmock.NewRows([]string{"content"}).AddRow([]byte("mismatch")))
			},
			want: attachment.ErrInvalid,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if test.prepare != nil {
				test.prepare(mock)
			}
			_, err = runtimepostgres.New(db).Load(context.Background(), test.tenant, test.eventID, test.reference)
			if !errors.Is(err, test.want) {
				t.Fatalf("Load error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresAttachmentCleanupFailureBoundaries(t *testing.T) {
	now := time.Now().UTC()
	for _, test := range []struct {
		name    string
		prepare func(sqlmock.Sqlmock)
		want    error
	}{
		{
			name: "begin failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "delete query failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentCleanup(mock, now).WillReturnError(errors.New("delete failed"))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "scan failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentCleanup(mock, now).WillReturnRows(sqlmock.NewRows([]string{"attachment_id"}).AddRow(nil))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "rows failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentCleanup(mock, now).WillReturnRows(sqlmock.NewRows([]string{"attachment_id"}).AddRow("attachment-1").RowError(0, errors.New("rows failed")))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "object delete failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentCleanup(mock, now).WillReturnRows(sqlmock.NewRows([]string{"attachment_id"}).AddRow("attachment-1"))
				mock.ExpectExec("DELETE FROM public.runtime_object").WithArgs("tenant-a", "attachment-1").WillReturnError(errors.New("object delete failed"))
				mock.ExpectRollback()
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "commit failure",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectAttachmentCleanup(mock, now).WillReturnRows(sqlmock.NewRows([]string{"attachment_id"}).AddRow("attachment-1"))
				mock.ExpectExec("DELETE FROM public.runtime_object").WithArgs("tenant-a", "attachment-1").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
			},
			want: runtimestorage.ErrStorage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			test.prepare(mock)
			_, err = runtimepostgres.New(db).CleanupAttachments(context.Background(), "tenant-a", now)
			if !errors.Is(err, test.want) {
				t.Fatalf("CleanupAttachments error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresAttachmentMethodsFailClosed(t *testing.T) {
	var nilStore *runtimepostgres.Store
	data := []byte("document")
	reference := attachmentReference(data)
	if _, err := nilStore.PutAttachment(context.Background(), "tenant-a", attachment.Upload{ID: reference.ID, Kind: reference.Kind, MIMEType: reference.MIMEType, Size: reference.Size}, strings.NewReader(string(data))); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil PutAttachment = %v", err)
	}
	if _, err := nilStore.Load(context.Background(), "tenant-a", "event-a", reference); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil Load = %v", err)
	}
	if err := nilStore.BindAttachments(context.Background(), "tenant-a", "event-a", []attachment.Reference{reference}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil BindAttachments = %v", err)
	}
	if _, err := nilStore.CleanupAttachments(context.Background(), "tenant-a", time.Now()); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil CleanupAttachments = %v", err)
	}
}

func attachmentReference(data []byte) attachment.Reference {
	digest := sha256.Sum256(data)
	return attachment.Reference{ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
}

func attachmentUpload(reference attachment.Reference) attachment.Upload {
	return attachment.Upload{ID: reference.ID, Kind: reference.Kind, MIMEType: reference.MIMEType, Name: reference.Name, Size: reference.Size, Provider: reference.Provider, ProviderID: reference.ProviderID}
}

func runtimeAttachmentRow(reference attachment.Reference, eventID any, expiresAt time.Time) *sqlmock.Rows {
	return sqlmock.NewRows(runtimeAttachmentColumns).AddRow("tenant-a", reference.ID, string(reference.Kind), reference.MIMEType, reference.Name, reference.Size, reference.SHA256, reference.Provider, reference.ProviderID, eventID, expiresAt)
}

func expectAttachmentLookup(mock sqlmock.Sqlmock, id string, lock bool) *sqlmock.ExpectedQuery {
	statement := "FROM public.runtime_attachment WHERE tenant_id=\\$1 AND attachment_id=\\$2"
	if lock {
		statement += " FOR UPDATE"
	}
	return mock.ExpectQuery(statement).WithArgs("tenant-a", id)
}

func expectObjectInsert(mock sqlmock.Sqlmock, reference attachment.Reference, data []byte) *sqlmock.ExpectedExec {
	return mock.ExpectExec("INSERT INTO public.runtime_object").WithArgs("tenant-a", reference.ID, reference.MIMEType, data, reference.Size, reference.SHA256)
}

func expectObjectPersisted(mock sqlmock.Sqlmock, reference attachment.Reference, data []byte) {
	expectObjectInsert(mock, reference, data).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT content_type,size,etag FROM public.runtime_object").
		WithArgs("tenant-a", reference.ID).
		WillReturnRows(sqlmock.NewRows([]string{"content_type", "size", "etag"}).AddRow(reference.MIMEType, reference.Size, reference.SHA256))
}

func expectMetadataInsert(mock sqlmock.Sqlmock, reference attachment.Reference) *sqlmock.ExpectedExec {
	return mock.ExpectExec("INSERT INTO public.runtime_attachment").
		WithArgs("tenant-a", reference.ID, reference.Kind, reference.MIMEType, reference.Name, reference.Size, reference.SHA256, reference.Provider, reference.ProviderID, sqlmock.AnyArg())
}

func expectMessageEvent(mock sqlmock.Sqlmock, eventID string) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery("SELECT EXISTS").WithArgs("tenant-a", eventID)
}

func expectAttachmentBind(mock sqlmock.Sqlmock, attachmentID, eventID string) *sqlmock.ExpectedExec {
	return mock.ExpectExec("UPDATE public.runtime_attachment SET event_id").WithArgs("tenant-a", attachmentID, eventID)
}

func expectAttachmentCleanup(mock sqlmock.Sqlmock, before time.Time) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery("DELETE FROM public.runtime_attachment").WithArgs("tenant-a", before)
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}
