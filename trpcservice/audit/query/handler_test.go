package query

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeStore struct {
	records    []QueryRecord
	failRecord bool
}

func (f *fakeStore) Query(_ context.Context, _ Filter) (Page, error) { return Page{}, nil }
func (f *fakeStore) RecordAccess(_ context.Context, r QueryRecord) error {
	if f.failRecord {
		return ErrInvalidFilter
	}
	f.records = append(f.records, r)
	return nil
}

type fakePrincipals struct {
	p   Principal
	err error
}

func (f fakePrincipals) Resolve(*http.Request) (Principal, error) { return f.p, f.err }

func TestHandlerDeniesCrossTenantWithoutGrant(t *testing.T) {
	store := &fakeStore{}
	handler := Handler{Store: store, Principals: fakePrincipals{p: Principal{Authenticated: true, TenantID: "t1",
		SubjectID: "op", CanReadAudit: true}}, MaxWindow: 31 * 24 * time.Hour, MaxPage: 200}
	req := httptest.NewRequest(http.MethodGet, "/v1/audit/events/?from=2026-09-01T00:00:00Z&to=2026-09-02T00:00:00Z", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want 403", rec.Code)
	}
	if len(store.records) != 1 || store.records[0].Decision != "denied" {
		t.Fatalf("records=%+v", store.records)
	}
}

func TestHandlerAllowsSingleTenant(t *testing.T) {
	store := &fakeStore{}
	handler := Handler{Store: store, Principals: fakePrincipals{p: Principal{Authenticated: true, TenantID: "t1",
		SubjectID: "op", CanReadAudit: true}}, MaxWindow: 31 * 24 * time.Hour, MaxPage: 200}
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t1/audit/events?from=2026-09-01T00:00:00Z&to=2026-09-02T00:00:00Z", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(store.records) != 1 || store.records[0].Decision != "allowed" || store.records[0].TenantID != "t1" {
		t.Fatalf("records=%+v", store.records)
	}
}

func TestHandlerDeniedRecordCarriesRequiredFields(t *testing.T) {
	store := &fakeStore{}
	handler := Handler{Store: store, Principals: fakePrincipals{p: Principal{Authenticated: true, TenantID: "t1",
		SubjectID: "op", CanReadAudit: true}}, MaxWindow: 31 * 24 * time.Hour, MaxPage: 200}
	req := httptest.NewRequest(http.MethodGet, "/v1/audit/events/?from=2026-09-01T00:00:00Z&to=2026-09-02T00:00:00Z", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want 403", rec.Code)
	}
	if len(store.records) != 1 {
		t.Fatalf("records=%+v", store.records)
	}
	r := store.records[0]
	if r.TenantID == "" || r.Subject == "" || r.From.IsZero() || r.To.IsZero() ||
		len(r.FilterDigest) != 64 || len(r.ResultDigest) != 64 || r.Decision != "denied" {
		t.Fatalf("denied record missing required fields: %+v", r)
	}
}

func TestHandlerFailClosedWhenAuditRecordFails(t *testing.T) {
	store := &fakeStore{failRecord: true}
	handler := Handler{Store: store, Principals: fakePrincipals{p: Principal{Authenticated: true, TenantID: "t1",
		SubjectID: "op", CanReadAudit: true}}, MaxWindow: 31 * 24 * time.Hour, MaxPage: 200}
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t1/audit/events?from=2026-09-01T00:00:00Z&to=2026-09-02T00:00:00Z", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500 when audit record fails", rec.Code)
	}
}

func TestHandlerAllowsCrossTenantWithGrant(t *testing.T) {
	store := &fakeStore{}
	handler := Handler{Store: store, Principals: fakePrincipals{p: Principal{Authenticated: true, TenantID: "t1",
		SubjectID: "op", CanReadAudit: true, CanReadAuditCrossTenant: true}}, MaxWindow: 31 * 24 * time.Hour, MaxPage: 200}
	req := httptest.NewRequest(http.MethodGet, "/v1/audit/events?from=2026-09-01T00:00:00Z&to=2026-09-02T00:00:00Z", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(store.records) != 1 || store.records[0].Decision != "allowed" || !store.records[0].CrossTenant {
		t.Fatalf("records=%+v", store.records)
	}
}

func TestHandlerRecordsUnauthenticatedAttempt(t *testing.T) {
	store := &fakeStore{}
	handler := Handler{Store: store, Principals: fakePrincipals{err: errors.New("bad token")},
		MaxWindow: 31 * 24 * time.Hour, MaxPage: 200}
	req := httptest.NewRequest(http.MethodGet, "/v1/audit/events/?from=2026-09-01T00:00:00Z&to=2026-09-02T00:00:00Z", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", rec.Code)
	}
	if len(store.records) != 1 || store.records[0].ReasonCode != "unauthenticated" ||
		store.records[0].Subject != "_anonymous" || store.records[0].TenantID != "_unknown" ||
		!store.records[0].CrossTenant {
		t.Fatalf("records=%+v", store.records)
	}
}
