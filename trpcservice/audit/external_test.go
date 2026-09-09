package audit

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

func TestHTTPArchiveIsScopedAppendOnlyAndRedacted(t *testing.T) {
	var method, authorization, payload string
	client := &http.Client{Transport: auditRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		method, authorization = request.Method, request.Header.Get("Authorization")
		body, _ := io.ReadAll(request.Body)
		payload = string(body)
		return &http.Response{StatusCode: http.StatusCreated, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	store := &HTTPArchive{TenantID: "tenant-a", Endpoint: "https://archive.example", Token: "archive-secret", Client: client, Redactor: servicelog.NewRedactor([]string{"message"}, []string{"archive-secret", "private body"})}
	record := Record{TenantID: "tenant-a", Decision: "allow", TraceID: "trace", RequestID: "request", CreatedAt: time.Unix(1, 0), Details: map[string]any{"message": "private body"}}
	if err := store.Append(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || authorization != "Bearer archive-secret" {
		t.Fatalf("method=%s authorization=%s", method, authorization)
	}
	if strings.Contains(payload, "private body") || strings.Contains(payload, "archive-secret") {
		t.Fatalf("archive payload leaked protected content: %s", payload)
	}
	record.TenantID = "tenant-b"
	if err := store.Append(context.Background(), record); err == nil {
		t.Fatal("cross-tenant append must fail")
	}
}

func TestRoutedStoreFailsClosedAfterPrimaryCommit(t *testing.T) {
	primary := NewMemoryStore(nil)
	store := &RoutedStore{Primary: primary, Resolve: func(context.Context, Record) (Store, error) {
		return failingStore{}, nil
	}}
	record := Record{TenantID: "tenant-a", Decision: "allow", TraceID: "trace"}
	if err := store.Append(context.Background(), record); err == nil {
		t.Fatal("archive failure must be visible")
	}
	if len(primary.Records("tenant-a")) != 1 {
		t.Fatal("primary audit was not committed")
	}
}

type failingStore struct{}

func (failingStore) Append(context.Context, Record) error { return io.ErrUnexpectedEOF }

type auditRoundTripFunc func(*http.Request) (*http.Response, error)

func (function auditRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
