package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// HTTPArchive appends redacted audit envelopes to a fixed external WORM
// endpoint. It never reads, updates, or deletes archive records.
type HTTPArchive struct {
	TenantID, Endpoint, Token string
	Client                    *http.Client
	Redactor                  *servicelog.Redactor
}

func (store *HTTPArchive) Append(ctx context.Context, record Record) error {
	if store == nil || ctx == nil || store.TenantID == "" || record.TenantID != store.TenantID || store.Token == "" {
		return errors.New("audit: external archive scope is invalid")
	}
	endpoint, err := url.Parse(store.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return errors.New("audit: external archive endpoint is invalid")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/v1/audit/events"
	redactor := store.Redactor
	if redactor == nil {
		redactor = servicelog.NewRedactor(nil, nil)
	}
	record.ErrorType = redactor.RedactString(record.ErrorType)
	record.Details = redactor.RedactMap(record.Details)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return errors.New("audit: external archive payload is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return errors.New("audit: external archive request failed")
	}
	request.Header.Set("Authorization", "Bearer "+store.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", auditID(record))
	client := store.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := clientCopy.Do(request)
	if err != nil {
		return errors.New("audit: external archive request failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("audit: external archive rejected append")
	}
	return nil
}

type ArchiveResolver func(context.Context, Record) (Store, error)

// RoutedStore commits the primary audit first, then synchronously mirrors to
// the tenant/app archive selected by the current published configuration.
type RoutedStore struct {
	Primary Store
	Resolve ArchiveResolver
}

func (store *RoutedStore) Append(ctx context.Context, record Record) error {
	if store == nil || store.Primary == nil {
		return errors.New("audit: primary store is required")
	}
	if err := store.Primary.Append(ctx, record); err != nil {
		return err
	}
	if store.Resolve == nil {
		return nil
	}
	archive, err := store.Resolve(ctx, record)
	if err != nil || archive == nil {
		return err
	}
	return archive.Append(ctx, record)
}

var _ Store = (*HTTPArchive)(nil)
var _ Store = (*RoutedStore)(nil)
