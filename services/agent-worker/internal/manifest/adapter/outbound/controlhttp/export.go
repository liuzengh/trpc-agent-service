// Package controlhttp reads immutable Manifest exports from their authenticated
// owner. Bootstrap must bind the retained increment durable before Synchronize.
package controlhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	protocol "github.com/liuzengh/trpc-agent-service/api/runtime/control/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/inbound/wire"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
)

var ErrUnavailable = errors.New("Manifest owner export unavailable")
var ErrInvalidExport = errors.New("Manifest owner export protocol invalid")

type Options struct {
	BaseURL        string
	Client         *http.Client
	RequestTimeout time.Duration
}
type Exporter struct {
	client   *http.Client
	endpoint string
	timeout  time.Duration
}
type Result struct {
	Pages, Events int
	Upper         *protocol.ExportPosition
}

func New(o Options) (*Exporter, error) {
	u, err := url.Parse(o.BaseURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || o.Client == nil || o.RequestTimeout <= 0 {
		return nil, ErrInvalidExport
	}
	u.Path = protocol.ManifestExportPath
	client := *o.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Exporter{&client, u.String(), o.RequestTimeout}, nil
}
func (e *Exporter) Synchronize(ctx context.Context, projection application.Projection, capacity int) (Result, error) {
	var result Result
	if projection == nil || capacity < 1 {
		return result, ErrInvalidExport
	}
	cursor := ""
	seen := map[string]bool{}
	for {
		if ctx.Err() != nil {
			return result, ErrUnavailable
		}
		page, err := e.page(ctx, cursor)
		if err != nil {
			return result, err
		}
		if result.Pages == 0 {
			result.Upper = page.SnapshotUpper
		} else if !samePosition(result.Upper, page.SnapshotUpper) {
			return result, ErrInvalidExport
		}
		result.Pages++
		for _, raw := range page.Events {
			publication, err := wire.Decode(raw)
			if err != nil {
				return result, ErrInvalidExport
			}
			if err = projection.Apply(ctx, publication, capacity); err != nil && !errors.Is(err, domain.ErrConflict) {
				return result, err
			}
			result.Events++
		}
		if page.Complete {
			return result, nil
		}
		if page.NextCursor == cursor || seen[page.NextCursor] {
			return result, ErrInvalidExport
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
	}
}
func (e *Exporter) page(ctx context.Context, cursor string) (protocol.ManifestExportPage, error) {
	var out protocol.ManifestExportPage
	target, _ := url.Parse(e.endpoint)
	query := target.Query()
	query.Set("limit", "4")
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	target.RawQuery = query.Encode()
	requestCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, target.String(), nil)
	if err != nil {
		return out, ErrUnavailable
	}
	response, err := e.client.Do(request)
	if err != nil {
		return out, ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return out, ErrUnavailable
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return out, ErrInvalidExport
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, protocol.MaxExportPageBytes+1))
	if err != nil {
		return out, ErrUnavailable
	}
	return decodePage(raw)
}
func samePosition(a, b *protocol.ExportPosition) bool {
	return a == nil && b == nil || a != nil && b != nil && a.CreatedAt.Equal(b.CreatedAt) && a.TenantID == b.TenantID && a.EventID == b.EventID
}
func exactKeys(raw []byte, keys ...string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := fields[key]; !ok {
			return false
		}
	}
	return true
}
func decodePage(raw []byte) (protocol.ManifestExportPage, error) {
	var page protocol.ManifestExportPage
	if len(raw) == 0 || len(raw) > protocol.MaxExportPageBytes || !utf8.Valid(raw) {
		return page, ErrInvalidExport
	}
	canonical, err := jcs.Transform(raw)
	if err != nil || !exactKeys(canonical, "schema_version", "snapshot_upper", "events", "next_cursor", "complete") {
		return page, ErrInvalidExport
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(canonical, &fields)
	for _, key := range []string{"schema_version", "events", "next_cursor", "complete"} {
		if bytes.Equal(fields[key], []byte("null")) {
			return page, ErrInvalidExport
		}
	}
	if !bytes.Equal(fields["snapshot_upper"], []byte("null")) && !exactKeys(fields["snapshot_upper"], "created_at", "tenant_id", "event_id") {
		return page, ErrInvalidExport
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&page) != nil {
		return page, ErrInvalidExport
	}
	if page.SchemaVersion != "v1" || page.Events == nil || len(page.Events) > protocol.MaxExportPageSize || len(page.NextCursor) > 8192 {
		return page, ErrInvalidExport
	}
	if page.SnapshotUpper == nil {
		if !page.Complete || len(page.Events) != 0 || page.NextCursor != "" {
			return page, ErrInvalidExport
		}
	} else if page.SnapshotUpper.CreatedAt.IsZero() || page.SnapshotUpper.TenantID == "" || page.SnapshotUpper.EventID == "" {
		return page, ErrInvalidExport
	}
	if page.Complete && page.NextCursor != "" || !page.Complete && (page.NextCursor == "" || len(page.Events) == 0) {
		return page, ErrInvalidExport
	}
	return page, nil
}
