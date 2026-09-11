package controlhttp

import (
	"context"
	"encoding/json"
	"errors"
	protocol "github.com/liuzengh/trpc-agent-service/api/runtime/control/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type projectionStub struct{ count int }

func (p *projectionStub) Apply(context.Context, domain.Publication, int) error { p.count++; return nil }
func (p *projectionStub) Read(context.Context, string, string) (domain.Publication, error) {
	return domain.Publication{}, domain.ErrMissing
}
func publicationFixture(t *testing.T) []byte {
	t.Helper()
	root, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("root missing")
		}
		root = parent
	}
	raw, err := os.ReadFile(filepath.Join(root, "api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestOwnerExportPagesAndConfirmedEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "two pages", true: "empty"}[empty], func(t *testing.T) {
			raw := publicationFixture(t)
			calls := 0
			upper := &protocol.ExportPosition{CreatedAt: time.Now().UTC(), TenantID: "tenant", EventID: "event"}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != protocol.ManifestExportPath {
					t.Error("wrong export path")
				}
				page := protocol.ManifestExportPage{SchemaVersion: "v1", SnapshotUpper: upper, Events: []json.RawMessage{raw}, Complete: calls == 2}
				if calls == 1 {
					page.NextCursor = "opaque-page-2"
				} else if r.URL.Query().Get("cursor") != "opaque-page-2" {
					t.Error("cursor not passed")
				}
				if empty {
					page = protocol.ManifestExportPage{SchemaVersion: "v1", Events: []json.RawMessage{}, Complete: true}
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(page)
			}))
			defer server.Close()
			e, err := New(Options{BaseURL: server.URL, Client: server.Client(), RequestTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			p := &projectionStub{}
			result, err := e.Synchronize(context.Background(), p, 100)
			if err != nil {
				t.Fatal(err)
			}
			if empty {
				if result.Pages != 1 || result.Events != 0 || result.Upper != nil {
					t.Fatal("empty collection unconfirmed")
				}
			} else if result.Pages != 2 || result.Events != 2 || p.count != 2 {
				t.Fatal("missing export page")
			}
		})
	}
}
func TestOwnerExportRejectsUpperDriftAndCursorLoops(t *testing.T) {
	for _, drift := range []bool{false, true} {
		t.Run(map[bool]string{false: "cursor loop", true: "upper drift"}[drift], func(t *testing.T) {
			raw := publicationFixture(t)
			calls := 0
			upper := protocol.ExportPosition{CreatedAt: time.Now().UTC(), TenantID: "tenant", EventID: "event"}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				u := upper
				if drift && calls > 1 {
					u.EventID = "changed"
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(protocol.ManifestExportPage{SchemaVersion: "v1", SnapshotUpper: &u, Events: []json.RawMessage{raw}, NextCursor: "same", Complete: false})
			}))
			defer server.Close()
			e, _ := New(Options{BaseURL: server.URL, Client: server.Client(), RequestTimeout: time.Second})
			if _, err := e.Synchronize(context.Background(), &projectionStub{}, 100); !errors.Is(err, ErrInvalidExport) {
				t.Fatalf("error %v", err)
			}
			if calls != 2 {
				t.Fatal("unbounded recovery loop")
			}
		})
	}
}
func TestOwnerExportClosedCompleteMarker(t *testing.T) {
	valid := `{"schema_version":"v1","snapshot_upper":null,"events":[],"next_cursor":"","complete":true}`
	if _, err := decodePage([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(valid, `"complete":true`, `"complete":false`, 1), strings.Replace(valid, `"events":[]`, `"events":null`, 1), strings.Replace(valid, `"complete":true`, `"Complete":true`, 1), strings.Replace(valid, `"complete":true`, `"complete":true,"complete":true`, 1), strings.Replace(valid, `"next_cursor":""`, `"next_cursor":"unexpected"`, 1)} {
		if _, err := decodePage([]byte(bad)); err == nil {
			t.Fatal("invalid complete marker accepted")
		}
	}
}
