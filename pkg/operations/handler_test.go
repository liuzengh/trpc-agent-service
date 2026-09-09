package operations

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
)

func principalRequest(t *testing.T, target string, authenticated bool) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	if authenticated {
		p, err := adminauth.NewPrincipal("operator", adminauth.RoleAuditor, []string{"tenant-a"})
		if err != nil {
			t.Fatal(err)
		}
		r = r.WithContext(adminauth.ContextWithPrincipal(r.Context(), p))
	}
	return r
}

func TestHandlerRejectsBeforeDatabase(t *testing.T) {
	for _, tc := range []struct {
		name, target string
		auth         bool
		method       string
		want         int
	}{
		{"unauthorized", "/api/v1/operations/apps?tenantId=tenant-a", false, "GET", 401},
		{"tenant boundary", "/api/v1/operations/apps?tenantId=tenant-b", true, "GET", 403},
		{"tenant missing", "/api/v1/operations/apps", true, "GET", 400},
		{"tenant duplicate", "/api/v1/operations/apps?tenantId=tenant-a&tenantId=tenant-a", true, "GET", 400},
		{"tenant empty", "/api/v1/operations/apps?tenantId=", true, "GET", 400},
		{"bad encoding", "/api/v1/operations/apps?tenantId=tenant-a&status=%zz", true, "GET", 400},
		{"unknown parameter", "/api/v1/operations/apps?tenantId=tenant-a&payload=x", true, "GET", 400},
		{"zero limit", "/api/v1/operations/apps?tenantId=tenant-a&limit=0", true, "GET", 400},
		{"large limit", "/api/v1/operations/apps?tenantId=tenant-a&limit=101", true, "GET", 400},
		{"duplicate limit", "/api/v1/operations/apps?tenantId=tenant-a&limit=1&limit=2", true, "GET", 400},
		{"invalid filter", "/api/v1/operations/apps?tenantId=tenant-a&status=a%27", true, "GET", 400},
		{"unknown view", "/api/v1/operations/nope?tenantId=tenant-a", true, "GET", 404},
		{"wrong prefix", "/apps?tenantId=tenant-a", true, "GET", 404},
		{"readonly", "/api/v1/operations/apps?tenantId=tenant-a", true, "POST", 405},
		{"overview filter", "/api/v1/operations/overview?tenantId=tenant-a&limit=10", true, "GET", 400},
		{"detail filter", "/api/v1/operations/requests/1?tenantId=tenant-a&status=x", true, "GET", 400},
		{"detail canonical", "/api/v1/operations/requests/01?tenantId=tenant-a", true, "GET", 400},
		{"detail overflow", "/api/v1/operations/requests/9223372036854775808?tenantId=tenant-a", true, "GET", 400},
		{"bad cursor", "/api/v1/operations/apps?tenantId=tenant-a&cursor=bad", true, "GET", 400},
		{"missing database", "/api/v1/operations/apps?tenantId=tenant-a", true, "GET", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := principalRequest(t, tc.target, tc.auth)
			r.Method = tc.method
			w := httptest.NewRecorder()
			NewHandler(nil).ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.want, w.Body)
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing no-store")
			}
		})
	}
}

func TestPermissionCannotBeBypassedWithUnvalidatedPrincipal(t *testing.T) {
	r := principalRequest(t, "/api/v1/operations/apps?tenantId=tenant-a", false)
	r = r.WithContext(adminauth.ContextWithPrincipal(context.Background(), adminauth.Principal{ID: "forged", Role: "unknown"}))
	w := httptest.NewRecorder()
	NewHandler(nil).ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestCursorBindsScopeAndNativeID(t *testing.T) {
	stamp := time.Date(2026, 9, 9, 1, 2, 3, 123456000, time.UTC)
	original := query{tenant: "tenant-a", status: "FAILED", limit: 1}
	token := cursorFor("executions", original, stamp, "10000000000000001")
	for _, change := range []string{"none", "tenant", "view", "filter", "version", "numeric", "extra", "trailing"} {
		t.Run(change, func(t *testing.T) {
			q := original
			q.cursor = token
			view := "executions"
			switch change {
			case "tenant":
				q.tenant = "tenant-b"
			case "view":
				view = "inbox"
			case "filter":
				q.status = "RUNNING"
			case "version", "numeric", "extra", "trailing":
				data, _ := base64.RawURLEncoding.DecodeString(token)
				var obj map[string]any
				_ = json.Unmarshal(data, &obj)
				switch change {
				case "version":
					obj["v"] = 2
				case "numeric":
					obj["id"] = "01"
				case "extra":
					obj["unexpected"] = "field"
				}
				data, _ = json.Marshal(obj)
				if change == "trailing" {
					data = append(data, []byte("{}")...)
				}
				q.cursor = base64.RawURLEncoding.EncodeToString(data)
			}
			err := q.parseCursor(view)
			if change == "none" {
				if err != nil || !q.after.Time.Equal(stamp) || q.after.ID != "10000000000000001" {
					t.Fatalf("round trip: %+v %v", q.after, err)
				}
			} else if err == nil {
				t.Fatal("cursor scope/format accepted")
			}
		})
	}
}

func TestListPreservesBigintCursorAndSafeResponse(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stamp := time.Date(2026, 9, 9, 1, 2, 3, 123456000, time.UTC)
	metadata := func(id string) string {
		return `{"id":"` + id + `","tenantId":"tenant-a","status":"FAILED","createdAt":"2000-01-02T03:04:05.123456Z","payload":"SECRET_SENTINEL"}`
	}
	mock.ExpectQuery("SELECT e.id::text,e.started_at,").WithArgs("tenant-a", "FAILED", 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "started_at", "metadata"}).AddRow("10000000000000001", stamp, metadata("10000000000000001")).AddRow("9999999999999999", stamp, metadata("9999999999999999")))
	w := httptest.NewRecorder()
	NewHandler(db).ServeHTTP(w, principalRequest(t, "/api/v1/operations/executions?tenantId=tenant-a&status=FAILED&limit=1", true))
	if w.Code != 200 || strings.Contains(w.Body.String(), "SECRET_SENTINEL") {
		t.Fatalf("response=%d %s", w.Code, w.Body)
	}
	var page Page
	if err = json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "10000000000000001" || page.NextCursor == "" {
		t.Fatalf("page=%+v", page)
	}
	// Predicate explicitly compares BIGINT, never the display ID's text order.
	mock.ExpectQuery(regexp.QuoteMeta("AND (e.started_at,e.id)<($3::timestamptz,$4::bigint)")).
		WithArgs("tenant-a", "FAILED", stamp, "10000000000000001", 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "started_at", "metadata"}).AddRow("9999999999999999", stamp, metadata("9999999999999999")))
	w = httptest.NewRecorder()
	NewHandler(db).ServeHTTP(w, principalRequest(t, "/api/v1/operations/executions?tenantId=tenant-a&status=FAILED&limit=1&cursor="+url.QueryEscape(page.NextCursor), true))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "9999999999999999") {
		t.Fatalf("response=%d %s", w.Code, w.Body)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseFailureNeverReturnsRawError(t *testing.T) {
	for _, rowFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "rows"}[rowFailure], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expected := mock.ExpectQuery("SELECT a.id::text,")
			if rowFailure {
				expected.WillReturnRows(sqlmock.NewRows([]string{"id", "time", "metadata"}).AddRow("a", time.Now(), "{}").RowError(0, errors.New("db://SECRET_SENTINEL")))
			} else {
				expected.WillReturnError(errors.New("db://SECRET_SENTINEL"))
			}
			w := httptest.NewRecorder()
			NewHandler(db).ServeHTTP(w, principalRequest(t, "/api/v1/operations/apps?tenantId=tenant-a", true))
			if w.Code != 503 || strings.Contains(w.Body.String(), "SECRET_SENTINEL") {
				t.Fatalf("response=%d %s", w.Code, w.Body)
			}
			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
