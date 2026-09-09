//go:build integration

package integration

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/operations"
)

const operationsSecret = "OPS_SECRET_SENTINEL"

var operationsInboxStates = []string{"RECEIVED", "PROCESSING", "RETRY_WAIT", "WAITING_APPROVAL", "WAITING_RECONCILIATION", "COMPLETED", "DEAD_LETTERED"}
var operationsOutboxStates = []string{"REPLY_PENDING", "DELIVERING", "DISPATCH_STARTED", "RETRY_WAIT", "WAITING_RECONCILIATION", "REPLIED", "DEAD_LETTERED"}
var operationsExecutionStates = []string{"FAILED", "SUCCEEDED", "RUNNING", "ABANDONED"}

type operationsFixture struct {
	tenant string
	ids    map[string][]string
	stamp  time.Time
}

// TestOperationsPostgres exercises the public HTTP boundary against the real
// migrated schema. Fixtures use only two fresh tenants and are removed even
// when an assertion fails. It never resets sequences or truncates shared tables.
func TestOperationsPostgres(t *testing.T) {
	db := openDatabase(t)
	for _, index := range []string{"idx_inbox_operations_tenant_time_id", "idx_execution_operations_tenant_time_id"} {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname=$1)`, index).Scan(&exists); err != nil || !exists {
			t.Fatalf("required operations index %s: exists=%t err=%v", index, exists, err)
		}
	}
	a := seedOperations(t, db)
	b := seedOperations(t, db)
	h := operations.NewHandler(db)
	p, err := adminauth.NewPrincipal("operations-integration", adminauth.RoleAuditor, []string{a.tenant, b.tenant})
	if err != nil {
		t.Fatal(err)
	}
	get := func(t *testing.T, path string, status int, dest any) string {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/operations/"+path, nil)
		r = r.WithContext(adminauth.ContextWithPrincipal(r.Context(), p))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("GET %s: status=%d want=%d body=%s", strings.Split(path, "?")[0], w.Code, status, w.Body)
		}
		body := w.Body.String()
		assertOperationsSafe(t, body)
		if dest != nil {
			if err := json.Unmarshal(w.Body.Bytes(), dest); err != nil {
				t.Fatal(err)
			}
		}
		return body
	}

	t.Run("all views pagination and tenant isolation", func(t *testing.T) {
		for _, fixture := range []operationsFixture{a, b} {
			for _, view := range []string{"apps", "versions", "deployments", "inbox", "executions", "outbox", "audit", "migrations"} {
				want := append([]string(nil), fixture.ids[view]...)
				numeric := view == "inbox" || view == "executions" || view == "outbox" || view == "audit"
				sort.Slice(want, func(i, j int) bool {
					if !numeric {
						return want[i] > want[j]
					}
					left, _ := strconv.ParseInt(want[i], 10, 64)
					right, _ := strconv.ParseInt(want[j], 10, 64)
					return left > right
				})
				var got []string
				cursor := ""
				for n := 0; ; n++ {
					if n > len(want) {
						t.Fatalf("%s pagination did not terminate", view)
					}
					path := view + "?tenantId=" + fixture.tenant + "&limit=1"
					if cursor != "" {
						path += "&cursor=" + url.QueryEscape(cursor)
					}
					var page operations.Page
					get(t, path, 200, &page)
					if page.Items == nil || len(page.Items) > 1 {
						t.Fatalf("%s invalid items array: %+v", view, page)
					}
					for _, item := range page.Items {
						if item.TenantID != fixture.tenant || !item.CreatedAt.Equal(fixture.stamp) {
							t.Fatalf("%s scope/time mismatch: %+v", view, item)
						}
						got = append(got, item.ID)
					}
					cursor = page.NextCursor
					if cursor == "" {
						break
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s tenant=%s IDs=%v want=%v", view, fixture.tenant, got, want)
				}
			}
		}
	})

	t.Run("overview counts states and safe metadata", func(t *testing.T) {
		for _, fixture := range []operationsFixture{a, b} {
			var overview operations.Overview
			get(t, "overview?tenantId="+fixture.tenant, 200, &overview)
			if overview.Counts != (operations.Counts{Apps: 3, Versions: 3, Deployments: 4, Inbox: 7, Outbox: 7, Executions: 4}) {
				t.Fatalf("counts=%+v", overview.Counts)
			}
			for _, check := range []struct {
				got  []operations.StateCount
				want []string
			}{{overview.InboxStates, operationsInboxStates}, {overview.OutboxStates, operationsOutboxStates}, {overview.ExecutionStates, operationsExecutionStates}} {
				seen := map[string]int64{}
				for _, state := range check.got {
					seen[state.Status] = state.Count
				}
				if len(seen) != len(check.want) {
					t.Fatalf("state groups=%v", seen)
				}
				for _, state := range check.want {
					if seen[state] != 1 {
						t.Fatalf("state %s count=%d", state, seen[state])
					}
				}
			}
		}
	})

	t.Run("status filters", func(t *testing.T) {
		for view, states := range map[string][]string{
			"apps": {"active", "suspended", "deleted"}, "versions": {"draft", "published", "retired"},
			"deployments": {"active", "paused", "rolled_back", "completed"},
			"inbox":       operationsInboxStates, "outbox": operationsOutboxStates, "executions": operationsExecutionStates,
			"audit": {"request.inspect"}, "migrations": {"PREPARE"},
		} {
			for _, state := range states {
				var page operations.Page
				get(t, view+"?tenantId="+a.tenant+"&status="+state, 200, &page)
				if len(page.Items) == 0 {
					t.Fatalf("%s %s missing", view, state)
				}
				for _, item := range page.Items {
					if item.Status != state || item.TenantID != a.tenant {
						t.Fatalf("%s %s filter mismatch: %+v", view, state, item)
					}
				}
			}
		}
		// One active migration per domain is a schema invariant. Transition the
		// same isolated row to verify every supported phase without bypassing it.
		for _, phase := range []string{"SNAPSHOT_COPY", "DUAL_WRITE", "CATCH_UP", "VALIDATE", "READ_SHADOW", "CUTOVER", "ROLLBACK_WINDOW", "COMPLETE", "ROLLED_BACK"} {
			operationsExec(t, db, `UPDATE data_migrations SET phase=$1 WHERE tenant_id=$2 AND id=$3`, phase, a.tenant, a.ids["migrations"][0])
			var page operations.Page
			get(t, "migrations?tenantId="+a.tenant+"&status="+phase, 200, &page)
			if len(page.Items) != 1 || page.Items[0].Phase != phase {
				t.Fatalf("migration phase %s: %+v", phase, page)
			}
		}
		operationsExec(t, db, `UPDATE data_migrations SET phase='PREPARE' WHERE tenant_id=$1`, a.tenant)
		for _, view := range []string{"apps", "versions", "deployments", "inbox", "executions", "outbox", "audit", "migrations"} {
			var page operations.Page
			body := get(t, view+"?tenantId="+a.tenant+"&status=DOES_NOT_EXIST", 200, &page)
			if page.Items == nil || len(page.Items) != 0 || page.NextCursor != "" || !strings.Contains(body, `"items":[]`) {
				t.Fatalf("%s empty page=%s", view, body)
			}
		}
	})

	t.Run("cursor binding and authorization", func(t *testing.T) {
		var first operations.Page
		get(t, "inbox?tenantId="+a.tenant+"&limit=1", 200, &first)
		if first.NextCursor == "" {
			t.Fatal("missing next cursor")
		}
		cursor := url.QueryEscape(first.NextCursor)
		get(t, "inbox?tenantId="+b.tenant+"&cursor="+cursor, 400, nil)
		get(t, "outbox?tenantId="+a.tenant+"&cursor="+cursor, 400, nil)
		get(t, "inbox?tenantId="+a.tenant+"&status=RECEIVED&cursor="+cursor, 400, nil)
		get(t, "inbox?tenantId="+a.tenant+"&tenantId="+a.tenant, 400, nil)
		get(t, "inbox?tenantId="+a.tenant+"&limit=101", 400, nil)
		get(t, "unknown?tenantId="+a.tenant, 404, nil)
		get(t, "requests/"+b.ids["inbox"][0]+"?tenantId="+a.tenant, 404, nil)
		limited, err := adminauth.NewPrincipal("tenant-a-only", adminauth.RoleAuditor, []string{a.tenant})
		if err != nil {
			t.Fatal(err)
		}
		original := p
		p = limited
		get(t, "overview?tenantId="+b.tenant, 403, nil)
		get(t, "requests/"+b.ids["inbox"][0]+"?tenantId="+b.tenant, 403, nil)
		p = original
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/operations/overview?tenantId="+a.tenant, nil))
		if w.Code != 401 {
			t.Fatalf("anonymous status=%d", w.Code)
		}
	})

	t.Run("request associations and explicit truncation", func(t *testing.T) {
		// A cross-tenant audit can contain the same resource ID. The detail must
		// still scope both the audit row and every association subquery.
		operationsExec(t, db, `UPDATE control_plane_audit SET resource_type='inbox',resource_id=$1 WHERE tenant_id=$2 AND id=$3`, a.ids["inbox"][0], b.tenant, b.ids["audit"][4])
		var detail operations.RequestDetail
		get(t, "requests/"+a.ids["inbox"][0]+"?tenantId="+a.tenant, 200, &detail)
		if detail.Inbox.ID != a.ids["inbox"][0] || len(detail.Executions) != 2 || len(detail.Outbox) != 1 || len(detail.Audit) != 4 || detail.Truncated {
			t.Fatalf("detail associations=%+v", detail)
		}
		wantExecutions := map[string]bool{a.ids["executions"][0]: true, a.ids["executions"][1]: true}
		for _, item := range detail.Executions {
			if !wantExecutions[item.ID] || item.TenantID != a.tenant {
				t.Fatalf("unrelated execution %+v", item)
			}
		}
		if detail.Outbox[0].ID != a.ids["outbox"][0] || detail.Outbox[0].InboxID != detail.Inbox.ID {
			t.Fatalf("unrelated outbox %+v", detail.Outbox)
		}
		for _, item := range detail.Audit {
			if item.TenantID != a.tenant || item.Actor != "operations-auditor" || item.ID == a.ids["audit"][4] || item.ID == a.ids["audit"][5] {
				t.Fatalf("unrelated audit %+v", item)
			}
		}
		if detail.Inbox.TraceID != "0123456789abcdef0123456789abcdef" || detail.Inbox.LeaseVersion != "10000000000000001" {
			t.Fatalf("safe trace/fence metadata missing: %+v", detail.Inbox)
		}
		var untraced operations.RequestDetail
		get(t, "requests/"+a.ids["inbox"][6]+"?tenantId="+a.tenant, 200, &untraced)
		if untraced.Inbox.TraceID != "" || untraced.Executions == nil || len(untraced.Executions) != 0 || untraced.Audit == nil || len(untraced.Audit) != 0 {
			t.Fatalf("empty associations/invalid trace: %+v", untraced)
		}
		operationsExec(t, db, `INSERT INTO control_plane_audit(tenant_id,actor,action,resource_type,resource_id,details)
		SELECT $1,'operations-auditor','request.inspect','inbox',$3,jsonb_build_object('rawError',$2::text) FROM generate_series(1,101)`, a.tenant, operationsSecret, a.ids["inbox"][0])
		get(t, "requests/"+a.ids["inbox"][0]+"?tenantId="+a.tenant, 200, &detail)
		if !detail.Truncated || len(detail.Audit) != 100 {
			t.Fatalf("detail truncation=%t audit=%d", detail.Truncated, len(detail.Audit))
		}
	})
}

func seedOperations(t *testing.T, db *sql.DB) operationsFixture {
	t.Helper()
	f := operationsFixture{tenant: "ops-" + uuid.NewString(), ids: map[string][]string{}, stamp: time.Date(2026, 9, 9, 1, 2, 3, 456789000, time.UTC)}
	// Random high IDs straddle 10^16 without touching database sequences. All
	// values are above JavaScript's exact-integer range; each table receives
	// both 16- and 17-digit values at an identical PostgreSQL timestamp.
	random := uuid.New()
	delta := int64(binary.BigEndian.Uint64(random[:8])%1_000_000_000+1) * 100
	for _, view := range []string{"inbox", "outbox", "executions", "audit"} {
		for i := 0; i < 7; i++ {
			n := int64(10_000_000_000_000_000) + delta + int64(i)
			if i%2 == 1 {
				n = 10_000_000_000_000_000 - delta - int64(i)
			}
			f.ids[view] = append(f.ids[view], strconv.FormatInt(n, 10))
		}
	}
	f.ids["executions"] = f.ids["executions"][:4]
	f.ids["audit"] = f.ids["audit"][:6]
	operationsExec(t, db, `INSERT INTO tenants(id,name,config) VALUES($1,'operations integration',jsonb_build_object('credential',$2::text))`, f.tenant, operationsSecret)
	t.Cleanup(func() {
		// Every DELETE has the random test tenant predicate; never truncate or
		// clear a shared table. Cascading metadata uses the tenant foreign key.
		for _, statement := range []string{
			`DELETE FROM control_plane_audit WHERE tenant_id=$1`,
			`DELETE FROM outbox_messages WHERE tenant_id=$1`,
			`DELETE FROM execution_records WHERE tenant_id=$1`,
			`DELETE FROM deployments WHERE tenant_id=$1`,
			`DELETE FROM agent_versions WHERE agent_app_id IN (SELECT id FROM agent_apps WHERE tenant_id=$1)`,
			`DELETE FROM agent_apps WHERE tenant_id=$1`,
			`DELETE FROM data_migrations WHERE tenant_id=$1`,
			`DELETE FROM inbox_messages WHERE tenant_id=$1`,
			`DELETE FROM tenants WHERE id=$1`,
		} {
			if _, err := db.Exec(statement, f.tenant); err != nil {
				t.Errorf("operations fixture cleanup failed: %v", err)
			}
		}
	})
	for i, status := range []string{"active", "suspended", "deleted"} {
		id := "app-" + uuid.NewString()
		f.ids["apps"] = append(f.ids["apps"], id)
		operationsExec(t, db, `INSERT INTO agent_apps(id,tenant_id,name,description,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$6)`, id, f.tenant, fmt.Sprintf("operations-%d", i), operationsSecret, status, f.stamp)
	}
	for i, status := range []string{"draft", "published", "retired"} {
		id := "version-" + uuid.NewString()
		f.ids["versions"] = append(f.ids["versions"], id)
		operationsExec(t, db, `INSERT INTO agent_versions(id,agent_app_id,version_number,config_snapshot,config_hash,status,created_by,created_at,published_at)
		VALUES($1,$2,$3,jsonb_build_object('apiKey',$4::text),$5,$6,$4,$7,$7)`, id, f.ids["apps"][0], i+1, operationsSecret, fmt.Sprintf("%064d", i+1), status, f.stamp)
	}
	for _, status := range []string{"active", "paused", "rolled_back", "completed"} {
		id := "deployment-" + uuid.NewString()
		f.ids["deployments"] = append(f.ids["deployments"], id)
		operationsExec(t, db, `INSERT INTO deployments(id,tenant_id,agent_app_id,agent_version_id,kind,traffic_bps,status,created_by,created_at,updated_at)
		VALUES($1,$2,$3,$4,'stable',10000,$5,$6,$7,$7)`, id, f.tenant, f.ids["apps"][0], f.ids["versions"][1], status, operationsSecret, f.stamp)
	}
	for i, status := range operationsInboxStates {
		trace := "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
		if i == 6 {
			trace = operationsSecret
		}
		operationsExec(t, db, `INSERT INTO inbox_messages(id,tenant_id,channel_type,channel_account_id,agent_app_name,external_message_id,conversation_id,user_id,session_id,session_sequence,payload_hash,payload,status,trace_parent,last_error,lease_owner,lease_version,approval_deadline,next_attempt_at,created_at,updated_at)
		VALUES($1::bigint,$2,'telegram',$3::text,'operations-0',$1::bigint::text,$3::text,$3::text,'identical-session',$4,$5,$6,$7,$8,$3::text,$3::text,10000000000000001,$9,$9,$9,$9)`,
			f.ids["inbox"][i], f.tenant, operationsSecret, i+1, strings.Repeat("a", 64), []byte(operationsSecret), status, trace, f.stamp)
		operationsExec(t, db, `INSERT INTO outbox_messages(id,inbox_id,tenant_id,channel_type,channel_account_id,conversation_id,content,trace_parent,status,last_error,lease_owner,lease_version,created_at,updated_at)
		VALUES($1,$2,$3,'telegram',$4::text,$4::text,$4::text,$5,$6,$4::text,$4::text,10000000000000001,$7,$7)`, f.ids["outbox"][i], f.ids["inbox"][i], f.tenant, operationsSecret, trace, operationsOutboxStates[i], f.stamp)
	}
	for i, status := range operationsExecutionStates {
		request := f.ids["inbox"][0]
		attempt := i + 1
		if i > 1 {
			request, attempt = f.ids["inbox"][i], 1
		}
		operationsExec(t, db, `INSERT INTO execution_records(id,tenant_id,session_id,agent_app_id,agent_version_id,deployment_id,status,error_message,started_at,completed_at,idempotency_key,payload_hash,attempt_number,execution_token,retry_safe)
		VALUES($1,$2,'identical-session',$3,$4,$5,$6,$7,$8,$8,$9,$10,$11,$12,$13)`, f.ids["executions"][i], f.tenant, f.ids["apps"][0], f.ids["versions"][1], f.ids["deployments"][0], status, operationsSecret, f.stamp, "inbox:"+request, strings.Repeat("a", 64), attempt, operationsSecret+"-"+uuid.NewString(), status == "FAILED")
	}
	resources := [][2]string{{"inbox", f.ids["inbox"][0]}, {"execution", f.ids["executions"][0]}, {"execution", f.ids["executions"][1]}, {"outbox", f.ids["outbox"][0]}, {"inbox", f.ids["inbox"][1]}, {"app", f.ids["apps"][0]}}
	for i, resource := range resources {
		operationsExec(t, db, `INSERT INTO control_plane_audit(id,tenant_id,actor,action,resource_type,resource_id,details,created_at)
		VALUES($1,$2,'operations-auditor','request.inspect',$4,$5,jsonb_build_object('rawError',$3::text,'secret',$3::text),$6)`, f.ids["audit"][i], f.tenant, operationsSecret, resource[0], resource[1], f.stamp)
	}
	id := "migration-" + uuid.NewString()
	f.ids["migrations"] = []string{id}
	operationsExec(t, db, `INSERT INTO data_migrations(id,tenant_id,domain,source_profile,target_profile,phase,paused,cursor,last_error,lease_owner,created_at,updated_at)
	VALUES($1,$2,'session',$3::text,$4,'PREPARE',TRUE,$3::text,$3::text,$3::text,$5,$5)`, id, f.tenant, operationsSecret, operationsSecret+"-target", f.stamp)
	return f
}

func operationsExec(t *testing.T, db *sql.DB, statement string, args ...any) {
	t.Helper()
	if _, err := db.Exec(statement, args...); err != nil {
		t.Fatalf("fixture %s: %v", strings.Join(strings.Fields(statement)[:3], " "), err)
	}
}

func assertOperationsSafe(t *testing.T, body string) {
	t.Helper()
	if strings.Contains(body, operationsSecret) {
		t.Fatal("sensitive fixture value leaked in operations response")
	}
	var decoded any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{"payload": true, "content": true, "config": true, "configSnapshot": true, "configHash": true, "executionToken": true, "lastError": true, "errorMessage": true, "details": true, "sourceProfile": true, "targetProfile": true, "leaseOwner": true, "traceParent": true}
	var walk func(any)
	walk = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			for key, item := range value {
				if forbidden[key] {
					t.Errorf("forbidden operations key %q", key)
				}
				if key == "id" || strings.HasSuffix(key, "Id") || key == "leaseVersion" {
					if _, ok := item.(string); !ok {
						t.Errorf("identifier %s must be JSON string, got %T", key, item)
					}
				}
				walk(item)
			}
		case []any:
			for _, item := range value {
				walk(item)
			}
		}
	}
	walk(decoded)
}
