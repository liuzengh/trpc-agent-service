package audit

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func policyRepository(raw string) *controlplane.MemoryRepository {
	data := controlplane.DefaultBootstrapData()
	data.Tenants[0].AuditPolicy = json.RawMessage(raw)
	return controlplane.NewMemoryRepository(data)
}

func privateSpool(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "spool")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestPolicyLevelsCannotSuppressSecurityEvents(t *testing.T) {
	for _, level := range []string{"basic", "full", "security"} {
		t.Run(level, func(t *testing.T) {
			base := NewMemoryWriter()
			writer, err := NewPolicyWriter(base, policyRepository(`{"level":"`+level+`"}`), "", 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, decision := range []string{"run_completed", "tool_denied"} {
				if err := writer.Record(context.Background(), Event{TenantID: "tutorial-tenant", Decision: decision, Details: map[string]any{"safe": "value", "nested": []any{map[string]any{"api_key": "private-canary"}}}}); err != nil {
					t.Fatal(err)
				}
			}
			rows := base.Events()
			want := 2
			if level == "security" {
				want = 1
			}
			if len(rows) != want {
				t.Fatal("policy filter incorrect")
			}
			for _, row := range rows {
				encoded, _ := json.Marshal(row)
				if strings.Contains(string(encoded), "private-canary") {
					t.Fatal("policy exported secret")
				}
				if row.Decision == "run_completed" && level == "basic" && len(row.Details) != 0 {
					t.Fatal("basic policy kept optional details")
				}
			}
		})
	}
	for _, raw := range []string{`{"level":"off"}`, `{"failure_mode":"ignore"}`, `{"retention_days":-1}`, `{"unexpected":true}`, `{} {}`} {
		if _, err := ParsePolicy(json.RawMessage(raw)); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
}

type failingAudit struct {
	*MemoryWriter
	fail, commitThenFail bool
}

func (w *failingAudit) Record(ctx context.Context, event Event) error {
	if w.fail {
		if w.commitThenFail {
			if err := w.MemoryWriter.Record(ctx, event); err != nil {
				return err
			}
		}
		return errors.New("synthetic audit failure")
	}
	return w.MemoryWriter.Record(ctx, event)
}

func TestDurableAuditBufferSurvivesRestartAndLostResponse(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-commit", true: "after-commit"}[committed], func(t *testing.T) {
			dir := privateSpool(t)
			base := &failingAudit{MemoryWriter: NewMemoryWriter(), fail: true, commitThenFail: committed}
			repo := policyRepository(`{"level":"full","failure_mode":"buffered"}`)
			first, err := NewPolicyWriter(base, repo, dir, 10)
			if err != nil {
				t.Fatal(err)
			}
			event := Event{TenantID: "tutorial-tenant", Decision: "tool_allowed", Details: map[string]any{"api_key": "private-canary"}}
			if err := first.Record(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			files, _ := os.ReadDir(dir)
			if len(files) != 1 {
				t.Fatal("audit was not durably buffered")
			}
			info, _ := files[0].Info()
			if info.Mode().Perm() != 0600 {
				t.Fatal("buffer is not private")
			}
			data, _ := os.ReadFile(filepath.Join(dir, files[0].Name()))
			if strings.Contains(string(data), "private-canary") {
				t.Fatal("secret in spool")
			}
			base.fail = false
			restarted, err := NewPolicyWriter(base, repo, dir, 10)
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.Maintain(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			files, _ = os.ReadDir(dir)
			if len(files) != 0 || len(base.Events()) != 1 {
				t.Fatal("replayed audit lost or duplicated")
			}
		})
	}
}

func TestAuditFailureModesAndCapacityFailClosed(t *testing.T) {
	base := &failingAudit{MemoryWriter: NewMemoryWriter(), fail: true}
	closed, _ := NewPolicyWriter(base, policyRepository(`{}`), "", 0)
	e := Event{TenantID: "tutorial-tenant", Decision: "tool_allowed"}
	if closed.Record(context.Background(), e) == nil {
		t.Fatal("fail_closed swallowed audit failure")
	}
	buffered, err := NewPolicyWriter(base, policyRepository(`{"failure_mode":"buffered"}`), privateSpool(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := buffered.Record(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if buffered.Record(context.Background(), e) == nil {
		t.Fatal("full buffer silently dropped audit")
	}
}

func TestRetentionIsOptInAndTenantScoped(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Tenants[0].AuditPolicy = json.RawMessage(`{"retention_days":1}`)
	other := data.Tenants[0]
	other.ID = "keep-tenant"
	other.AuditPolicy = json.RawMessage(`{}`)
	data.Tenants = append(data.Tenants, other)
	base := NewMemoryWriter()
	ctx := context.Background()
	for _, tenant := range []string{"tutorial-tenant", "keep-tenant"} {
		if err := base.Record(ctx, Event{TenantID: tenant, Decision: "run_completed", OccurredAt: time.Now().Add(-48 * time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	w, _ := NewPolicyWriter(base, controlplane.NewMemoryRepository(data), "", 0)
	if err := w.Maintain(ctx, true); err != nil {
		t.Fatal(err)
	}
	rows := base.Events()
	if len(rows) != 1 || rows[0].TenantID != "keep-tenant" {
		t.Fatal("retention crossed tenant or default-preserve boundary")
	}
}

func TestAuditSpoolRejectsSymlinks(t *testing.T) {
	dir := privateSpool(t)
	target := filepath.Join(t.TempDir(), "private")
	if err := os.WriteFile(target, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "audit-malicious.json")); err != nil {
		t.Fatal(err)
	}
	s, _ := newSpool(dir, 10)
	if s.flush(context.Background(), NewMemoryWriter(), 10) == nil {
		t.Fatal("followed symlink")
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "private" {
		t.Fatal("modified symlink target")
	}
}

type policyOutage struct {
	controlplane.Repository
	outage bool
}

func (r *policyOutage) GetTenant(ctx context.Context, id string) (controlplane.Tenant, error) {
	if r.outage {
		return controlplane.Tenant{}, errors.New("synthetic control plane outage")
	}
	return r.Repository.GetTenant(ctx, id)
}
func TestPolicyCacheIsBoundedAndDoesNotInventTenantPolicy(t *testing.T) {
	repo := &policyOutage{Repository: policyRepository(`{"failure_mode":"buffered"}`)}
	base := NewMemoryWriter()
	w, err := NewPolicyWriter(base, repo, privateSpool(t), 10)
	if err != nil {
		t.Fatal(err)
	}
	e := Event{TenantID: "tutorial-tenant", Decision: "tool_allowed"}
	if err := w.Record(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	repo.outage = true
	if err := w.Record(context.Background(), e); err != nil {
		t.Fatal("last valid policy should bridge short metadata outage")
	}
	if err := w.Record(context.Background(), Event{TenantID: "unknown", Decision: "tool_allowed"}); err == nil {
		t.Fatal("invented a policy for unknown tenant")
	}
	cached := w.policies[e.TenantID]
	cached.until = time.Now().Add(-time.Second)
	w.policies[e.TenantID] = cached
	if err := w.Record(context.Background(), e); err == nil {
		t.Fatal("stale policy continued indefinitely")
	}
}
