package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

var ErrEventConflict = errors.New("audit identity was reused with different metadata")

type Policy struct {
	Level         string `json:"level"`
	RetentionDays int    `json:"retention_days"`
	FailureMode   string `json:"failure_mode"`
}

func ParsePolicy(raw json.RawMessage) (Policy, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	p := Policy{Level: "basic", FailureMode: "fail_closed"}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return p, errors.New("invalid tenant audit policy")
	}
	if (p.Level != "basic" && p.Level != "full" && p.Level != "security") || p.RetentionDays < 0 || p.RetentionDays > 36500 || (p.FailureMode != "fail_closed" && p.FailureMode != "buffered") {
		return p, errors.New("unsupported audit policy")
	}
	return p, nil
}
func securityEvent(event Event) bool {
	for _, prefix := range []string{"tool_", "approval_", "admin_", "audit_", "channel_"} {
		if strings.HasPrefix(event.Decision, prefix) {
			return true
		}
	}
	for _, word := range []string{"fail", "deny", "reject", "unknown", "dead", "cancel"} {
		if strings.Contains(event.Decision, word) {
			return true
		}
	}
	return event.ErrorType != ""
}

type policyCache struct {
	policy Policy
	until  time.Time
}
type PolicyWriter struct {
	base       Writer
	repository controlplane.Repository
	spool      *spool
	mu         sync.Mutex
	policies   map[string]policyCache
	cursor     string
	operations *metrics.Operation
}

func NewPolicyWriter(base Writer, repository controlplane.Repository, directory string, maxRecords int) (*PolicyWriter, error) {
	if base == nil || repository == nil {
		return nil, errors.New("audit policy dependencies required")
	}
	w := &PolicyWriter{base: base, repository: repository, policies: map[string]policyCache{}, operations: metrics.NewOperation("audit")}
	if directory != "" {
		var err error
		w.spool, err = newSpool(directory, maxRecords)
		if err != nil {
			return nil, err
		}
	}
	return w, nil
}
func (w *PolicyWriter) policy(ctx context.Context, tenantID string) (Policy, error) {
	tenant, err := w.repository.GetTenant(ctx, tenantID)
	if err == nil {
		if tenant.ID != tenantID {
			return Policy{}, errors.New("audit tenant scope mismatch")
		}
		p, e := ParsePolicy(tenant.AuditPolicy)
		if e != nil {
			w.mu.Lock()
			delete(w.policies, tenantID)
			w.mu.Unlock()
			return p, e
		}
		w.mu.Lock()
		w.policies[tenantID] = policyCache{p, time.Now().Add(time.Minute)}
		w.mu.Unlock()
		return p, nil
	}
	if errors.Is(err, controlplane.ErrNotFound) {
		w.mu.Lock()
		delete(w.policies, tenantID)
		w.mu.Unlock()
	}
	if !errors.Is(err, controlplane.ErrNotFound) {
		w.mu.Lock()
		cached, ok := w.policies[tenantID]
		w.mu.Unlock()
		if ok && time.Now().Before(cached.until) {
			return cached.policy, nil
		}
	}
	return Policy{}, errors.New("audit policy unavailable")
}
func (w *PolicyWriter) Record(ctx context.Context, event Event) (resultErr error) {
	started := time.Now()
	operation := "record"
	defer func() { w.operations.Finish(ctx, started, event.TenantID, "", operation, "audit", resultErr) }()
	p, err := w.policy(ctx, event.TenantID)
	if err != nil {
		return err
	}
	critical := securityEvent(event)
	if p.Level == "security" && !critical {
		operation = "record.filtered"
		return nil
	}
	event = sanitizeEvent(event)
	if p.Level == "basic" && !critical {
		event.Details = map[string]any{}
	}
	err = w.base.Record(ctx, event)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrEventConflict) {
		return err
	}
	if p.FailureMode != "buffered" || w.spool == nil {
		return errors.New("audit persistence failed; operation refused")
	}
	operation = "record.buffered"
	return w.spool.append(event)
}
func (w *PolicyWriter) Ready(ctx context.Context) error { return w.base.Ready(ctx) }
func (w *PolicyWriter) Close() error                    { return w.base.Close() }
func (w *PolicyWriter) Query(ctx context.Context, q Query) ([]Event, error) {
	r, ok := w.base.(Reader)
	if !ok {
		return nil, errors.New("audit querying unavailable")
	}
	return r.Query(ctx, q)
}

// Each node flushes its own durable buffer. Retention is enabled only on Jobs
// nodes and uses bounded tenant pages; zero days always means keep forever.
func (w *PolicyWriter) Maintain(ctx context.Context, retention bool) (resultErr error) {
	started := time.Now()
	defer func() { w.operations.Finish(ctx, started, "", "", "maintenance", "audit", resultErr) }()
	if w.spool != nil {
		if err := w.spool.flush(ctx, w.base, 100); err != nil {
			return err
		}
	}
	if !retention {
		return nil
	}
	lister, ok := w.repository.(interface {
		ListTenants(context.Context, string, int) ([]controlplane.Tenant, error)
	})
	if !ok {
		return errors.New("tenant enumeration unavailable for audit retention")
	}
	pruner, ok := w.base.(interface {
		Prune(context.Context, string, time.Time, int) (int, error)
	})
	if !ok {
		return errors.New("audit retention unavailable")
	}
	w.mu.Lock()
	after := w.cursor
	w.mu.Unlock()
	tenants, err := lister.ListTenants(ctx, after, 100)
	if err != nil {
		return errors.New("audit retention tenants unavailable")
	}
	for _, tenant := range tenants {
		p, err := ParsePolicy(tenant.AuditPolicy)
		if err != nil {
			return err
		}
		if p.RetentionDays > 0 {
			if _, err := pruner.Prune(ctx, tenant.ID, time.Now().AddDate(0, 0, -p.RetentionDays), 1000); err != nil {
				return err
			}
		}
		after = tenant.ID
	}
	if len(tenants) < 100 {
		after = ""
	}
	w.mu.Lock()
	w.cursor = after
	w.mu.Unlock()
	return nil
}
func (w *PolicyWriter) RunMaintenance(ctx context.Context, retention bool) error {
	timer := time.NewTicker(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
			work, cancel := context.WithTimeout(ctx, 10*time.Second)
			_ = w.Maintain(work, retention)
			cancel()
		}
	}
}
