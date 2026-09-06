package admin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestTenantPoliciesVersionedAndValidated(t *testing.T) {
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	defer repo.Close()
	writer := audit.NewMemoryWriter()
	s := &Service{repository: repo, audit: writer}
	ctx := context.Background()
	updated, err := s.UpdateTenantPolicies(ctx, TenantPolicyInput{TenantID: "tutorial-tenant", ExpectedVersion: 1, AuditPolicy: json.RawMessage(`{"level":"security","retention_days":30}`)})
	if err != nil || updated.Version != 2 {
		t.Fatalf("update=%+v err=%v", updated, err)
	}
	if _, err = s.UpdateTenantPolicies(ctx, TenantPolicyInput{TenantID: updated.ID, ExpectedVersion: 1}); !errors.Is(err, controlplane.ErrConflict) {
		t.Fatal("stale policy overwrite")
	}
	if _, err = s.UpdateTenantPolicies(ctx, TenantPolicyInput{TenantID: updated.ID, ExpectedVersion: 2, AuditPolicy: json.RawMessage(`{"failure_mode":"ignore"}`)}); err == nil {
		t.Fatal("invalid policy accepted")
	}
	if len(writer.Events()) != 1 {
		t.Fatal("policy request not audited")
	}
}
