package application_test

import (
	"context"
	backend "github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

type catalogueBackendAccess struct{ c *backend.Catalog }

func (a catalogueBackendAccess) CheckBackend(_ context.Context, tenant, id string, rev uint64, role string) error {
	_, err := a.c.Resolve(tenant, backend.Selection{BackendID: id, Revision: rev, Role: backend.Role(role)})
	return err
}
func TestManagedMemoryProfilesUsePGOrRedisCatalogue(t *testing.T) {
	for _, kind := range []backend.Kind{backend.PostgreSQL, backend.Redis} {
		t.Run(string(kind), func(t *testing.T) {
			directory, err := backend.NewCatalog([]backend.Entry{{ID: "shared", Revision: 1, Label: "Shared", Kind: kind, Roles: []backend.Role{backend.Session, backend.Memory}, Enabled: true, TenantIDs: []string{"tnt_a"}}})
			if err != nil {
				t.Fatal(err)
			}
			h := newCredentialHarnessWithBackend(t, catalogueBackendAccess{directory})
			c := modelCredentialCommand(1, "memory-role", "model-secret")
			c.Write.Config.Storage = map[string]application.StorageConfig{"session": {Kind: domain.StorageKindManagedSession, BackendID: "shared", BackendRevision: 1}, "memory": {Kind: domain.StorageKindManagedMemory, BackendID: "shared", BackendRevision: 1}}
			h.save(t, c)
			spec := h.spec(t)
			if spec.Storage["memory"].BackendID != "shared" || spec.Storage["memory"].DSNCredentialID != "" {
				t.Fatal("Memory selection/credential boundary")
			}
			p, err := h.service.PublishProfileRevision(context.Background(), application.PublishProfileRevisionCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ExpectedRevision: 2})
			if err != nil || !p.Created {
				t.Fatal(p, err)
			}
		})
	}
}
