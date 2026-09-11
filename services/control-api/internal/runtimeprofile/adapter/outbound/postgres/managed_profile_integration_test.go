package postgresadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	backend "github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/credentialcrypto"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
	"time"
)

type catalogCheck struct{ catalog *backend.Catalog }

func (c catalogCheck) CheckBackend(_ context.Context, tenant, id string, revision uint64, role string) error {
	_, err := c.catalog.Resolve(tenant, backend.Selection{BackendID: id, Revision: revision, Role: backend.Role(role)})
	return err
}
func TestManagedProfileSavePublishAgainstPostgreSQL(t *testing.T) {
	store, pool := credentialPostgres(t)
	ctx := context.Background()
	cipher, err := credentialcrypto.New(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := backend.NewCatalog([]backend.Entry{
		{ID: "pg", Revision: 1, Label: "SQL", Kind: backend.PostgreSQL, Roles: []backend.Role{backend.Session, backend.Memory}, Enabled: true, TenantIDs: []string{"tenant-a"}},
		{ID: "redis", Revision: 1, Label: "Redis", Kind: backend.Redis, Roles: []backend.Role{backend.Session}, Enabled: true, TenantIDs: []string{"tenant-a"}},
		{ID: "s3", Revision: 1, Label: "Objects", Kind: backend.S3, Roles: []backend.Role{backend.Artifact}, Enabled: true, TenantIDs: []string{"tenant-a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service := application.NewService(application.Dependencies{Store: store, Credentials: store, Cipher: cipher, TenantAccess: postgresCredentialAccess{}, OwnerAccess: postgresCredentialAccess{}, Backends: catalogCheck{catalog}, NewProfileID: func() (string, error) { return "unused", nil }, NewRevisionID: func() (string, error) { return "managed-revision", nil }, NewCredentialID: func() (string, error) {
		t.Error("platform storage must not create credentials")
		return "", errors.New("unexpected credential")
	}, Now: time.Now})
	input, err := application.DecodeProfileWrite([]byte(`{"expected_draft_revision":1,"credential_protocol_version":"v1","config":{"models":{},"tools":{},"knowledge":{},"storage":{"session":{"kind":"managed_session","backend_id":"redis","backend_revision":1},"memory":{"kind":"managed_memory","backend_id":"pg","backend_revision":1},"artifact":{"kind":"managed_artifact","backend_id":"s3","backend_revision":1}}},"credentials":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	command := application.SaveCredentialDraftCommand{TenantID: "tenant-a", ProfileID: "profile-a", ActorUserID: "actor", IdempotencyKey: "managed-save", Write: input}
	saved, err := service.SaveCredentialDraft(ctx, command)
	if err != nil || saved.DraftRevision != 2 {
		t.Fatal(saved, err)
	}
	published, err := service.PublishProfileRevision(ctx, application.PublishProfileRevisionCommand{TenantID: "tenant-a", ProfileID: "profile-a", ActorUserID: "actor", ExpectedRevision: 2})
	if err != nil || !published.Created {
		t.Fatal(published, err)
	}
	stored, err := store.GetProfileRevision(ctx, "tenant-a", "profile-a", published.Revision.RevisionNumber)
	if err != nil {
		t.Fatal(err)
	}
	var spec domain.Spec
	if err := json.Unmarshal(stored.Spec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Storage["session"].BackendID != "redis" || spec.Storage["artifact"].BackendID != "s3" {
		t.Fatal(spec.Storage)
	}
	read, err := service.GetCredentialDraft(ctx, "tenant-a", "profile-a", "actor")
	if err != nil {
		t.Fatal(err)
	}
	public, err := json.Marshal(read)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(public, []byte("dsn")) || bytes.Contains(public, []byte("destination")) {
		t.Fatal("platform credential fields in public read", string(public))
	}
	// A later draft choice must not mutate the previously published snapshot.
	command.IdempotencyKey = "managed-next"
	command.Write.ExpectedDraftRevision = 2
	r := command.Write.Config.Storage["session"]
	r.BackendID = "pg"
	command.Write.Config.Storage["session"] = r
	if _, err := service.SaveCredentialDraft(ctx, command); err != nil {
		t.Fatal(err)
	}
	again, err := store.GetProfileRevision(ctx, "tenant-a", "profile-a", stored.RevisionNumber)
	if err != nil || !bytes.Equal(stored.Spec, again.Spec) || stored.SpecDigest != again.SpecDigest {
		t.Fatal("revision mutated", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM runtime_profile_credentials").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("platform credential persisted", count)
	}
	t.Log("MANAGED_PROFILE_PG=PASS save/read/publish, no platform credentials, immutable revision after backend change")
}
