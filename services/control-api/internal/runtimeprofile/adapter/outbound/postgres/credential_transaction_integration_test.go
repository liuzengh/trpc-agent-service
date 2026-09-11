package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

func TestCredentialTransactionAgainstPostgreSQL(t *testing.T) {
	store, _ := credentialPostgres(t)
	ctx := context.Background()
	abort := errors.New("abort callback")
	err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
		draft, err := tx.GetDraft(ctx)
		if err != nil {
			return err
		}
		draft.Revision = 2
		draft.Spec = json.RawMessage(`{"changed":true}`)
		if err := tx.SaveDraft(ctx, draft, 1); err != nil {
			return err
		}
		if err := tx.InsertCredential(ctx, testCredential("credential-rollback")); err != nil {
			return err
		}
		if err := tx.InsertReceipt(ctx, testReceipt("rollback")); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("callback error = %v", err)
	}
	err = store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
		draft, err := tx.GetDraft(ctx)
		if err != nil {
			return err
		}
		if draft.Revision != 1 || string(draft.Spec) != `{}` {
			t.Fatalf("draft changed after rollback: %#v", draft)
		}
		if _, err := tx.GetCredential(ctx, "credential-rollback"); !errors.Is(err, domain.ErrCredentialNotFound) {
			t.Fatalf("credential rollback = %v", err)
		}
		if _, found, err := tx.FindReceipt(ctx, "actor", "rollback"); err != nil || found {
			t.Fatalf("receipt rollback: found=%v err=%v", found, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCredentialTransactionCommitCASAndClearAgainstPostgreSQL(t *testing.T) {
	store, _ := credentialPostgres(t)
	ctx := context.Background()
	run := func(fn func(application.CredentialTransaction) error) error {
		return store.WithinProfile(ctx, "tenant-a", "profile-a", fn)
	}
	if err := run(func(tx application.CredentialTransaction) error {
		draft, err := tx.GetDraft(ctx)
		if err != nil {
			return err
		}
		draft.Revision++
		draft.Spec = json.RawMessage(`{"committed":true}`)
		if err := tx.InsertCredential(ctx, testCredential("credential-a")); err != nil {
			return err
		}
		if err := tx.SaveDraft(ctx, draft, 1); err != nil {
			return err
		}
		return tx.InsertReceipt(ctx, testReceipt("save"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := run(func(tx application.CredentialTransaction) error {
		draft, err := tx.GetDraft(ctx)
		if err != nil {
			return err
		}
		if draft.Revision != 2 {
			t.Fatalf("committed draft revision=%d", draft.Revision)
		}
		if err := tx.SaveDraft(ctx, draft, 1); !errors.Is(err, application.ErrDraftRevisionConflict) {
			t.Fatalf("stale draft CAS=%v", err)
		}
		if _, err := tx.GetRevision(ctx, 99); !errors.Is(err, application.ErrProfileRevisionNotFound) {
			t.Fatalf("missing revision=%v", err)
		}
		revision, err := tx.GetRevision(ctx, 1)
		if err != nil || revision.ID != "revision-a" {
			t.Fatalf("revision=%#v error=%v", revision, err)
		}
		c, err := tx.GetCredential(ctx, "credential-a")
		if err != nil {
			return err
		}
		c.Revision = 2
		c.Ciphertext = []byte("rotated-encrypted-fixture")
		if err := tx.UpdateCredential(ctx, c, 1); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := run(func(tx application.CredentialTransaction) error {
		c, err := tx.GetCredential(ctx, "credential-a")
		if err != nil {
			return err
		}
		if c.Revision != 2 || string(c.Ciphertext) != "rotated-encrypted-fixture" {
			t.Fatalf("rotation not persisted")
		}
		if err := tx.UpdateCredential(ctx, c, 1); !errors.Is(err, domain.ErrCredentialConflict) {
			t.Fatalf("stale credential CAS=%v", err)
		}
		changed := c.Clone()
		changed.Revision = 3
		changed.AudienceDigest = "changed-purpose"
		if err := tx.UpdateCredential(ctx, changed, 2); !errors.Is(err, domain.ErrCredentialAssociation) {
			t.Fatalf("immutable purpose=%v", err)
		}
		c.Status = domain.CredentialCleared
		c.Ciphertext = nil
		c.Revision = 3
		return tx.UpdateCredential(ctx, c, 2)
	}); err != nil {
		t.Fatal(err)
	}
	if err := run(func(tx application.CredentialTransaction) error {
		c, err := tx.GetCredential(ctx, "credential-a")
		if err != nil {
			return err
		}
		if c.Status != domain.CredentialCleared || c.Revision != 3 || len(c.Ciphertext) != 0 {
			t.Fatalf("clear did not remove value")
		}
		c.Status, c.Ciphertext, c.Revision = domain.CredentialActive, []byte("resurrection"), 4
		if err := tx.UpdateCredential(ctx, c, 3); !errors.Is(err, domain.ErrCredentialUnavailable) {
			t.Fatalf("resurrection=%v", err)
		}
		receipt, found, err := tx.FindReceipt(ctx, "actor", "save")
		if err != nil || !found || receipt.RequestMAC != "non-secret-request-mac" {
			t.Fatalf("receipt read=%#v found=%v err=%v", receipt, found, err)
		}
		if err := tx.InsertReceipt(ctx, testReceipt("save")); !errors.Is(err, application.ErrCredentialIdempotencyConflict) {
			t.Fatalf("duplicate receipt=%v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialTransactionScopesAgainstPostgreSQL(t *testing.T) {
	store, _ := credentialPostgres(t)
	ctx := context.Background()
	if err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
		if err := tx.InsertCredential(ctx, testCredential("credential-a")); err != nil {
			return err
		}
		return tx.InsertReceipt(ctx, testReceipt("save"))
	}); err != nil {
		t.Fatal(err)
	}
	for _, scope := range [][2]string{{"tenant-a", "profile-b"}, {"tenant-b", "profile-a"}} {
		if err := store.WithinProfile(ctx, scope[0], scope[1], func(tx application.CredentialTransaction) error {
			if _, err := tx.GetCredential(ctx, "credential-a"); !errors.Is(err, domain.ErrCredentialNotFound) {
				t.Fatalf("cross-scope credential=%v", err)
			}
			if _, err := tx.GetRevision(ctx, 1); !errors.Is(err, application.ErrProfileRevisionNotFound) {
				t.Fatalf("cross-scope revision=%v", err)
			}
			if _, found, err := tx.FindReceipt(ctx, "actor", "save"); err != nil || found {
				t.Fatalf("cross-scope receipt found=%v err=%v", found, err)
			}
			if err := tx.InsertCredential(ctx, testCredential("injected")); !errors.Is(err, domain.ErrCredentialAssociation) {
				t.Fatalf("cross-scope insert=%v", err)
			}
			draft := domain.ProfileDraft{TenantID: "tenant-a", ProfileID: "profile-a", Revision: 2}
			if err := tx.SaveDraft(ctx, draft, 1); !errors.Is(err, domain.ErrCredentialAssociation) {
				t.Fatalf("cross-scope draft=%v", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	called := false
	err := store.WithinProfile(ctx, "missing-tenant", "profile-a", func(application.CredentialTransaction) error { called = true; return nil })
	if !errors.Is(err, application.ErrRuntimeProfileNotFound) || called {
		t.Fatalf("missing owner err=%v called=%v", err, called)
	}
	if err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
		if _, found, err := tx.FindReceipt(ctx, "actor-b", "save"); err != nil || found {
			t.Fatalf("cross-actor receipt found=%v err=%v", found, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialTransactionConcurrentCASAgainstPostgreSQL(t *testing.T) {
	store, _ := credentialPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
		return tx.InsertCredential(ctx, testCredential("concurrent"))
	}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsOut := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errorsOut <- store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
				c, err := tx.GetCredential(ctx, "concurrent")
				if err != nil {
					return err
				}
				c.Revision = 2
				c.Ciphertext = []byte("new-encrypted-value")
				return tx.UpdateCredential(ctx, c, 1)
			})
		}()
	}
	close(start)
	wg.Wait()
	close(errorsOut)
	success, conflicts := 0, 0
	for err := range errorsOut {
		if err == nil {
			success++
		} else if errors.Is(err, domain.ErrCredentialConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("success=%d conflicts=%d", success, conflicts)
	}
}

func TestCredentialDatabaseConstraintsAgainstPostgreSQL(t *testing.T) {
	store, pool := credentialPostgres(t)
	ctx := context.Background()
	if err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
		return tx.InsertCredential(ctx, testCredential("constraints"))
	}); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`UPDATE runtime_profile_credentials SET audience_digest='changed',credential_revision=2 WHERE id='constraints'`,
		`UPDATE runtime_profile_credentials SET purpose='changed',credential_revision=2 WHERE id='constraints'`,
		`UPDATE runtime_profile_credentials SET category='tools',credential_revision=2 WHERE id='constraints'`,
		`UPDATE runtime_profile_credentials SET resource_name='other',credential_revision=2 WHERE id='constraints'`,
		`UPDATE runtime_profile_credentials SET ciphertext=NULL,credential_revision=2 WHERE id='constraints'`,
		`UPDATE runtime_profile_credentials SET status='cleared',credential_revision=2 WHERE id='constraints'`,
		`UPDATE runtime_profile_credentials SET credential_revision=3 WHERE id='constraints'`,
	} {
		if _, err := pool.Exec(ctx, statement); err == nil {
			t.Fatalf("invalid update accepted: %s", statement)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE runtime_profile_credentials SET status='cleared',ciphertext=NULL,credential_revision=2 WHERE id='constraints'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE runtime_profile_credentials SET status='active',ciphertext='encrypted',credential_revision=3 WHERE id='constraints'`); err == nil {
		t.Fatal("database resurrected a cleared credential")
	}
}

func testCredential(id string) domain.ProfileCredential {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	return domain.ProfileCredential{ID: id, TenantID: "tenant-a", ProfileID: "profile-a", Category: "models", ResourceName: "primary", Purpose: "api_key", AudienceDigest: "sha256:" + strings.Repeat("a", 64), Revision: 1, Status: domain.CredentialActive, Ciphertext: []byte("encrypted-fixture"), CreatedBy: "actor", UpdatedBy: "actor", CreatedAt: now, UpdatedAt: now}
}

func testReceipt(key string) application.CredentialReceipt {
	return application.CredentialReceipt{ActorUserID: "actor", Key: key, RequestMAC: "non-secret-request-mac", Result: json.RawMessage(`{"draft_revision":2}`), CreatedAt: time.Now().UTC()}
}

func credentialPostgres(t *testing.T) (*postgresadapter.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("CONTROL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CONTROL_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("profile_credential_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		if err != nil {
			t.Errorf("cleanup schema: %v", err)
		}
		admin.Close()
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	baseline, err := migrations.Files.ReadFile("0001_baseline.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(baseline)); err != nil {
		t.Fatalf("apply isolated baseline: %v", err)
	}
	const setup = `
	INSERT INTO user_accounts(id, username, normalized_username) VALUES ('actor', 'actor', 'actor'), ('actor-b', 'actor-b', 'actor-b');
	INSERT INTO tenants(id, slug, name, created_at, updated_at) VALUES ('tenant-a','tenant-a','Tenant A',now(),now()), ('tenant-b','tenant-b','Tenant B',now(),now());
	INSERT INTO runtime_profiles(tenant_id,id,name,created_by,created_at,updated_at) VALUES ('tenant-a','profile-a','Profile A','actor',now(),now()), ('tenant-a','profile-b','Profile B','actor',now(),now()), ('tenant-b','profile-a','Profile Other Tenant','actor',now(),now());
	INSERT INTO runtime_profile_drafts(tenant_id,profile_id,spec_revision,spec_jsonb,updated_by,updated_at) VALUES ('tenant-a','profile-a',1,'{}','actor',now()), ('tenant-a','profile-b',1,'{}','actor',now()), ('tenant-b','profile-a',1,'{}','actor',now());
	INSERT INTO runtime_profile_revisions(tenant_id,id,profile_id,revision_number,source_draft_revision,schema_version,spec_jsonb,spec_digest,published_by,published_at) VALUES ('tenant-a','revision-a','profile-a',1,1,'v1','{}','sha256:' || repeat('a',64),'actor',now());
	UPDATE runtime_profiles SET latest_revision_number=1 WHERE tenant_id='tenant-a' AND id='profile-a';
	`
	if _, err := pool.Exec(ctx, setup); err != nil {
		t.Fatal(err)
	}
	return postgresadapter.NewStore(pool), pool
}
