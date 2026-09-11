package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/jackc/pgx/v5/pgxpool"
	events "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/manifestadapter"
	execapp "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	execdomain "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/inbound/wire"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

func fixture(t *testing.T) []byte {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		next := filepath.Dir(dir)
		if next == dir {
			t.Fatal("repository root missing")
		}
		dir = next
	}
	b, err := os.ReadFile(filepath.Join(dir, "api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestManifestProjectionV1Postgres(t *testing.T) {
	urls := []string{os.Getenv("WORKER_TEST_ADMIN_URL"), os.Getenv("WORKER_TEST_MIGRATION_URL"), os.Getenv("WORKER_TEST_RUNTIME_URL")}
	for _, u := range urls {
		if u == "" {
			t.Skip("requires dedicated Worker PostgreSQL")
		}
	}
	if os.Getenv("WORKER_TEST_ALLOW_RESET") != "1" {
		t.Fatal("explicit test reset required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pools := make([]*pgxpool.Pool, 3)
	for i, u := range urls {
		p, e := pgxpool.New(ctx, u)
		if e != nil {
			t.Fatal(e)
		}
		pools[i] = p
		t.Cleanup(p.Close)
	}
	if err := migrations.ApplyForRuntime(ctx, pools[1], "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	if _, err := pools[0].Exec(ctx, `TRUNCATE worker.runtime_manifests CASCADE;TRUNCATE worker.manifest_conflicts;TRUNCATE worker.manifest_conflict_identities`); err != nil {
		t.Fatal(err)
	}
	p := postgresadapter.New(pools[2])
	raw := fixture(t)
	pub, err := wire.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	e, err := events.DecodeRuntimeManifestPublishedEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	c, err := protocol.VerifyRuntimeManifest(e.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	reader := manifestadapter.Reader{Projection: p, ContractDigest: c.PlatformContract.Digest}
	route := execdomain.Route{TenantID: pub.TenantID, ManifestRef: pub.ManifestID, ManifestDigest: pub.ContentDigest, DeploymentRevisionID: pub.DeploymentRevisionID}
	if _, err = reader.Resolve(ctx, route); !errors.Is(err, execapp.ErrManifestMissing) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- p.Apply(ctx, pub, 1) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, err := reader.Resolve(ctx, route)
	if err != nil || plan.ManifestID != pub.ManifestID || plan.ProfileRevision != 2 || plan.MaxOutputTokens != 4096 {
		t.Fatalf("plan %+v %v", plan, err)
	}
	other := route
	other.TenantID = "other-tenant"
	if _, err = reader.Resolve(ctx, other); !errors.Is(err, execapp.ErrManifestMissing) {
		t.Fatal(err)
	}
	other = route
	other.ManifestDigest = execdomain.Digest([]byte("changed"))
	if _, err = reader.Resolve(ctx, other); !errors.Is(err, execapp.ErrManifestInvalid) {
		t.Fatal(err)
	}
	if _, err = (manifestadapter.Reader{Projection: p}).Resolve(ctx, route); !errors.Is(err, execapp.ErrManifestInvalid) {
		t.Fatal("missing release pin accepted", err)
	}
	// Changed content with the same event cannot replace the first projection.
	c.AgentPlan.Nodes[c.AgentPlan.Root] = protocol.ManifestNode{Kind: "llm", Instruction: "different", ModelResource: "primary", ToolResources: []string{}, KnowledgeResources: []string{}, CallableEntries: []string{}}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	b, err = jcs.Transform(b)
	if err != nil {
		t.Fatal(err)
	}
	e.Manifest.Content = b
	e.Manifest.ContentDigest = execdomain.Digest(b)
	b, err = json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := wire.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Apply(ctx, changed, 1); !errors.Is(err, domain.ErrConflict) {
		t.Fatal(err)
	}
	if _, err = reader.Resolve(ctx, route); !errors.Is(err, execapp.ErrManifestInvalid) {
		t.Fatal("conflicted projection remained executable", err)
	}
	var originalDigest string
	if err = pools[2].QueryRow(ctx, `SELECT content_digest FROM runtime_manifests`).Scan(&originalDigest); err != nil || originalDigest != pub.ContentDigest {
		t.Fatal("first content overwritten", err)
	}
	t.Run("crossed manifest and revision poison every identity", func(t *testing.T) {
		x := namedPublication(t, "evt_cross_x", "tenant-cross", "rmf_cross_x", "dpr_cross_one", 1)
		y := namedPublication(t, "evt_cross_y", "tenant-cross", "rmf_cross_y", "dpr_cross_two", 2)
		z := namedPublication(t, "evt_cross_z", "tenant-cross", "rmf_cross_z", "dpr_cross_three", 3)
		for _, m := range []domain.Publication{x, y, z} {
			if err := p.Apply(ctx, m, 16); err != nil {
				t.Fatal(err)
			}
			if _, err := reader.Resolve(ctx, publicationRoute(m)); err != nil {
				t.Fatal(err)
			}
		}
		crossed := namedPublication(t, "evt_cross_bad", "tenant-cross", x.ManifestID, y.DeploymentRevisionID, 2)
		if err := p.Apply(ctx, crossed, 16); !errors.Is(err, domain.ErrConflict) {
			t.Fatalf("crossed identity = %v", err)
		}
		for _, m := range []domain.Publication{x, y} {
			if _, err := reader.Resolve(ctx, publicationRoute(m)); !errors.Is(err, execapp.ErrManifestInvalid) {
				t.Fatalf("implicated %s remains executable: %v", m.ManifestID, err)
			}
			var digest string
			if err := pools[2].QueryRow(ctx, `SELECT content_digest FROM runtime_manifests WHERE tenant_id=$1 AND manifest_id=$2`, m.TenantID, m.ManifestID).Scan(&digest); err != nil || digest != m.ContentDigest {
				t.Fatalf("first content changed: %s %v", digest, err)
			}
		}
		if _, err := reader.Resolve(ctx, publicationRoute(z)); err != nil {
			t.Fatalf("unrelated identity poisoned: %v", err)
		}
		t.Log("MANIFEST_CROSSED_IDENTITIES=PASS X/rev1 and Y/rev2 both non-executable after X/rev2 conflict; original digests preserved; unrelated Z remains executable")
	})
	t.Run("same event poisons absent target before later arrival", func(t *testing.T) {
		original := namedPublication(t, "evt_poison_original", "tenant-poison", "rmf_poison_old", "dpr_poison_old", 1)
		if err := p.Apply(ctx, original, 16); err != nil {
			t.Fatal(err)
		}
		unseen := namedPublication(t, original.EventID, "tenant-poison", "rmf_poison_new", "dpr_poison_new", 2)
		if err := p.Apply(ctx, unseen, 16); !errors.Is(err, domain.ErrConflict) {
			t.Fatalf("same event changed identity = %v", err)
		}
		if _, err := reader.Resolve(ctx, publicationRoute(unseen)); !errors.Is(err, execapp.ErrManifestInvalid) {
			t.Fatalf("absent conflicted identity reported missing: %v", err)
		}
		later := namedPublication(t, "evt_poison_later", unseen.TenantID, unseen.ManifestID, unseen.DeploymentRevisionID, 2)
		if err := p.Apply(ctx, later, 16); !errors.Is(err, domain.ErrConflict) {
			t.Fatalf("later fresh event revived poisoned target: %v", err)
		}
		for _, m := range []domain.Publication{original, unseen} {
			if _, err := reader.Resolve(ctx, publicationRoute(m)); !errors.Is(err, execapp.ErrManifestInvalid) {
				t.Fatalf("poisoned identity remains executable: %v", err)
			}
		}
		var count int
		if err := pools[2].QueryRow(ctx, `SELECT count(*) FROM runtime_manifests WHERE manifest_id=$1`, unseen.ManifestID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("poisoned absent target inserted: %d %v", count, err)
		}
		t.Log("MANIFEST_ABSENT_IDENTITY_TOMBSTONE=PASS conflicting old event poisons unseen target; fresh later event remains rejected without publication insertion")
	})
	t.Log("WORKER_MANIFEST_V1=PASS strict compiled fixture, missing-before-arrival, concurrent replay, fixed identity, release pin, cross-tenant rejection, durable conflict without overwrite")
}

func publicationRoute(m domain.Publication) execdomain.Route {
	return execdomain.Route{TenantID: m.TenantID, ManifestRef: m.ManifestID, ManifestDigest: m.ContentDigest, DeploymentRevisionID: m.DeploymentRevisionID}
}

func namedPublication(t *testing.T, eventID, tenant, manifestID, revisionID string, revision int64) domain.Publication {
	t.Helper()
	e, err := events.DecodeRuntimeManifestPublishedEvent(fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	c, err := protocol.VerifyRuntimeManifest(e.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	c.TenantID = tenant
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	b, err = jcs.Transform(b)
	if err != nil {
		t.Fatal(err)
	}
	e.EventID, e.TenantID, e.DeploymentRevisionID, e.RevisionNumber = eventID, tenant, revisionID, revision
	e.Manifest.ID, e.Manifest.TenantID, e.Manifest.DeploymentRevisionID, e.Manifest.RevisionNumber = manifestID, tenant, revisionID, revision
	e.Manifest.Content, e.Manifest.ContentDigest = b, execdomain.Digest(b)
	b, err = json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	m, err := wire.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
