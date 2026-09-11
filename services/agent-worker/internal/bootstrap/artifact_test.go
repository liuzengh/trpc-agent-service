package bootstrap

import (
	"context"
	"errors"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"testing"
)

type artifactLedgerFixture struct {
	run domain.Run
	err error
}

func (f artifactLedgerFixture) FindRun(context.Context, string, string) (domain.Run, error) {
	return f.run, f.err
}
func TestArtifactHTTPRunManifestMismatchDeniedBeforeStorage(t *testing.T) {
	route := domain.Route{TenantID: "tenant", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("fixed")), DeploymentRevisionID: "revision"}
	r := proof.ArtifactRequest{TenantID: route.TenantID, RunID: "run", ManifestRef: route.ManifestRef, ManifestDigest: route.ManifestDigest, DeploymentRevisionID: route.DeploymentRevisionID, Operation: "load", Name: "x"}
	for _, field := range []string{"tenant", "manifest", "digest", "revision", "missing"} {
		t.Run(field, func(t *testing.T) {
			copy := r
			q := artifactQueries{ledger: artifactLedgerFixture{run: domain.Run{Request: domain.Requested{Route: route}, SessionID: "session"}}}
			switch field {
			case "tenant":
				copy.TenantID = "other"
			case "manifest":
				copy.ManifestRef = "other"
			case "digest":
				copy.ManifestDigest = domain.Digest([]byte("other"))
			case "revision":
				copy.DeploymentRevisionID = "other"
			case "missing":
				q.ledger = artifactLedgerFixture{err: domain.ErrNotFound}
			}
			_, err := q.Artifact(context.Background(), copy)
			if !errors.Is(err, httpadapter.ErrAttemptDenied) {
				t.Fatalf("got %v", err)
			}
		})
	}
}
