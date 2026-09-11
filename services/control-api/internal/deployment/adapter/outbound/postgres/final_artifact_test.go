package postgresadapter_test

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"strings"
	"testing"
)

func TestFinalArtifactManifestReadIsTenantAndIdentityScoped(t *testing.T) {
	db := &summaryDBStub{row: summaryRowStub{err: pgx.ErrNoRows}}
	_, err := postgresadapter.NewStore(db).GetPublishedRevisionByManifest(context.Background(), "tenant", "manifest")
	if !errors.Is(err, application.ErrDeploymentRevisionNotFound) {
		t.Fatal(err)
	}
	for _, part := range []string{"m.tenant_id=$1", "m.id=$2", "m.tenant_id=r.tenant_id", "m.deployment_revision_id=r.id", "content_jsonb"} {
		if !strings.Contains(db.query, part) {
			t.Fatal("missing exact binding", part)
		}
	}
	if len(db.args) != 2 || db.args[0] != "tenant" || db.args[1] != "manifest" {
		t.Fatal(db.args)
	}
}
