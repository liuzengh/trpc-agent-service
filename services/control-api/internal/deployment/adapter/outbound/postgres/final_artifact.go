package postgresadapter

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

// GetPublishedRevisionByManifest reads one immutable owner record; no latest-pointer lookup.
func (s *Store) GetPublishedRevisionByManifest(ctx context.Context, tenant, manifest string) (domain.PublishedRevision, error) {
	const q = `SELECT ` + publishedRevisionColumns + ` FROM deployment_revisions AS r JOIN runtime_manifests AS m ON m.tenant_id=r.tenant_id AND m.deployment_revision_id=r.id WHERE m.tenant_id=$1 AND m.id=$2`
	out, e := scanPublishedRevision(s.db.QueryRow(ctx, q, tenant, manifest))
	if errors.Is(e, pgx.ErrNoRows) {
		return out, application.ErrDeploymentRevisionNotFound
	}
	return out, e
}
