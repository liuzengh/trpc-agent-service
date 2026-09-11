// Package deploymentadapter reads exact publications through their owning
// application, never through Deployment persistence or a redacted manifest view.
package deploymentadapter

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	deploymentapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	deploymentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

type OwnerReader interface {
	GetDeploymentRevision(context.Context, string, string, string, int64) (deploymentdomain.PublishedRevision, error)
}
type Reader struct{ owner OwnerReader }

func New(owner OwnerReader) *Reader { return &Reader{owner} }
func (r *Reader) ReadExact(ctx context.Context, tenant, actor string, selector domain.TargetSelector) (domain.PublishedTarget, error) {
	if r == nil || r.owner == nil {
		return domain.PublishedTarget{}, application.ErrDependencyUnavailable
	}
	published, err := r.owner.GetDeploymentRevision(ctx, tenant, selector.DeploymentID, actor, selector.RevisionNumber)
	switch {
	case errors.Is(err, deploymentapp.ErrDeploymentNotFound), errors.Is(err, deploymentapp.ErrDeploymentRevisionNotFound):
		return domain.PublishedTarget{}, application.ErrTargetNotFound
	case errors.Is(err, deploymentapp.ErrTenantForbidden):
		return domain.PublishedTarget{}, application.ErrPermissionDenied
	case errors.Is(err, deploymentapp.ErrPublicationIntegrity):
		return domain.PublishedTarget{}, &domain.Error{Code: domain.TargetIntegrity}
	case err != nil:
		return domain.PublishedTarget{}, application.ErrDependencyUnavailable
	}
	revision, manifest := published.Revision, published.Manifest
	target := domain.PublishedTarget{TenantID: revision.TenantID, DeploymentID: revision.DeploymentID, RevisionNumber: revision.RevisionNumber, DeploymentRevisionID: revision.ID, ManifestID: manifest.ID, ManifestDigest: manifest.ContentDigest}
	if err := target.Validate(tenant); err != nil {
		return domain.PublishedTarget{}, err
	}
	if target.Selector() != selector || published.ManifestID != manifest.ID || published.ManifestDigest != manifest.ContentDigest || manifest.TenantID != tenant || manifest.DeploymentID != revision.DeploymentID || manifest.DeploymentRevisionID != revision.ID || manifest.RevisionNumber != revision.RevisionNumber {
		return domain.PublishedTarget{}, &domain.Error{Code: domain.TargetIntegrity}
	}
	return target, nil
}

var _ application.PublishedTargetReader = (*Reader)(nil)
