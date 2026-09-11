package deploymentadapter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	deploymentapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	deploymentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

type ownerStub struct {
	value                     deploymentdomain.PublishedRevision
	err                       error
	tenant, deployment, actor string
	number                    int64
}

func (o *ownerStub) GetDeploymentRevision(_ context.Context, tenant, deployment, actor string, n int64) (deploymentdomain.PublishedRevision, error) {
	o.tenant, o.deployment, o.actor, o.number = tenant, deployment, actor, n
	return o.value, o.err
}
func publication() deploymentdomain.PublishedRevision {
	digest := "sha256:" + strings.Repeat("a", 64)
	return deploymentdomain.PublishedRevision{Revision: deploymentdomain.DeploymentRevision{ID: "dpr_a", TenantID: "tnt_a", DeploymentID: "dpl_a", RevisionNumber: 3}, Manifest: deploymentdomain.RuntimeManifest{ID: "rmf_a", TenantID: "tnt_a", DeploymentID: "dpl_a", DeploymentRevisionID: "dpr_a", RevisionNumber: 3, ContentDigest: digest}, ManifestID: "rmf_a", ManifestDigest: digest, ManifestView: []byte(`{"not":"the runtime source"}`)}
}
func TestExactOwnerReadPreservesTrustedIdentity(t *testing.T) {
	o := &ownerStub{value: publication()}
	r := New(o)
	target, err := r.ReadExact(context.Background(), "tnt_a", "usr_a", domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 3})
	if err != nil {
		t.Fatal(err)
	}
	if o.tenant != "tnt_a" || o.actor != "usr_a" || o.deployment != "dpl_a" || o.number != 3 || target.ManifestID != "rmf_a" || target.DeploymentRevisionID != "dpr_a" {
		t.Fatal("owner query arguments or fixed target changed")
	}
	for _, mutate := range []func(*deploymentdomain.PublishedRevision){func(p *deploymentdomain.PublishedRevision) { p.Manifest.TenantID = "tnt_b" }, func(p *deploymentdomain.PublishedRevision) { p.Manifest.DeploymentRevisionID = "dpr_b" }, func(p *deploymentdomain.PublishedRevision) { p.ManifestDigest = "sha256:" + strings.Repeat("b", 64) }, func(p *deploymentdomain.PublishedRevision) { p.Manifest.RevisionNumber = 4 }} {
		p := publication()
		mutate(&p)
		o.value = p
		_, err := r.ReadExact(context.Background(), "tnt_a", "usr_a", domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 3})
		var e *domain.Error
		if !errors.As(err, &e) || e.Code != domain.TargetIntegrity {
			t.Fatal("mismatched publication accepted", err)
		}
	}
}
func TestOwnerErrorsAreTranslatedWithoutPersistenceAccess(t *testing.T) {
	for _, tc := range []struct{ input, want error }{{deploymentapp.ErrDeploymentNotFound, application.ErrTargetNotFound}, {deploymentapp.ErrTenantForbidden, application.ErrPermissionDenied}, {errors.New("database details"), application.ErrDependencyUnavailable}} {
		_, err := New(&ownerStub{err: tc.input}).ReadExact(context.Background(), "tnt_a", "usr_a", domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 3})
		if !errors.Is(err, tc.want) {
			t.Fatal(err)
		}
	}
}
