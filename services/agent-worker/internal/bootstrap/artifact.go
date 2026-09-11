package bootstrap

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

type artifactLedger interface {
	FindRun(context.Context, string, string) (domain.Run, error)
}
type artifactQueries struct {
	pool      *pgxpool.Pool
	ledger    artifactLedger
	manifests application.ManifestReader
}

func (q artifactQueries) Artifact(ctx context.Context, r proof.ArtifactRequest) (proof.ArtifactResponse, error) {
	var out proof.ArtifactResponse
	if r.TenantID == "" || r.RunID == "" || r.ManifestRef == "" || !domain.DigestValid(r.ManifestDigest) || r.DeploymentRevisionID == "" || !trpcagent.ValidArtifactName(r.Name) || (r.Operation != "save" && r.Operation != "load") || (r.Version != nil && (*r.Version < 0 || r.Operation != "load")) || strings.ContainsAny(r.MimeType, "\r\n") {
		return out, httpadapter.ErrArtifactInvalid
	}
	if len(r.Content) > proof.MaxArtifactBytes {
		return out, httpadapter.ErrArtifactCapacity
	}
	run, err := q.ledger.FindRun(ctx, r.TenantID, r.RunID)
	if errors.Is(err, domain.ErrNotFound) {
		return out, httpadapter.ErrAttemptDenied
	}
	if err != nil {
		return out, httpadapter.ErrUnavailable
	}
	route := run.Request.Route
	if route.TenantID != r.TenantID || route.ManifestRef != r.ManifestRef || route.ManifestDigest != r.ManifestDigest || route.DeploymentRevisionID != r.DeploymentRevisionID || run.SessionID == "" {
		return out, httpadapter.ErrAttemptDenied
	}
	p, err := q.manifests.Resolve(ctx, route)
	if err != nil || p.Artifact == nil {
		return out, httpadapter.ErrAttemptDenied
	}
	if r.Operation == "save" && (!trpcagent.ValidArtifactMIME(r.MimeType) || int64(len(r.Content)) > p.Artifact.Backend.Limits.MaxBytes) {
		return out, httpadapter.ErrArtifactCapacity
	}
	scope := trpcagent.ArtifactSessionInfo(r.TenantID, run.SessionID)
	store, err := artifactstore.Open(ctx, q.pool, p.Artifact.Backend, artifactstore.Credentials{AccessKeyID: r.Credentials.AccessKeyID, SecretAccessKey: r.Credentials.SecretAccessKey}, r.TenantID, scope)
	if err != nil {
		return out, httpadapter.ErrUnavailable
	}
	defer store.Close()
	var value *artifact.Artifact
	var version int
	if r.Operation == "save" {
		value = &artifact.Artifact{Data: r.Content, MimeType: r.MimeType, Name: r.Name}
		version, err = store.SaveArtifact(ctx, scope, r.Name, value)
	} else {
		selected := r.Version
		if selected == nil {
			var versions []int
			versions, err = store.ListVersions(ctx, scope, r.Name)
			if err == nil && len(versions) > 0 {
				v := slices.Max(versions)
				selected = &v
			}
		}
		if err == nil && selected != nil {
			version = *selected
			value, err = store.LoadArtifact(ctx, scope, r.Name, selected)
		}
	}
	if errors.Is(err, artifactstore.ErrCapacity) {
		return out, httpadapter.ErrArtifactCapacity
	}
	if err != nil {
		return out, httpadapter.ErrUnavailable
	}
	if value == nil {
		return out, httpadapter.ErrArtifactMissing
	}
	if len(value.Data) > proof.MaxArtifactBytes {
		return out, httpadapter.ErrArtifactCapacity
	}
	meta := trpcagent.ArtifactMetadata(scope, r.Name, version, value)
	out = proof.ArtifactResponse{Name: meta.Name, Version: meta.Version, Ref: meta.Ref, MimeType: meta.MimeType, SizeBytes: meta.SizeBytes, SHA256: meta.SHA256}
	if r.Operation == "load" {
		out.Content = value.Data
	}
	return out, nil
}
