package bootstrap

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/finalartifacthttp"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

type replyArtifactLedger interface {
	AcceptedArtifact(context.Context, string, string, string, string, int) (domain.AcceptedArtifact, error)
}

// The completion-scoped resolver must verify durable accepted evidence. An old
// execution token/lease is deliberately absent; active-attempt resolution is not
// authority to download a Final after that Attempt has ended.
type replyArtifactCredentials interface {
	ResolveReplyArtifact(context.Context, proof.ReplyArtifactRequest, domain.AcceptedArtifact, domain.Plan) (artifactstore.Credentials, error)
}
type replyArtifactStore interface {
	LoadArtifact(context.Context, artifact.SessionInfo, string, *int) (*artifact.Artifact, error)
	Close()
}
type replyArtifactOpen func(context.Context, *pgxpool.Pool, datav1.Snapshot, artifactstore.Credentials, string, artifact.SessionInfo) (replyArtifactStore, error)
type replyArtifactQueries struct {
	pool        *pgxpool.Pool
	ledger      replyArtifactLedger
	manifests   application.ManifestReader
	credentials replyArtifactCredentials
	open        replyArtifactOpen
}

func (q replyArtifactQueries) ReplyArtifact(ctx context.Context, r proof.ReplyArtifactRequest) (proof.ReplyArtifactResponse, error) {
	var zero proof.ReplyArtifactResponse
	if r.Validate() != nil {
		return zero, httpadapter.ErrArtifactInvalid
	}
	if q.ledger == nil || q.manifests == nil || q.credentials == nil {
		return zero, httpadapter.ErrUnavailable
	}
	accepted, err := q.ledger.AcceptedArtifact(ctx, r.IntentID, r.RunID, r.CompletionID, r.Name, r.Version)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrNotReady):
			return zero, httpadapter.ErrArtifactMissing
		case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrInvalid):
			return zero, httpadapter.ErrAttemptDenied
		default:
			return zero, httpadapter.ErrUnavailable
		}
	}
	run, a := accepted.Run, accepted.Attachment
	route := run.Request.Route
	if run.Status != domain.Succeeded || run.Request.RunID != r.RunID || run.SessionID == "" || route.TenantID == "" || route.ManifestRef == "" || !domain.DigestValid(route.ManifestDigest) || route.DeploymentRevisionID == "" || a.Validate() != nil || a.Name != r.Name || a.Version != r.Version {
		return zero, httpadapter.ErrAttemptDenied
	}
	p, err := q.manifests.Resolve(ctx, route)
	if err != nil {
		return zero, httpadapter.ErrUnavailable
	}
	if p.TenantID != route.TenantID || p.ManifestID != route.ManifestRef || p.ManifestDigest != route.ManifestDigest || p.DeploymentRevisionID != route.DeploymentRevisionID || p.Artifact == nil {
		return zero, httpadapter.ErrAttemptDenied
	}
	b := p.Artifact.Backend
	digest, err := b.Digest()
	if err != nil || b.ValidateForRole("artifact") != nil || b.TenantID != route.TenantID {
		return zero, httpadapter.ErrAttemptDenied
	}
	for purpose, use := range map[string]domain.CredentialUse{"access_key_id": p.Artifact.AccessKeyID, "secret_access_key": p.Artifact.SecretAccessKey} {
		if use.CredentialID == "" || use.Purpose != purpose || use.AudienceDigest != digest {
			return zero, httpadapter.ErrAttemptDenied
		}
	}
	if a.SizeBytes > proof.MaxArtifactBytes || int64(a.SizeBytes) > b.Limits.MaxBytes {
		return zero, httpadapter.ErrArtifactCapacity
	}
	secrets, err := q.credentials.ResolveReplyArtifact(ctx, r, accepted, p)
	if err != nil {
		if errors.Is(err, finalartifacthttp.ErrDenied) {
			return zero, httpadapter.ErrAttemptDenied
		}
		return zero, httpadapter.ErrUnavailable
	}
	scope := trpcagent.ArtifactSessionInfo(route.TenantID, run.SessionID)
	open := q.open
	if open == nil {
		open = func(ctx context.Context, pool *pgxpool.Pool, b datav1.Snapshot, c artifactstore.Credentials, tenant string, scope artifact.SessionInfo) (replyArtifactStore, error) {
			return artifactstore.Open(ctx, pool, b, c, tenant, scope)
		}
	}
	store, err := open(ctx, q.pool, b, secrets, route.TenantID, scope)
	secrets = artifactstore.Credentials{}
	if err != nil {
		return zero, httpadapter.ErrUnavailable
	}
	if store == nil {
		return zero, httpadapter.ErrUnavailable
	}
	defer store.Close()
	selected := r.Version
	value, err := store.LoadArtifact(ctx, scope, r.Name, &selected)
	if err != nil {
		if value != nil {
			clear(value.Data)
		}
		return zero, httpadapter.ErrUnavailable
	}
	if value == nil {
		return zero, httpadapter.ErrArtifactMissing
	}
	meta := trpcagent.ArtifactMetadata(scope, r.Name, r.Version, value)
	if ctx.Err() != nil || meta.MimeType != a.MimeType || meta.SizeBytes != a.SizeBytes || meta.SHA256 != a.SHA256 {
		clear(value.Data)
		return zero, httpadapter.ErrUnavailable
	}
	return proof.ReplyArtifactResponse{Content: value.Data, MimeType: a.MimeType, SizeBytes: a.SizeBytes, SHA256: a.SHA256}, nil
}
