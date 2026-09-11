package bootstrap

import (
	"context"
	"errors"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type proofLedger interface {
	ActiveToken(context.Context, string, string, string, string) (domain.Grant, error)
	Final(context.Context, string) (domain.Final, error)
}
type proofQueries struct {
	ledger    proofLedger
	manifests application.ManifestReader
}

func (q proofQueries) VerifyAttempt(ctx context.Context, r proof.AttemptRequest) (proof.AttemptResponse, error) {
	var response proof.AttemptResponse
	grant, err := q.ledger.ActiveToken(ctx, r.WorkloadIdentity, r.ExecutionToken, r.ManifestID, r.ManifestDigest)
	if errors.Is(err, domain.ErrFenced) || errors.Is(err, domain.ErrNotFound) {
		return response, httpadapter.ErrAttemptDenied
	}
	if err != nil {
		return response, httpadapter.ErrUnavailable
	}
	plan, err := q.manifests.Resolve(ctx, grant.Run.Request.Route)
	if errors.Is(err, application.ErrManifestInvalid) || errors.Is(err, application.ErrManifestUnsupported) {
		return response, httpadapter.ErrAttemptDenied
	}
	if errors.Is(err, application.ErrManifestContractMismatch) {
		// Release skew is temporary: the Attempt may be retried once this Worker
		// runs the release that compiled the Manifest.
		return response, httpadapter.ErrUnavailable
	}
	if err != nil {
		return response, httpadapter.ErrUnavailable
	}
	if plan.TenantID != grant.Run.Request.Route.TenantID || plan.ManifestID != r.ManifestID || plan.ManifestDigest != r.ManifestDigest || plan.DeploymentRevisionID != grant.Run.Request.Route.DeploymentRevisionID {
		return response, httpadapter.ErrAttemptDenied
	}
	// Recheck after the projection query rather than authorizing with an old lease
	// if manifest loading waited behind another transaction.
	current, err := q.ledger.ActiveToken(ctx, r.WorkloadIdentity, r.ExecutionToken, r.ManifestID, r.ManifestDigest)
	if errors.Is(err, domain.ErrFenced) || errors.Is(err, domain.ErrNotFound) {
		return response, httpadapter.ErrAttemptDenied
	}
	if err != nil {
		return response, httpadapter.ErrUnavailable
	}
	if current.AttemptID != grant.AttemptID || current.LeaseEpoch != grant.LeaseEpoch || current.Generation != grant.Generation {
		return response, httpadapter.ErrAttemptDenied
	}
	uses := make([]proof.CredentialUse, 0, 2)
	for _, use := range plan.Uses() {
		uses = append(uses, proof.CredentialUse{CredentialID: use.CredentialID, Purpose: use.Purpose, AudienceDigest: use.AudienceDigest})
	}
	return proof.AttemptResponse{TenantID: plan.TenantID, ProfileID: plan.ProfileID, ProfileRevisionNumber: plan.ProfileRevision, RunID: grant.Run.Request.RunID, AttemptID: grant.AttemptID, WorkerID: grant.WorkerID, LeaseEpoch: grant.LeaseEpoch, ExpiresAt: current.LeaseUntil, ManifestID: plan.ManifestID, ManifestDigest: plan.ManifestDigest, AllowedUses: uses}, nil
}
func (q proofQueries) VerifyFinal(ctx context.Context, r proof.FinalRequest) (proof.FinalResponse, error) {
	f, err := q.ledger.Final(ctx, r.IntentID)
	if errors.Is(err, domain.ErrNotFound) {
		return proof.FinalResponse{}, httpadapter.ErrNotYet
	}
	if err != nil {
		return proof.FinalResponse{}, httpadapter.ErrUnavailable
	}
	actual := proof.FinalRequest{IntentID: f.IntentID, Digest: f.Digest, AdmissionID: f.AdmissionID, RunID: f.RunID, AttemptID: f.AttemptID, CompletionID: f.CompletionID, ExecutionGeneration: f.ExecutionGeneration, Sequence: f.Sequence}
	if actual != r {
		return proof.FinalResponse{}, httpadapter.ErrFinalMismatch
	}
	return proof.FinalResponse{FinalRequest: actual, TenantID: f.TenantID, ManifestDigest: f.ManifestDigest}, nil
}
