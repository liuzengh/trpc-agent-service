package application

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

// Ledger owns the Final barrier, ordered parts and the A1/A2/result transactions.
// No Provider call occurs within a Ledger method. Time authorization uses DB time.
type Ledger interface {
	Find(context.Context, string) (domain.Receipt, string, bool, error)
	Accept(context.Context, domain.Prepared) (domain.Receipt, error)
	Get(context.Context, string) (domain.Snapshot, error)
	ClaimDue(context.Context, domain.ClaimRequest) ([]domain.Claim, error)
	FinishPreparation(context.Context, domain.Claim, domain.Result) error
	MarkCalling(context.Context, domain.CallingRequest) (domain.Attempt, error)
	Finish(context.Context, domain.Attempt, domain.Result) error
	Observe(context.Context, domain.Observation) error
	ResolveObserved(context.Context, string) (bool, error)
	Observations(context.Context, string) ([]domain.StoredObservation, error)
	RecoverExpiredClaims(context.Context, int) (int, error)
	RecoverStaleCalling(context.Context, int) (int, error)
}

type AdmissionReplyReader interface {
	ReadReplyTarget(context.Context, string, string) (domain.Target, error)
}

// FinalAuthorization is an immutable committed output record, not a live lease
// and not a claim made by a Worker transport message. Its owner is Execution.
type FinalAuthorization struct {
	IntentID, Digest, AdmissionID, RunID, AttemptID, CompletionID string
	ExecutionGeneration, Sequence                                 int64
	TenantID, ManifestDigest                                      string
}
type CommittedFinalVerifier interface {
	VerifyCommittedFinal(context.Context, domain.Intent, string) (FinalAuthorization, error)
}

type SendRequest struct {
	Claim                    domain.Claim
	RequestID, RequestDigest string
}
type SenderProvider interface {
	Reserve(context.Context, SendRequest) (ReservedSender, error)
}

// Reserve performs no Provider side effect. SendFinal is used only after A2.
// Release is idempotent and is deferred until bounded evidence persistence ends.
type ReservedSender interface {
	SendFinal(context.Context, domain.Attempt) domain.Result
	Release()
}

// TracedClaim carries durable technical context separately from send authority.
type TracedClaim struct {
	domain.Claim
	Carrier tracecontext.Carrier
}
type TracedClaimer interface {
	ClaimTraced(context.Context, domain.ClaimRequest) ([]TracedClaim, error)
}

// ArtifactReader returns bytes for an attachment already bound to a committed
// Final; implementations authenticate to Worker, never to an object store.
type ArtifactReader interface {
	ReadReplyArtifact(context.Context, domain.Intent, domain.Attachment) ([]byte, error)
}
