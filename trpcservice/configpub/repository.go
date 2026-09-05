package configpub

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// BindingOwnership is the server-owned result of resolving a binding reference
// against the durable binding registry during validation.
type BindingOwnership int

const (
	BindingMissing BindingOwnership = iota
	BindingOwned
	BindingForeignTenant
)

// BindingChecker resolves binding references for revision validation. It is
// implemented by the tenant registry boundary; it never returns secret
// material.
type BindingChecker interface {
	// BindingOwnershipFor reports whether (channel, bindingID) is unknown,
	// owned by the tenant, or owned by a different tenant.
	BindingOwnershipFor(ctx context.Context, tenantID, channel, bindingID string) (BindingOwnership, error)
}

// Validator performs the semantic half of revision validation that needs the
// durable binding registry. The pure half lives in Document.validateShape.
type Validator struct {
	Bindings BindingChecker
}

// Validate runs the bounded semantic validation of a document for a tenant.
// It returns a *ValidationError with a bounded category, or a transport-class
// error (mapped to ErrUnavailable by callers) when the checker itself fails.
func (v Validator) Validate(ctx context.Context, tenantID string, doc Document) error {
	if err := doc.validateShape(); err != nil {
		return err
	}
	// The production vector dependency can never be enabled through config
	// publication: derived-vector lifecycle stays governed by P1-06.
	if doc.BackendPolicy.Vector != "none" {
		return validationError(CategoryForbiddenProdDep)
	}
	if v.Bindings == nil {
		return nil
	}
	for _, binding := range doc.Bindings {
		ownership, err := v.Bindings.BindingOwnershipFor(ctx, tenantID, binding.Channel, binding.BindingID)
		if err != nil {
			return err
		}
		switch ownership {
		case BindingOwned:
			// ok
		case BindingForeignTenant:
			return validationError(CategoryCrossTenantRef)
		default:
			return validationError(CategoryUnknownRef)
		}
	}
	return nil
}

// Repository is the durable fact boundary. Implementations must keep every
// multi-row mutation inside one PostgreSQL transaction with CAS guards, must
// never delete revision history, and must map infrastructure failures to
// ErrUnavailable.
type Repository interface {
	// CreateRevision inserts a draft revision. Re-submitting the exact same
	// (tenant, version, content) converges to the stored revision; a version
	// reuse with different content or a duplicate fingerprint fails.
	CreateRevision(ctx context.Context, tc tenant.TenantContext, version int64, doc Document, actorCategory string, now time.Time) (Revision, error)

	// Revision reads one revision. Missing revisions yield ErrRevisionNotFound.
	Revision(ctx context.Context, tenantID string, version int64) (Revision, error)

	// ValidateRevision transitions a draft revision to validated or rejected
	// inside one transaction, using the validator for the semantic checks.
	ValidateRevision(ctx context.Context, tc tenant.TenantContext, version int64, validator Validator, actorCategory string, now time.Time) (Revision, error)

	// Operation returns the recorded operation for an idempotency key.
	Operation(ctx context.Context, tenantID, operationID string) (OperationRecord, bool, error)

	// Publish atomically: CAS on the active version, demotes the previous
	// published revision, activates the validated target, upserts rollout
	// state, and records the committed operation.
	Publish(ctx context.Context, tc tenant.TenantContext, op OperationRecord, percentage int, now time.Time) (OperationRecord, error)

	// SetRollout atomically updates the percentage of the currently active
	// revision under CAS and records the committed operation.
	SetRollout(ctx context.Context, tc tenant.TenantContext, op OperationRecord, percentage int, now time.Time) (OperationRecord, error)

	// Rollback atomically recalls the active revision, re-activates a
	// previously published target under CAS, resets rollout state, and
	// records the committed operation.
	Rollback(ctx context.Context, tc tenant.TenantContext, op OperationRecord, now time.Time) (OperationRecord, error)

	// Rollout reads the durable rollout state. managed=false means the tenant
	// has no publication state and keeps using the registry config version.
	Rollout(ctx context.Context, tenantID string) (RolloutState, bool, error)
}
