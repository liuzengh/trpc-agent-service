package task

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

// SourceProjector is the injected source boundary. Production Memory and
// Knowledge repositories remain outside this phase; tests and composition may
// provide an implementation here.
type SourceProjector interface {
	Load(context.Context, tenant.TenantContext, Task) (vector.SourceDocument, error)
}
type SourceProjectorFunc func(context.Context, tenant.TenantContext, Task) (vector.SourceDocument, error)

func (f SourceProjectorFunc) Load(ctx context.Context, tc tenant.TenantContext, task Task) (vector.SourceDocument, error) {
	if f == nil {
		return vector.SourceDocument{}, ErrInvalidTask
	}
	return f(ctx, tc, task)
}

func verifyLoadedSource(task Task, source vector.SourceDocument) Failure {
	if source.SourceType != task.SourceType || source.SourceID != task.SourceID || source.ProjectionScope != task.ProjectionScope {
		return Failure{Kind: FailureDeadLetter, Category: CategoryInvalidRequest}
	}
	if source.Model != task.Model || source.ModelVersion != task.ModelVersion {
		return Failure{Kind: FailureDeadLetter, Category: CategoryModelMismatch}
	}
	if source.Dimension != task.Dimension {
		return Failure{Kind: FailureDeadLetter, Category: CategoryDimensionMismatch}
	}
	if source.SchemaVersion != task.SchemaVersion {
		return Failure{Kind: FailureDeadLetter, Category: CategorySchemaMismatch}
	}
	if source.SourceVersion != task.SourceVersion || source.SourceSequence != task.SourceSequence {
		return Failure{Kind: FailureStale, Category: CategoryStaleTask}
	}
	if task.Operation == vector.OperationUpsert && source.Deleted {
		return Failure{Kind: FailureDeadLetter, Category: CategoryInvalidRequest}
	}
	if task.Operation == vector.OperationDelete && !source.Deleted {
		return Failure{Kind: FailureStale, Category: CategoryStaleTask}
	}
	if vector.ContentHash(source.Content) != task.ContentHash {
		return Failure{Kind: FailureStale, Category: CategoryStaleTask}
	}
	return Failure{}
}
