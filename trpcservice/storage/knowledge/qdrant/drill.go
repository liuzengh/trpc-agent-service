package qdrant

import (
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// NewMigrationDrill wires two exact Qdrant bindings into the durable Knowledge
// migration driver. The caller still owns the PostgreSQL authority, ledger and
// durable probe source; this helper prevents source/target direction mistakes.
func NewMigrationDrill(authority migration.Repository, ledger knowledgedriver.MutationLedger, probes knowledgedriver.ProbeSource, source, target *Adapter) (knowledgedriver.Driver, error) {
	if authority == nil || ledger == nil || probes == nil || source == nil || target == nil || source.snapshotWatermark != target.snapshotWatermark {
		return knowledgedriver.Driver{}, runtime.ErrInvariantViolation
	}
	return knowledgedriver.Driver{Authority: authority, Ledger: ledger,
		Source: source, Backfill: source, Target: target,
		ReverseSource: target, ReverseReplica: source,
		SourceInventory: source, TargetInventory: target,
		ProbeSource: probes, SearchTarget: target}, nil
}
