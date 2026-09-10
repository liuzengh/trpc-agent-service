package knowledgedriver

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
)

type SwitchMetadata struct {
	SwitchID, ActorID, ReasonCode, CorrelationID, TraceID, Traceparent string
}

type CutoverRequest struct {
	TenantID, MigrationID                  string
	ExpectedTenantVersion, ExpectedVersion int64
	Verification                           migration.Verification
	At                                     time.Time
	Metadata                               SwitchMetadata
}

type ObserveRequest struct {
	TenantID, MigrationID                  string
	ExpectedTenantVersion, ExpectedVersion int64
	At, ObserveUntil                       time.Time
}

type RollbackRequest struct {
	TenantID, MigrationID                  string
	ExpectedTenantVersion, ExpectedVersion int64
	RollbackSyncWatermark                  string
	At                                     time.Time
	Metadata                               SwitchMetadata
}

type CleanupRequest struct {
	TenantID, MigrationID                  string
	ExpectedTenantVersion, ExpectedVersion int64
	RollbackSyncWatermark                  string
	At                                     time.Time
}

type SwitchResult struct {
	Migration           migration.Migration
	TenantVersion       int64
	ActiveConfigVersion int64
	RolledBack          bool
}

type DrainStatus struct {
	SourceInFlight, TargetInFlight         int64
	ForwardOutstanding, ReverseOutstanding int64
	ActiveConfigVersion                    int64
	RolledBack                             bool
}

type CutoverPublisher interface {
	Cutover(context.Context, CutoverRequest) (SwitchResult, error)
	BeginObserve(context.Context, ObserveRequest) (SwitchResult, error)
	Rollback(context.Context, RollbackRequest) (SwitchResult, error)
	Cleanup(context.Context, CleanupRequest) (SwitchResult, error)
	DrainStatus(context.Context, string, string) (DrainStatus, error)
}
