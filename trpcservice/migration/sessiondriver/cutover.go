package sessiondriver

import "github.com/liuzengh/trpc-agent-service/trpcservice/migration"

// These aliases keep Session driver users source-compatible while the shared
// control-plane contract remains independent of Session storage internals.
type SwitchMetadata = migration.SessionSwitchMetadata
type CutoverRequest = migration.SessionCutoverRequest
type ObserveRequest = migration.SessionObserveRequest
type RollbackRequest = migration.SessionRollbackRequest
type CleanupRequest = migration.SessionCleanupRequest
type SwitchResult = migration.SessionSwitchResult
type DrainStatus = migration.SessionDrainStatus
type CutoverPublisher = migration.SessionCutoverPublisher
