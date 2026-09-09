// Package recovery implements the P2-02 local PostgreSQL logical
// backup/restore and bounded session-event replay boundary.
//
// Scope: offline operator tooling for an isolated local drill. It never
// starts business workers, dispatchers, vector workers or telemetry
// exporters, never adds a public HTTP/admin endpoint, never grants the
// runtime role extra privileges and never prints DSNs, passwords, tenant
// identifiers, business identifiers or data content. Every failure is
// reduced to one stable sentinel error (mapped to a stable process exit
// category by the command entrypoint).
//
// NOT INCLUDED in this boundary (must stay documented as such): production
// backup scheduling/retention/encryption/off-site replication, PITR/WAL
// archiving, production restore cutover/RPO/RTO, Object Storage byte
// backup, Secret Manager value backup, Milvus index backup and any claim of
// whole-system event sourcing.
package recovery

import "errors"

var (
	// ErrInvalidConfig covers malformed operator input: missing DSN or
	// directories, invalid paths, unsupported options.
	ErrInvalidConfig = errors.New("recovery: invalid configuration")
	// ErrDependencyUnavailable covers unreachable PostgreSQL servers and
	// missing or unusable pg_dump/pg_restore client tooling.
	ErrDependencyUnavailable = errors.New("recovery: dependency unavailable")
	// ErrIntegrityMismatch covers archive/manifest checksum mismatches,
	// truncated archives, damaged manifests, missing completion markers and
	// archive TOC contents outside the expected data-only table set.
	ErrIntegrityMismatch = errors.New("recovery: integrity mismatch")
	// ErrForbiddenState covers refused preconditions: non-empty restore
	// target, migration version/checksum mismatch, schema drift, restore
	// role without the required FORCE-RLS exemption, restore target equal
	// to the backup source, unknown catalog objects.
	ErrForbiddenState = errors.New("recovery: forbidden state")
	// ErrReplayViolation covers session-event integrity violations found by
	// the bounded replay (gap, unknown event type, illegal payload, parent
	// rule, watermark regression). No row is ever modified because of it.
	ErrReplayViolation = errors.New("recovery: replay integrity violation")
	// ErrRestoreFailed covers pg_restore execution failures under the
	// single-transaction restore protocol (the target stays migrations-only
	// and empty afterwards).
	ErrRestoreFailed = errors.New("recovery: restore failed")
	// ErrBackupFailed covers pg_dump execution failures and backup
	// publication failures (no acceptable artifact is published).
	ErrBackupFailed = errors.New("recovery: backup failed")
	// ErrTimeoutOrCancelled covers context deadline/timeout and explicit
	// cancellation of any backup/verify/restore/replay step.
	ErrTimeoutOrCancelled = errors.New("recovery: timeout or cancelled")
)
