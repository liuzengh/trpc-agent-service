package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockKey is an arbitrary but fixed advisory-lock key, spelling "trpc"
// and a schema generation in hex. Its only job is to be the same number in every
// process that runs this migration and a number no unrelated code is likely to
// pick.
//
// It is deliberately neither the control plane's key nor the session
// directory's. These tables are independent of both, and sharing a key would
// only serialise unrelated startups against each other.
const migrationLockKey int64 = 0x7472_7063_7403_0001

const acquireMigrationLockSQL = `SELECT pg_advisory_xact_lock($1)`

// migrations brings an empty schema up to the current channel-pipeline shape.
//
// The whole list is a single transaction, and DDL in PostgreSQL is
// transactional, so a failure half way leaves nothing behind. Every statement is
// IF NOT EXISTS and the transaction takes an advisory lock first; both are
// needed, because IF NOT EXISTS alone still races — two sessions creating the
// same table concurrently both pass the existence check and the loser fails on
// pg_type's unique index rather than being skipped.
//
// This is deliberately not a migration framework. There is no version table and
// no rollback: the list describes the schema the current build needs, and every
// statement in it is written to be a no-op against a database that already has
// it. That is what lets an ALTER for a column a later build added sit next to
// the CREATE that includes it — a fresh database gets the column from the
// CREATE and skips the ALTER, an existing one skips the CREATE and gets the
// column from the ALTER, and both end up with the same schema. It also means
// re-running this is always safe, which is the property startup depends on.
//
// The list only grows in ways that keep that true. A change that cannot be
// expressed as an idempotent statement — dropping a column, narrowing a type,
// anything needing a backfill that is not itself re-runnable — does need a real
// migration tool, and the shape of a statement here is the test for which kind
// a change is.
//
// # Decisions that are not visible in the DDL
//
// Bodies and delivery targets are text, not jsonb. jsonb normalises key order
// and whitespace, so a delivery target would not come back byte-identical to
// what an adapter stored — and a delivery target is a blob defined and parsed
// by that adapter, not by this platform, so silently rewriting it is a change
// to data this package does not own. Storing it as text also means the database
// offers no convenient way to index or query into a message body, which for
// columns that hold third-party conversation content is a feature.
//
// There are no foreign keys between these three tables, and none out of the set.
// Not to the control plane, because it may be in another schema or another
// database. Not between the tables themselves, because retention for a
// conversation body, an execution record and a delivery receipt is not the same
// policy, and a foreign key would make purging one depend on purging the others
// in the right order. The pipeline contract also rules out any key that would
// cascade a delete into stored content.
//
// The CHECK constraints restate invariants the Go code already enforces. That
// duplication is the point: the Go code is what runs, and the constraints are
// what stops an operator's UPDATE, a botched backfill or a future second writer
// from leaving a row no reader can interpret. In particular the claim and send
// checks state that a row is claimed exactly when it carries a whole claim,
// which is what makes an attempt token a real fence — no row can be in-flight
// without a token to fence it with.
var migrations = []string{
	// channel_inbox_messages holds one row per accepted external event, and is
	// the only table here that stores the third-party payload.
	//
	// The uniqueness that matters is (tenant_id, channel_binding_id,
	// external_event_id): IM platforms redeliver, so the same event arrives
	// several times, and this index is what turns the second delivery into a
	// duplicate result instead of a second answer. It is scoped to the binding
	// rather than the tenant because two bindings are two different upstream
	// platforms whose event id spaces have nothing to do with each other.
	`CREATE TABLE IF NOT EXISTS channel_inbox_messages (
		tenant_id          text        NOT NULL,
		inbox_id           text        NOT NULL,
		channel            text        NOT NULL,
		channel_binding_id text        NOT NULL,
		agent_app_id       text        NOT NULL,
		principal_id       text        NOT NULL,
		session_id         text        NOT NULL,
		external_event_id  text        NOT NULL,
		received_at        timestamptz NOT NULL,
		accepted_at        timestamptz NOT NULL,
		message            text        NOT NULL,
		delivery_channel   text        NOT NULL,
		delivery_version   integer     NOT NULL,
		delivery_payload   text        NOT NULL,
		CONSTRAINT channel_inbox_messages_pkey PRIMARY KEY (tenant_id, inbox_id),
		CONSTRAINT channel_inbox_messages_event_key
			UNIQUE (tenant_id, channel_binding_id, external_event_id),
		CONSTRAINT channel_inbox_messages_external_event_id_check CHECK (
			external_event_id <> '' AND octet_length(external_event_id) <= 256),
		CONSTRAINT channel_inbox_messages_delivery_version_check CHECK (
			delivery_version > 0 AND delivery_version <= 65535),
		CONSTRAINT channel_inbox_messages_size_check CHECK (
			octet_length(message) <= 262144 AND octet_length(delivery_payload) <= 8192)
	)`,

	// channel_agent_runs holds one row per accepted event, created in the same
	// transaction as that event.
	//
	// The primary key leads with tenant_id so no lookup can reach another
	// tenant's row by guessing an id. run_id additionally carries a global
	// unique index, which is what lets a wakeup carrying only a run id be
	// resolved without the platform having to put a tenant id on the wire.
	//
	// accept_sequence is unique within a Session and is the definition of
	// "in order" for that conversation. It is not received_at, which comes from
	// a third party, and not created_at, which is a clock: it is a counter
	// assigned under a transaction-level lock on the Session key.
	`CREATE TABLE IF NOT EXISTS channel_agent_runs (
		tenant_id            text        NOT NULL,
		run_id               text        NOT NULL,
		request_id           text        NOT NULL,
		inbox_id             text        NOT NULL,
		channel              text        NOT NULL,
		channel_binding_id   text        NOT NULL,
		agent_app_id         text        NOT NULL,
		principal_id         text        NOT NULL,
		session_id           text        NOT NULL,
		accept_sequence      bigint      NOT NULL,
		status               text        NOT NULL,
		attempt              integer     NOT NULL,
		max_attempts         integer     NOT NULL,
		max_run_duration_ms  bigint      NOT NULL,
		recovery_grace_ms    bigint      NOT NULL,
		next_attempt_at      timestamptz NOT NULL,
		last_dispatched_at   timestamptz,
		claim_token          text        NOT NULL DEFAULT '',
		claimed_by           text        NOT NULL DEFAULT '',
		claimed_at           timestamptz,
		execute_deadline_at  timestamptz,
		recover_after        timestamptz,
		execution_started_at timestamptz,
		-- Nullable and without a default: "this Run has never reached the
		-- Runner" has to be representable, and a default would make every row
		-- created by this statement claim otherwise. It is deliberately absent
		-- from the claim CHECK below, because unlike execution_started_at it
		-- outlives the claim that set it.
		first_execution_started_at timestamptz,
		revision_id          text        NOT NULL DEFAULT '',
		error_type           text        NOT NULL DEFAULT '',
		event_count          integer     NOT NULL DEFAULT 0,
		output_parts         integer     NOT NULL DEFAULT 0,
		execution_millis     bigint      NOT NULL DEFAULT 0,
		finished_at          timestamptz,
		created_at           timestamptz NOT NULL,
		updated_at           timestamptz NOT NULL,
		CONSTRAINT channel_agent_runs_pkey PRIMARY KEY (tenant_id, run_id),
		CONSTRAINT channel_agent_runs_run_id_key UNIQUE (run_id),
		CONSTRAINT channel_agent_runs_request_key UNIQUE (tenant_id, request_id),
		CONSTRAINT channel_agent_runs_inbox_key UNIQUE (tenant_id, inbox_id),
		CONSTRAINT channel_agent_runs_sequence_key UNIQUE
			(tenant_id, agent_app_id, principal_id, session_id, accept_sequence),
		CONSTRAINT channel_agent_runs_status_check CHECK (
			status IN ('accepted', 'running', 'succeeded', 'failed')),
		CONSTRAINT channel_agent_runs_sequence_check CHECK (accept_sequence > 0),
		CONSTRAINT channel_agent_runs_max_attempts_check CHECK (
			max_attempts >= 1 AND max_attempts <= 16),
		-- The attempt cap lives here as well as in the claim statement. The
		-- statement is what stops a Worker taking a seventeenth attempt; this
		-- is what stops anything else creating a row that says it did.
		CONSTRAINT channel_agent_runs_attempt_check CHECK (
			attempt >= 0 AND attempt <= max_attempts),
		CONSTRAINT channel_agent_runs_budget_check CHECK (
			max_run_duration_ms > 0 AND recovery_grace_ms > 0),
		CONSTRAINT channel_agent_runs_stats_check CHECK (
			event_count >= 0 AND output_parts >= 0 AND execution_millis >= 0),
		-- A Run is running exactly when it carries a whole claim, and carries
		-- none of it otherwise. Without this a row could be running with no
		-- token, which is a Run nobody can finish and recovery cannot date.
		CONSTRAINT channel_agent_runs_claim_check CHECK (
			(status = 'running') = (claim_token <> '')
			AND (claim_token <> '') = (claimed_at IS NOT NULL)
			AND (claim_token <> '') = (execute_deadline_at IS NOT NULL)
			AND (claim_token <> '') = (recover_after IS NOT NULL)
			AND (execution_started_at IS NULL OR claim_token <> '')),
		-- A terminal Run is dated, a failed one is classified, and a succeeded
		-- one is not: an outcome nobody can read is not worth storing.
		CONSTRAINT channel_agent_runs_terminal_check CHECK (
			(status IN ('succeeded', 'failed')) = (finished_at IS NOT NULL)
			AND (status <> 'failed' OR error_type <> '')
			AND (status <> 'succeeded' OR error_type = ''))
	)`,

	// first_execution_started_at for a table that predates it. The CREATE above
	// is a no-op on such a database, so without this pair an existing
	// deployment would keep a runs table the current reader cannot scan.
	//
	// Adding a nullable column with no default is a catalogue-only change in
	// PostgreSQL 11 and later: no table rewrite, no lock held for the length of
	// one. That is what makes it safe to run at startup on a large table.
	`ALTER TABLE channel_agent_runs
		ADD COLUMN IF NOT EXISTS first_execution_started_at timestamptz`,

	// What can be recovered of the history, and only that. A Run that is
	// running right now still carries the transient mark of the attempt that is
	// executing, so for those rows the two marks are the same instant and the
	// backfill is exact.
	//
	// Nothing else is recoverable. Every path that ends an attempt clears
	// execution_started_at, so a Run that ran and was requeued, finished or
	// abandoned before this migration has no record of when it first reached
	// the Runner, and the attempt counter cannot stand in for one: it counts
	// claims, and a Worker that died before starting is indistinguishable from
	// one that died after. Those rows keep NULL, which reads as "unknown"
	// rather than as a guess an operator would later act on.
	`UPDATE channel_agent_runs
		SET first_execution_started_at = execution_started_at
		WHERE first_execution_started_at IS NULL AND execution_started_at IS NOT NULL`,

	// channel_outbox_messages holds one row per part of one answer.
	//
	// (tenant_id, channel_binding_id, idempotency_key) is what makes a
	// re-executed Run reuse the parts its first execution created instead of
	// queueing a second copy of the same answer. The key is derived from the
	// request id and the part number, so it is stable across attempts even
	// though the text may not be.
	`CREATE TABLE IF NOT EXISTS channel_outbox_messages (
		tenant_id           text        NOT NULL,
		outbox_id           text        NOT NULL,
		run_id              text        NOT NULL,
		request_id          text        NOT NULL,
		channel             text        NOT NULL,
		channel_binding_id  text        NOT NULL,
		session_id          text        NOT NULL,
		part_no             integer     NOT NULL,
		idempotency_key     text        NOT NULL,
		client_message_id   text        NOT NULL,
		status              text        NOT NULL,
		attempt             integer     NOT NULL,
		max_attempts        integer     NOT NULL,
		next_attempt_at     timestamptz NOT NULL,
		last_dispatched_at  timestamptz,
		send_token          text        NOT NULL DEFAULT '',
		sent_by             text        NOT NULL DEFAULT '',
		send_deadline_at    timestamptz,
		duplicate_risk      boolean     NOT NULL DEFAULT false,
		error_type          text        NOT NULL DEFAULT '',
		external_message_id text        NOT NULL DEFAULT '',
		sent_at             timestamptz,
		body                text        NOT NULL,
		delivery_channel    text        NOT NULL,
		delivery_version    integer     NOT NULL,
		delivery_payload    text        NOT NULL,
		created_at          timestamptz NOT NULL,
		updated_at          timestamptz NOT NULL,
		CONSTRAINT channel_outbox_messages_pkey PRIMARY KEY (tenant_id, outbox_id),
		CONSTRAINT channel_outbox_messages_idempotency_key
			UNIQUE (tenant_id, channel_binding_id, idempotency_key),
		CONSTRAINT channel_outbox_messages_part_key UNIQUE (tenant_id, run_id, part_no),
		CONSTRAINT channel_outbox_messages_status_check CHECK (
			status IN ('pending', 'sending', 'sent', 'failed')),
		CONSTRAINT channel_outbox_messages_part_no_check CHECK (
			part_no >= 0 AND part_no < 64),
		CONSTRAINT channel_outbox_messages_max_attempts_check CHECK (
			max_attempts >= 1 AND max_attempts <= 16),
		CONSTRAINT channel_outbox_messages_attempt_check CHECK (
			attempt >= 0 AND attempt <= max_attempts),
		CONSTRAINT channel_outbox_messages_delivery_version_check CHECK (
			delivery_version > 0 AND delivery_version <= 65535),
		CONSTRAINT channel_outbox_messages_size_check CHECK (
			octet_length(body) <= 262144 AND octet_length(delivery_payload) <= 8192),
		-- A part is sending exactly when it carries a whole send attempt.
		CONSTRAINT channel_outbox_messages_send_check CHECK (
			(status = 'sending') = (send_token <> '')
			AND (send_token <> '') = (send_deadline_at IS NOT NULL)),
		-- Only a delivered part is dated and carries a channel-side id, and a
		-- delivered part carries no error class.
		CONSTRAINT channel_outbox_messages_sent_check CHECK (
			(status = 'sent') = (sent_at IS NOT NULL)
			AND (status <> 'sent' OR error_type = '')
			AND (status <> 'failed' OR error_type <> '')
			AND (external_message_id = '' OR status = 'sent'))
	)`,

	// The recovery scanner reads only running Runs whose grace has expired, so
	// the index is partial. On a healthy system that predicate matches almost
	// nothing, which is the difference between a scan every few seconds costing
	// an index probe and it costing a sequential scan of every Run ever.
	`CREATE INDEX IF NOT EXISTS channel_agent_runs_recovery_idx
		ON channel_agent_runs (recover_after)
		WHERE status = 'running'`,

	// The dispatch scanner reads due accepted Runs in the same order the
	// in-memory Store returns them, so the index covers the ordering too.
	`CREATE INDEX IF NOT EXISTS channel_agent_runs_dispatch_idx
		ON channel_agent_runs (next_attempt_at, tenant_id, run_id)
		WHERE status = 'accepted'`,

	// ClaimNextRun looks up one Session's earliest unfinished Run. The unique
	// sequence index above can serve that, but only by scanning past however
	// many finished Runs the conversation has accumulated; this partial index
	// holds just the unfinished ones.
	`CREATE INDEX IF NOT EXISTS channel_agent_runs_pending_idx
		ON channel_agent_runs (tenant_id, agent_app_id, principal_id, session_id, accept_sequence)
		WHERE status IN ('accepted', 'running')`,

	// Listing a Run's parts is the read after every finish.
	`CREATE INDEX IF NOT EXISTS channel_outbox_messages_run_idx
		ON channel_outbox_messages (tenant_id, run_id, part_no)`,

	// The two Outbox scanners mirror the two Run scanners.
	`CREATE INDEX IF NOT EXISTS channel_outbox_messages_recovery_idx
		ON channel_outbox_messages (send_deadline_at)
		WHERE status = 'sending'`,

	`CREATE INDEX IF NOT EXISTS channel_outbox_messages_dispatch_idx
		ON channel_outbox_messages (next_attempt_at, tenant_id, outbox_id)
		WHERE status = 'pending'`,

	// The claim index used to lead with channel_binding_id, from when a
	// dispatcher was pinned to one binding and claimed only that binding's
	// parts. It no longer is: a shared pool claims a tenant's due parts in
	// deadline order regardless of binding, so leading with a column the query
	// does not mention would leave the index unusable for it. Dropping the old
	// name and creating a new one keeps both statements no-ops on the second
	// run, which is what this list requires of every change; reusing the name
	// would mean dropping and rebuilding the index on every startup.
	`DROP INDEX IF EXISTS channel_outbox_messages_claim_idx`,

	`CREATE INDEX IF NOT EXISTS channel_outbox_messages_ready_idx
		ON channel_outbox_messages (tenant_id, next_attempt_at, outbox_id)
		WHERE status = 'pending'`,
}

// Migrate creates the channel pipeline tables if they are not already there.
//
// It is safe to call concurrently from several processes and safe to call again
// on an already-migrated database; both are the normal case when every worker
// migrates on startup.
//
// It is not called by New. A caller decides when schema changes happen.
//
// Migrate borrows pool and does not close it. It acts on the first schema of
// the pool's search_path, so a pool pointed at one schema migrates that schema
// only.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if pool == nil {
		return errInvalidPool()
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return storageError(ctx, "begin migration", err)
	}
	// A no-op once the transaction has committed.
	defer func() { _ = tx.Rollback(ctx) }()

	// Held until this transaction ends, so it covers every statement below
	// without a separate release path on the error return.
	if _, err := tx.Exec(ctx, acquireMigrationLockSQL, migrationLockKey); err != nil {
		return storageError(ctx, "acquire migration lock", err)
	}
	for i, statement := range migrations {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return storageError(ctx, fmt.Sprintf("apply migration statement %d", i+1), err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return storageError(ctx, "commit migration", err)
	}
	return nil
}
