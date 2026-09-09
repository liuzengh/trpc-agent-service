package datamigration

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
)

// The route-changing hooks commit phase, tenant configuration and audit in
// one transaction. Executor's subsequent Advance is an idempotent receipt.
type liveExecutorStore struct{ *PostgresStore }

func (s *liveExecutorStore) Advance(ctx context.Context, id, owner string, version int64, patch JobPatch) (Job, error) {
	if patch.Phase != nil && (*patch.Phase == PhaseRollbackWindow || *patch.Phase == PhaseComplete || *patch.Phase == PhaseRolledBack) {
		job, err := s.Get(ctx, id)
		if err != nil {
			return Job{}, err
		}
		if job.Phase == *patch.Phase {
			patch.Phase = nil
		}
	}
	return s.PostgresStore.Advance(ctx, id, owner, version, patch)
}

func (c *LiveCoordinator) executor(actor, reason string) *Executor {
	return &Executor{Store: &liveExecutorStore{NewPostgresStore(c.db)}, Source: liveSource{c}, Target: liveTarget{c},
		Owner: c.owner, LeaseTTL: c.leaseTTL, BatchSize: c.batchSize,
		Hooks: Hooks{
			Prepare: func(ctx context.Context, job Job, _ LeaseFence) error {
				return c.withJob(ctx, job.ID, func(access *liveAccess) error {
					_, _, _, err := access.source.Keys(ctx, "", 1)
					return err
				})
			},
			EnableDualWrite: func(ctx context.Context, job Job, _ LeaseFence) error {
				return c.withJob(ctx, job.ID, func(access *liveAccess) error {
					if !access.route.Mirroring || !access.route.ScanDone || access.route.WriteProfile != job.SourceProfile {
						return ErrMigrationConflict
					}
					return c.drain(ctx, access)
				})
			},
			Validate: func(ctx context.Context, job Job, _ LeaseFence) error {
				return c.withJob(ctx, job.ID, func(access *liveAccess) error { return c.verifyAll(ctx, access) })
			},
			ShadowRead: func(ctx context.Context, job Job, _ LeaseFence) error {
				return c.withJob(ctx, job.ID, func(access *liveAccess) error { return c.verifyAll(ctx, access) })
			},
			Cutover: func(ctx context.Context, job Job, _ LeaseFence) error {
				return c.switchRoute(ctx, job.ID, PhaseRollbackWindow, actor, reason)
			},
			Complete: func(ctx context.Context, job Job, _ LeaseFence) error {
				return c.switchRoute(ctx, job.ID, PhaseComplete, actor, reason)
			},
			Rollback: func(ctx context.Context, job Job, _ LeaseFence) error {
				return c.switchRoute(ctx, job.ID, PhaseRolledBack, actor, reason)
			},
		}}
}

func (c *LiveCoordinator) RunOnce(ctx context.Context, id string) (Job, error) {
	if c == nil || c.db == nil {
		return Job{}, ErrMigrationCapability
	}
	return c.executor("", "").RunOnce(ctx, id)
}

func (c *LiveCoordinator) Complete(ctx context.Context, id, actor, reason string) (Job, error) {
	if err := validateLiveActor(actor, reason); err != nil {
		return Job{}, err
	}
	job, err := c.Get(ctx, id)
	if err != nil || job.Phase == PhaseComplete {
		return job, err
	}
	return c.executor(actor, reason).Complete(ctx, id)
}

func (c *LiveCoordinator) Rollback(ctx context.Context, id, actor, reason string) (Job, error) {
	if err := validateLiveActor(actor, reason); err != nil {
		return Job{}, err
	}
	job, err := c.Get(ctx, id)
	if err != nil || job.Phase == PhaseRolledBack {
		return job, err
	}
	return c.executor(actor, reason).Rollback(ctx, id)
}

func (c *LiveCoordinator) Pause(ctx context.Context, id, actor, reason string) (Job, error) {
	return c.setPaused(ctx, id, actor, reason, true)
}

func (c *LiveCoordinator) Resume(ctx context.Context, id, actor, reason string) (Job, error) {
	return c.setPaused(ctx, id, actor, reason, false)
}

// Abort ends a pre-cutover migration without touching source data. The
// source profile must still be the configured authority; unrelated tenant
// configuration revisions do not prevent this recovery action.
func (c *LiveCoordinator) Abort(ctx context.Context, id, actor, reason string) (result Job, resultErr error) {
	if c == nil || c.db == nil {
		return Job{}, ErrMigrationCapability
	}
	if err := validateLiveActor(actor, reason); err != nil {
		return Job{}, err
	}
	ctx = nonNilMigrationContext(ctx)
	store := NewPostgresStore(c.db)
	job, err := store.Claim(ctx, id, c.owner, c.leaseTTL)
	if err != nil {
		return Job{}, err
	}
	defer func() {
		releaseCtx, cancel := migrationPersistenceContext(ctx)
		defer cancel()
		resultErr = errors.Join(resultErr, store.Release(releaseCtx, id, c.owner, job.LeaseVersion))
	}()
	err = c.executor(actor, reason).withLease(ctx, job, func(leaseCtx context.Context) (resultErr error) {
		gate, err := c.gate(leaseCtx, job.TenantID, job.Domain, false)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, gate.close()) }()
		route, found, err := c.route(leaseCtx, gate.conn, job.TenantID, job.Domain)
		if err != nil {
			return err
		}
		if !found || route.JobID != id || !route.Mirroring || route.ReadProfile != route.SourceProfile {
			return ErrApprovalRequired
		}
		tx, err := gate.conn.BeginTx(leaseCtx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		backendKey, profileKey, _ := liveStorageKeys(job.Domain)
		var version int64
		var backend, profile string
		if err := tx.QueryRowContext(leaseCtx, `SELECT config_version,COALESCE(config->'storage'->>$2,''),
			COALESCE(config->'storage'->>$3,'') FROM tenants WHERE id=$1 FOR UPDATE`,
			job.TenantID, backendKey, profileKey).Scan(&version, &backend, &profile); err != nil {
			return err
		}
		if backend != route.SourceBackend || profile != route.SourceProfile {
			return ErrMigrationConflict
		}
		res, err := tx.ExecContext(leaseCtx, `UPDATE data_migration_live_routes SET mirroring=FALSE,
			read_profile=$2,write_profile=$2,config_version=$3,updated_at=clock_timestamp() WHERE migration_id=$1`,
			id, route.SourceProfile, version)
		if err := requireLiveRow(res, err); err != nil {
			return err
		}
		if err := liveAudit(leaseCtx, tx, route, actor, reason, "data_migration.abort"); err != nil {
			return err
		}
		res, err = tx.ExecContext(leaseCtx, `UPDATE data_migrations SET phase='ROLLED_BACK',paused=FALSE,
			last_error='',updated_at=clock_timestamp() WHERE id=$1 AND lease_owner=$2 AND lease_version=$3
			AND lease_until>clock_timestamp() AND phase IN ('PREPARE','SNAPSHOT_COPY','DUAL_WRITE','CATCH_UP','VALIDATE','READ_SHADOW','CUTOVER')`,
			id, c.owner, job.LeaseVersion)
		if err := requireLiveRow(res, err); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		return job, err
	}
	return store.Get(ctx, id)
}

func (c *LiveCoordinator) setPaused(ctx context.Context, id, actor, reason string, paused bool) (result Job, resultErr error) {
	if c == nil || c.db == nil {
		return Job{}, ErrMigrationCapability
	}
	if err := validateLiveActor(actor, reason); err != nil {
		return Job{}, err
	}
	ctx = nonNilMigrationContext(ctx)
	store := NewPostgresStore(c.db)
	job, err := store.Claim(ctx, id, c.owner, c.leaseTTL)
	if err != nil {
		return Job{}, err
	}
	defer func() {
		releaseCtx, cancel := migrationPersistenceContext(ctx)
		defer cancel()
		resultErr = errors.Join(resultErr, store.Release(releaseCtx, id, c.owner, job.LeaseVersion))
	}()
	err = c.executor(actor, reason).withLease(ctx, job, func(leaseCtx context.Context) (resultErr error) {
		gate, err := c.gate(leaseCtx, job.TenantID, job.Domain, false)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, gate.close()) }()
		route, found, err := c.route(leaseCtx, gate.conn, job.TenantID, job.Domain)
		if err != nil {
			return err
		}
		if !found || route.JobID != id || !route.Mirroring {
			return ErrMigrationConflict
		}
		tx, err := gate.conn.BeginTx(leaseCtx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		res, err := tx.ExecContext(leaseCtx, `UPDATE data_migrations SET paused=$4,updated_at=clock_timestamp()
			WHERE id=$1 AND lease_owner=$2 AND lease_version=$3 AND lease_until>clock_timestamp()`,
			id, c.owner, job.LeaseVersion, paused)
		if err := requireLiveRow(res, err); err != nil {
			return err
		}
		action := "data_migration.resume"
		if paused {
			action = "data_migration.pause"
		}
		if err := liveAudit(leaseCtx, tx, route, actor, reason, action); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		return job, err
	}
	return store.Get(ctx, id)
}

func requireLiveRow(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrMigrationFence
	}
	return nil
}

func (c *LiveCoordinator) withJob(ctx context.Context, id string, fn func(*liveAccess) error) (resultErr error) {
	return c.withJobBackends(ctx, id, true, fn)
}

func (c *LiveCoordinator) withJobBackends(ctx context.Context, id string, needTarget bool, fn func(*liveAccess) error) (resultErr error) {
	job, err := c.Get(ctx, id)
	if err != nil {
		return err
	}
	gate, err := c.gate(ctx, job.TenantID, job.Domain, false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, gate.close()) }()
	route, found, err := c.route(ctx, gate.conn, job.TenantID, job.Domain)
	if err != nil {
		return err
	}
	if !found || route.JobID != id || !route.Mirroring {
		return ErrMigrationConflict
	}
	if err := verifyLiveFence(ctx, gate.conn, route); err != nil {
		return err
	}
	if needTarget {
		return c.access(ctx, gate, route, fn)
	}
	source, release, err := c.resolveBackend(ctx, route, route.SourceProfile)
	if err != nil {
		return err
	}
	defer release()
	if err := validateLiveProfile(source, route, route.SourceProfile); err != nil {
		return err
	}
	return fn(&liveAccess{gate: gate, route: route, source: source})
}

type liveSource struct{ c *LiveCoordinator }

func (s liveSource) Snapshot(ctx context.Context, tenantID string, domain Domain, cursor string, limit int) (batch Batch, resultErr error) {
	fence, err := LeaseFenceFromContext(ctx)
	if err != nil {
		return Batch{}, err
	}
	err = s.c.withJob(ctx, fence.MigrationID, func(access *liveAccess) error {
		if access.route.TenantID != tenantID || access.route.Domain != domain {
			return ErrMigrationFence
		}
		if err := s.c.drain(ctx, access); err != nil {
			return err
		}
		keys, next, done, err := access.source.Keys(ctx, cursor, limit)
		if err != nil {
			return err
		}
		if len(keys) > limit || (!done && (next == "" || next == cursor)) {
			return ErrCursorStalled
		}
		unique, err := uniqueLiveKeys(keys)
		if err != nil || len(unique) != len(keys) {
			return ErrInvalidRecord
		}
		for _, key := range keys {
			record, err := s.c.captureKey(ctx, access, key)
			if err != nil {
				return err
			}
			batch.Records = append(batch.Records, record)
		}
		if err := access.gate.conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)
			FROM data_migration_live_journal WHERE migration_id=$1`, fence.MigrationID).Scan(&batch.Watermark); err != nil {
			return err
		}
		batch.NextCursor, batch.Done = next, done
		_, err = access.gate.conn.ExecContext(ctx, `UPDATE data_migration_live_routes
			SET scan_cursor=$2,scan_done=$3,updated_at=clock_timestamp() WHERE migration_id=$1`, fence.MigrationID, next, done)
		return err
	})
	return batch, err
}

func (s liveSource) Changes(ctx context.Context, tenantID string, domain Domain, watermark int64, limit int) (batch Batch, resultErr error) {
	fence, err := LeaseFenceFromContext(ctx)
	if err != nil {
		return Batch{}, err
	}
	err = s.c.withJob(ctx, fence.MigrationID, func(access *liveAccess) error {
		if access.route.TenantID != tenantID || access.route.Domain != domain {
			return ErrMigrationFence
		}
		if err := s.c.drain(ctx, access); err != nil {
			return err
		}
		rows, err := access.gate.conn.QueryContext(ctx, liveRecordSelect+
			` WHERE migration_id=$1 AND sequence>$2 ORDER BY sequence LIMIT $3`, fence.MigrationID, watermark, limit+1)
		if err != nil {
			return err
		}
		defer rows.Close()
		batch.Watermark, batch.Done = watermark, true
		seen := make(map[string]bool)
		for rows.Next() {
			record, err := scanLiveRecord(rows)
			if err != nil {
				return err
			}
			// Executor batches forbid repeated keys. End before the repeat,
			// retaining its version for the next Changes call.
			if len(batch.Records) == limit || seen[record.Key] {
				batch.Done = false
				break
			}
			seen[record.Key] = true
			batch.Records = append(batch.Records, record)
			batch.Watermark = record.Version
		}
		batch.NextCursor = strconv.FormatInt(batch.Watermark, 10)
		return rows.Err()
	})
	return batch, err
}

type liveTarget struct{ c *LiveCoordinator }

func (t liveTarget) Upsert(ctx context.Context, tenantID string, domain Domain, fence LeaseFence, records []Record) error {
	contextFence, err := LeaseFenceFromContext(ctx)
	if err != nil || contextFence != fence {
		return ErrMigrationFence
	}
	return t.c.withJob(ctx, fence.MigrationID, func(access *liveAccess) error {
		if access.route.TenantID != tenantID || access.route.Domain != domain {
			return ErrMigrationFence
		}
		// A newer writer may have run between Snapshot and Upsert. Replay the
		// durable ordered journal; older snapshot receipts cannot overwrite it.
		if err := t.c.projectPending(ctx, access); err != nil {
			return err
		}
		for _, record := range records {
			if err := t.c.project(ctx, access, record); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *LiveCoordinator) verifyAll(ctx context.Context, access *liveAccess) error {
	if !access.route.ScanDone {
		return ErrMigrationCapability
	}
	if err := c.drain(ctx, access); err != nil {
		return err
	}
	// The journal inventory contains both the initial scan and every later
	// mutation, including keys no longer returned by source enumeration.
	cursor := ""
	for {
		rows, err := access.gate.conn.QueryContext(ctx, `SELECT record_key FROM (
			SELECT DISTINCT ON (key_hash) key_hash,record_key FROM data_migration_live_journal
			WHERE migration_id=$1 AND key_hash>$2 ORDER BY key_hash,sequence DESC
		) inventory ORDER BY key_hash LIMIT $3`, access.route.JobID, cursor, c.batchSize)
		if err != nil {
			return err
		}
		var keys []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				_ = rows.Close()
				return err
			}
			keys = append(keys, key)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := compareLiveBackends(ctx, access, key); err != nil {
				return err
			}
			cursor = liveKeyHash(key)
		}
		if len(keys) < c.batchSize {
			break
		}
	}
	// Re-enumeration detects a deficient inventory or an unexpected target
	// key. Such a key fails verification instead of being silently deleted.
	for _, backend := range []LiveBackend{access.source, access.target} {
		cursor = ""
		for {
			keys, next, done, err := backend.Keys(ctx, cursor, c.batchSize)
			if err != nil {
				return err
			}
			if len(keys) > c.batchSize || (!done && (next == "" || next == cursor)) {
				return ErrCursorStalled
			}
			for _, key := range keys {
				var known bool
				if err := access.gate.conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM data_migration_live_journal
					WHERE migration_id=$1 AND key_hash=$2 AND record_key=$3)`, access.route.JobID, liveKeyHash(key), key).Scan(&known); err != nil {
					return err
				}
				if !known {
					return ErrLiveVerification
				}
				if err := compareLiveBackends(ctx, access, key); err != nil {
					return err
				}
			}
			if done {
				break
			}
			cursor = next
		}
	}
	return verifyLiveFence(ctx, access.gate.conn, access.route)
}

func compareLiveBackends(ctx context.Context, access *liveAccess, key string) error {
	source, err := readLiveRecord(ctx, access.source, key, 0)
	if err != nil {
		return err
	}
	target, err := readLiveRecord(ctx, access.target, key, 0)
	if err != nil {
		return err
	}
	if !sameLiveRecord(source, target) {
		return ErrLiveVerification
	}
	return nil
}

func (c *LiveCoordinator) switchRoute(ctx context.Context, id string, next Phase, actor, reason string) error {
	rollback := next == PhaseRolledBack
	return c.withJobBackends(ctx, id, !rollback, func(access *liveAccess) error {
		if rollback {
			if _, _, _, err := access.source.Keys(ctx, "", 1); err != nil {
				return err
			}
			if err := c.reconcileSourceOnly(ctx, access); err != nil {
				return err
			}
		} else {
			if err := c.verifyAll(ctx, access); err != nil {
				return err
			}
		}
		route := access.route
		if actor == "" {
			actor, reason = route.Actor, route.Reason
		}
		if err := validateLiveActor(actor, reason); err != nil {
			return err
		}
		fence, err := LeaseFenceFromContext(ctx)
		if err != nil {
			return err
		}
		tx, err := access.gate.conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		job, err := scanJob(tx.QueryRowContext(ctx, migrationSelect+` WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		now, err := databaseNow(ctx, tx)
		if err != nil {
			return err
		}
		if err := checkLease(job, fence.Owner, fence.Version, now); err != nil {
			return err
		}
		if job.Paused || !canTransition(job.Phase, next) {
			return ErrMigrationConflict
		}
		backendKey, profileKey, _ := liveStorageKeys(job.Domain)
		oldBackend, oldProfile := route.SourceBackend, route.SourceProfile
		newBackend, newProfile := route.TargetBackend, route.TargetProfile
		readProfile, writeProfile, mirroring := route.TargetProfile, route.SourceProfile, true
		action := "data_migration.cutover"
		if next == PhaseComplete {
			oldBackend, oldProfile = route.TargetBackend, route.TargetProfile
			writeProfile, mirroring, action = route.TargetProfile, false, "data_migration.complete"
		} else if next == PhaseRolledBack {
			oldBackend, oldProfile = route.TargetBackend, route.TargetProfile
			newBackend, newProfile = route.SourceBackend, route.SourceProfile
			readProfile, writeProfile, mirroring, action = route.SourceProfile, route.SourceProfile, false, "data_migration.rollback"
			var currentBackend, currentProfile string
			if err := tx.QueryRowContext(ctx, `SELECT config_version,COALESCE(config->'storage'->>$2,''),
				COALESCE(config->'storage'->>$3,'') FROM tenants WHERE id=$1 FOR UPDATE`,
				route.TenantID, backendKey, profileKey).Scan(&route.ConfigVersion, &currentBackend, &currentProfile); err != nil {
				return err
			}
			if currentBackend != oldBackend || currentProfile != oldProfile {
				return ErrMigrationConflict
			}
		}
		var configVersion int64
		err = tx.QueryRowContext(ctx, `UPDATE tenants SET config=jsonb_set(jsonb_set(config,
			ARRAY['storage',$2]::text[],to_jsonb($4::text),true),ARRAY['storage',$3]::text[],to_jsonb($5::text),true),
			config_version=config_version+1,updated_at=clock_timestamp()
			WHERE id=$1 AND config_version=$6 AND config->'storage'->>$2=$7
			AND config->'storage'->>$3=$8 AND status='active' RETURNING config_version`,
			route.TenantID, backendKey, profileKey, newBackend, newProfile, route.ConfigVersion, oldBackend, oldProfile).Scan(&configVersion)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrMigrationConflict
		}
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE data_migration_live_routes SET read_profile=$2,write_profile=$3,
			mirroring=$4,config_version=$5,updated_at=clock_timestamp() WHERE migration_id=$1`,
			id, readProfile, writeProfile, mirroring, configVersion)
		if err := requireLiveRow(res, err); err != nil {
			return err
		}
		if err := liveAudit(ctx, tx, route, actor, reason, action); err != nil {
			return err
		}
		res, err = tx.ExecContext(ctx, `UPDATE data_migrations SET phase=$4,last_error='',updated_at=clock_timestamp()
			WHERE id=$1 AND lease_owner=$2 AND lease_version=$3 AND lease_until>clock_timestamp()`,
			id, fence.Owner, fence.Version, next)
		if err := requireLiveRow(res, err); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (c *LiveCoordinator) reconcileSourceOnly(ctx context.Context, access *liveAccess) error {
	for {
		rows, err := access.gate.conn.QueryContext(ctx, `SELECT record_key FROM data_migration_live_intents
			WHERE migration_id=$1 ORDER BY key_hash LIMIT $2`, access.route.JobID, c.batchSize)
		if err != nil {
			return err
		}
		var keys []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				_ = rows.Close()
				return err
			}
			keys = append(keys, key)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if _, err := c.captureKey(ctx, access, key); err != nil {
				return err
			}
			if _, err := access.gate.conn.ExecContext(ctx, `DELETE FROM data_migration_live_intents
				WHERE migration_id=$1 AND key_hash=$2 AND record_key=$3`, access.route.JobID, liveKeyHash(key), key); err != nil {
				return err
			}
		}
		if len(keys) < c.batchSize {
			return verifyLiveFence(ctx, access.gate.conn, access.route)
		}
	}
}

var _ Source = liveSource{}
var _ Target = liveTarget{}
var _ Store = (*liveExecutorStore)(nil)
var _ LeaseRenewer = (*liveExecutorStore)(nil)
