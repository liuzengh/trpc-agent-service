package datamigration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

type liveAccess struct {
	gate           *liveGate
	route          liveRoute
	source, target LiveBackend
}

func (c *LiveCoordinator) operationGate(ctx context.Context, tenantID string, domain Domain) (*liveGate, liveRoute, bool, error) {
	gate, err := c.gate(ctx, tenantID, domain, true)
	if err != nil {
		return nil, liveRoute{}, false, err
	}
	route, found, err := c.route(ctx, gate.conn, tenantID, domain)
	if err != nil {
		return nil, liveRoute{}, false, errors.Join(err, gate.close())
	}
	if !found || !route.Mirroring {
		return gate, route, found, nil
	}
	if err := gate.close(); err != nil {
		return nil, liveRoute{}, false, err
	}
	// Never upgrade while holding the shared lock: concurrent upgrades would
	// deadlock. The route is read again after acquiring the exclusive gate.
	gate, err = c.gate(ctx, tenantID, domain, false)
	if err != nil {
		return nil, liveRoute{}, false, err
	}
	route, found, err = c.route(ctx, gate.conn, tenantID, domain)
	if err != nil {
		return nil, liveRoute{}, false, errors.Join(err, gate.close())
	}
	return gate, route, found, nil
}

func (c *LiveCoordinator) access(ctx context.Context, gate *liveGate, route liveRoute, fn func(*liveAccess) error) error {
	source, releaseSource, err := c.resolveBackend(ctx, route, route.SourceProfile)
	if err != nil {
		return err
	}
	defer releaseSource()
	target, releaseTarget, err := c.resolveBackend(ctx, route, route.TargetProfile)
	if err != nil {
		return err
	}
	defer releaseTarget()
	if err := validateLivePeers(source, target, route); err != nil {
		return err
	}
	return fn(&liveAccess{gate: gate, route: route, source: source, target: target})
}

// WithRead resolves the durable profile on every call. A target read during
// the rollback window first reconciles any source write left by a crash.
func (c *LiveCoordinator) WithRead(ctx context.Context, tenantID string, domain Domain, fallbackProfile string, fn func(context.Context, string) error) (resultErr error) {
	if fn == nil {
		return ErrMigrationCapability
	}
	ctx = nonNilMigrationContext(ctx)
	gate, route, found, err := c.operationGate(ctx, tenantID, domain)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, gate.close()) }()
	if !found {
		return fn(ctx, fallbackProfile)
	}
	if !route.Mirroring {
		if err := c.checkSelectedProfile(ctx, route, route.ReadProfile); err != nil {
			return err
		}
		return fn(ctx, route.ReadProfile)
	}
	return c.access(ctx, gate, route, func(access *liveAccess) error {
		if err := c.drain(ctx, access); err != nil {
			return err
		}
		return fn(ctx, route.ReadProfile)
	})
}

// WithWrite captures every supplied key before invoking the source write.
// An empty key set explicitly denotes an unsupported domain mutation: it is
// allowed when idle, but rejected while online migration is active.
func (c *LiveCoordinator) WithWrite(ctx context.Context, tenantID string, domain Domain, fallbackProfile string, keys []string, fn func(context.Context, string) error) error {
	return c.withWrite(ctx, tenantID, domain, fallbackProfile,
		func(context.Context, string) ([]string, error) { return keys, nil }, len(keys) == 0, fn)
}

// WithWriteKeys discovers keys under the active migration's exclusive gate.
// The callback must enumerate every key the mutation may affect, including
// a newly allocated object version. With no active migration it is not run.
func (c *LiveCoordinator) WithWriteKeys(ctx context.Context, tenantID string, domain Domain, fallbackProfile string,
	keysFn func(context.Context, string) ([]string, error), fn func(context.Context, string) error) error {
	return c.withWrite(ctx, tenantID, domain, fallbackProfile, keysFn, false, fn)
}

func (c *LiveCoordinator) withWrite(ctx context.Context, tenantID string, domain Domain, fallbackProfile string,
	keysFn func(context.Context, string) ([]string, error), unsupported bool, fn func(context.Context, string) error) (resultErr error) {
	if keysFn == nil || fn == nil {
		return ErrMigrationCapability
	}
	ctx = nonNilMigrationContext(ctx)
	gate, route, found, err := c.operationGate(ctx, tenantID, domain)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, gate.close()) }()
	if !found {
		return fn(ctx, fallbackProfile)
	}
	if !route.Mirroring {
		if err := c.checkSelectedProfile(ctx, route, route.WriteProfile); err != nil {
			return err
		}
		return fn(ctx, route.WriteProfile)
	}
	if unsupported {
		return fmt.Errorf("%w: mutation has no complete migration record identity", ErrMigrationCapability)
	}
	return c.access(ctx, gate, route, func(access *liveAccess) error {
		// Preserve the sequence delete -> recreate even when the previous
		// target operation failed. No new source mutation precedes recovery.
		if err := c.drain(ctx, access); err != nil {
			return err
		}
		keys, err := keysFn(ctx, route.WriteProfile)
		if err != nil {
			return err
		}
		keys, err = uniqueLiveKeys(keys)
		if err != nil {
			return err
		}
		if err := persistLiveIntents(ctx, gate.conn, route.JobID, keys); err != nil {
			return err
		}
		writeErr := fn(ctx, route.WriteProfile)
		// This detached, bounded attempt is useful after cancellation too. An
		// error from the source keeps intents even if its current data matches.
		captureCtx, cancel := migrationPersistenceContext(ctx)
		defer cancel()
		captureErr := c.recoverKeys(captureCtx, access, keys, writeErr == nil)
		return errors.Join(writeErr, captureErr)
	})
}

func uniqueLiveKeys(keys []string) ([]string, error) {
	if len(keys) > 10000 {
		return nil, fmt.Errorf("%w: mutation affects more than 10000 records", ErrMigrationCapability)
	}
	seen := make(map[string]bool, len(keys))
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" || len(key) > maxRecordKeyBytes || strings.ContainsAny(key, "\x00\r\n") {
			return nil, ErrInvalidRecord
		}
		if !seen[key] {
			seen[key] = true
			result = append(result, key)
		}
	}
	return result, nil
}

func persistLiveIntents(ctx context.Context, conn *sql.Conn, id string, keys []string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, key := range keys {
		var saved string
		err := tx.QueryRowContext(ctx, `INSERT INTO data_migration_live_intents (migration_id,key_hash,record_key)
			VALUES ($1,$2,$3) ON CONFLICT (migration_id,key_hash) DO UPDATE SET record_key=data_migration_live_intents.record_key
			RETURNING record_key`, id, liveKeyHash(key), key).Scan(&saved)
		if err != nil {
			return err
		}
		if saved != key {
			return ErrInvalidRecord
		}
	}
	return tx.Commit()
}

const liveRecordSelect = `SELECT record_key,sequence,content_hash,payload,deleted FROM data_migration_live_journal`

func scanLiveRecord(row rowScanner) (Record, error) {
	var record Record
	err := row.Scan(&record.Key, &record.Version, &record.Hash, &record.Payload, &record.Deleted)
	return record, err
}

func readLiveRecord(ctx context.Context, backend LiveBackend, key string, version int64) (Record, error) {
	record, err := backend.Read(ctx, key, version)
	if err != nil {
		return Record{}, err
	}
	if record.Key != key || record.Version != version {
		return Record{}, ErrInvalidRecord
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	record.Hash = strings.ToLower(record.Hash)
	return record, nil
}

func sameLiveRecord(a, b Record) bool {
	return a.Key == b.Key && a.Deleted == b.Deleted && strings.EqualFold(a.Hash, b.Hash) && bytes.Equal(a.Payload, b.Payload)
}

func (c *LiveCoordinator) captureKey(ctx context.Context, access *liveAccess, key string) (Record, error) {
	if reconciler, ok := access.source.(LiveBackendSourceReconciler); ok {
		if err := reconciler.ReconcileSource(ctx, key); err != nil {
			return Record{}, err
		}
	}
	record, err := readLiveRecord(ctx, access.source, key, 0)
	if err != nil {
		return Record{}, err
	}
	previous, err := scanLiveRecord(access.gate.conn.QueryRowContext(ctx, liveRecordSelect+
		` WHERE migration_id=$1 AND key_hash=$2 ORDER BY sequence DESC LIMIT 1`, access.route.JobID, liveKeyHash(key)))
	if err == nil {
		if previous.Key != key {
			return Record{}, ErrInvalidRecord
		}
		if sameLiveRecord(previous, record) {
			return previous, nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Record{}, err
	}
	if record.Payload == nil {
		record.Payload = []byte{}
	}
	err = access.gate.conn.QueryRowContext(ctx, `INSERT INTO data_migration_live_journal
		(migration_id,key_hash,record_key,payload,content_hash,deleted)
		VALUES ($1,$2,$3,$4,$5,$6) RETURNING sequence`, access.route.JobID, liveKeyHash(key), key,
		record.Payload, record.Hash, record.Deleted).Scan(&record.Version)
	return record, err
}

func verifyLiveFence(ctx context.Context, conn *sql.Conn, route liveRoute) error {
	value := ctx.Value(leaseFenceContextKey{})
	if value == nil {
		return nil
	}
	fence, err := LeaseFenceFromContext(ctx)
	if err != nil || fence.MigrationID != route.JobID {
		return ErrMigrationFence
	}
	var current bool
	err = conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM data_migrations
		WHERE id=$1 AND tenant_id=$2 AND domain=$3 AND lease_owner=$4 AND lease_version=$5
		AND lease_until>clock_timestamp() AND NOT paused)`, route.JobID, route.TenantID, route.Domain,
		fence.Owner, fence.Version).Scan(&current)
	if err != nil {
		return err
	}
	if !current {
		return ErrMigrationFence
	}
	return nil
}

func (c *LiveCoordinator) project(ctx context.Context, access *liveAccess, record Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if err := verifyLiveFence(ctx, access.gate.conn, access.route); err != nil {
		return err
	}
	var projected bool
	if err := access.gate.conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM data_migration_live_journal
		WHERE migration_id=$1 AND key_hash=$2 AND sequence>=$3 AND projected_at IS NOT NULL)`,
		access.route.JobID, liveKeyHash(record.Key), record.Version).Scan(&projected); err != nil {
		return err
	}
	if projected {
		_, err := access.gate.conn.ExecContext(ctx, `UPDATE data_migration_live_journal
			SET projected_at=COALESCE(projected_at,clock_timestamp()) WHERE migration_id=$1 AND sequence=$2`,
			access.route.JobID, record.Version)
		return err
	}
	if err := access.target.Apply(ctx, record); err != nil {
		return err
	}
	actual, err := readLiveRecord(ctx, access.target, record.Key, record.Version)
	if err != nil {
		return err
	}
	if !sameLiveRecord(record, actual) {
		return ErrLiveVerification
	}
	if err := verifyLiveFence(ctx, access.gate.conn, access.route); err != nil {
		return err
	}
	result, err := access.gate.conn.ExecContext(ctx, `UPDATE data_migration_live_journal SET projected_at=clock_timestamp()
		WHERE migration_id=$1 AND sequence=$2 AND key_hash=$3 AND content_hash=$4 AND deleted=$5`,
		access.route.JobID, record.Version, liveKeyHash(record.Key), record.Hash, record.Deleted)
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

func (c *LiveCoordinator) pending(ctx context.Context, access *liveAccess) ([]Record, error) {
	rows, err := access.gate.conn.QueryContext(ctx, liveRecordSelect+
		` WHERE migration_id=$1 AND projected_at IS NULL ORDER BY sequence LIMIT $2`, access.route.JobID, c.batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		record, err := scanLiveRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (c *LiveCoordinator) projectPending(ctx context.Context, access *liveAccess) error {
	for {
		records, err := c.pending(ctx, access)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			if err := c.project(ctx, access, record); err != nil {
				return err
			}
		}
	}
}

func (c *LiveCoordinator) recoverKeys(ctx context.Context, access *liveAccess, keys []string, clear bool) error {
	for _, key := range keys {
		if _, err := c.captureKey(ctx, access, key); err != nil {
			return errors.Join(ErrLiveDirty, err)
		}
	}
	if err := c.projectPending(ctx, access); err != nil {
		return err
	}
	if clear {
		for _, key := range keys {
			if _, err := access.gate.conn.ExecContext(ctx, `DELETE FROM data_migration_live_intents
				WHERE migration_id=$1 AND key_hash=$2 AND record_key=$3`, access.route.JobID, liveKeyHash(key), key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *LiveCoordinator) drain(ctx context.Context, access *liveAccess) error {
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
		if len(keys) == 0 {
			return c.projectPending(ctx, access)
		}
		if err := c.recoverKeys(ctx, access, keys, true); err != nil {
			return err
		}
	}
}

// LiveTombstone is convenient for adapters whose authoritative read proves
// that a key is absent. Missing reads must not be converted to empty values.
func LiveTombstone(key string, version int64) Record {
	digest := sha256.Sum256(nil)
	return Record{Key: key, Version: version, Hash: hex.EncodeToString(digest[:]), Payload: []byte{}, Deleted: true}
}
