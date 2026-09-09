package datamigration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// PostgresTarget is the concrete target for the Redis -> PostgreSQL vertical
// slice. It validates the migration lease and all record writes in one
// transaction. Older versions are ignored; equal versions must have the same
// content hash, otherwise the migration fails closed on source corruption.
type PostgresTarget struct{ DB *sql.DB }

func NewPostgresTarget(db *sql.DB) *PostgresTarget { return &PostgresTarget{DB: db} }

func (t *PostgresTarget) Upsert(ctx context.Context, tenantID string, domain Domain, fence LeaseFence, records []Record) error {
	if t == nil || t.DB == nil {
		return fmt.Errorf("%w: postgres target is unavailable", ErrMigrationCapability)
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil || !validDomain(domain) {
		return fmt.Errorf("%w: invalid target scope", ErrInvalidMigration)
	}
	if fence.MigrationID == "" || fence.Owner == "" || fence.Version <= 0 {
		return ErrMigrationFence
	}
	normalizedRecords := make([]Record, len(records))
	for i, record := range records {
		if record.Payload == nil {
			// PostgreSQL BYTEA is NOT NULL. Canonicalize an empty/tombstone
			// payload before it crosses the SQL driver boundary.
			record.Payload = []byte{}
		}
		normalizedRecords[i] = record
	}
	records = normalizedRecords
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return err
		}
	}
	tx, err := t.DB.BeginTx(nonNilMigrationContext(ctx), nil)
	if err != nil {
		return fmt.Errorf("begin postgres migration target: %w", err)
	}
	defer tx.Rollback()
	var marker int
	err = tx.QueryRowContext(nonNilMigrationContext(ctx), `
		SELECT 1 FROM data_migrations
		WHERE id=$1 AND lease_owner=$2 AND lease_version=$3
		  AND tenant_id=$4 AND domain=$5
		  AND phase IN ('SNAPSHOT_COPY','CATCH_UP')
		  AND lease_until > clock_timestamp()
		`, fence.MigrationID, fence.Owner, fence.Version, tenantID, domain).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMigrationFence
	}
	if err != nil {
		return fmt.Errorf("check postgres migration target fence: %w", err)
	}
	for _, record := range records {
		// Reserve a missing key atomically. Two workers can both arrive here
		// during dual-write; ON CONFLICT DO NOTHING makes the loser re-read the
		// committed row instead of surfacing a spurious unique-key failure.
		result, err := tx.ExecContext(nonNilMigrationContext(ctx), `
			INSERT INTO data_migration_records
				(tenant_id,domain,record_key,migration_id,payload,version,content_hash,deleted,updated_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp())
				ON CONFLICT (tenant_id,domain,record_key,migration_id) DO NOTHING`, tenantID, domain, record.Key, fence.MigrationID, record.Payload, record.Version, record.Hash, record.Deleted)
		if err != nil {
			return fmt.Errorf("insert postgres migration record %q: %w", record.Key, err)
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect postgres migration record %q insert: %w", record.Key, err)
		}
		if inserted == 1 {
			continue
		}
		var existingVersion int64
		var existingHash string
		var existingDeleted bool
		if err := tx.QueryRowContext(nonNilMigrationContext(ctx), `
			SELECT version, content_hash, deleted FROM data_migration_records
			WHERE tenant_id=$1 AND domain=$2 AND record_key=$3 AND migration_id=$4
			FOR UPDATE`, tenantID, domain, record.Key, fence.MigrationID).Scan(&existingVersion, &existingHash, &existingDeleted); err != nil {
			return fmt.Errorf("read postgres migration record %q after conflict: %w", record.Key, err)
		}
		switch {
		case existingVersion > record.Version:
			continue
		case existingVersion == record.Version:
			if !strings.EqualFold(existingHash, record.Hash) || existingDeleted != record.Deleted {
				return fmt.Errorf("%w: version conflict for record %q", ErrInvalidRecord, record.Key)
			}
			continue
		default:
			_, err = tx.ExecContext(nonNilMigrationContext(ctx), `
				UPDATE data_migration_records
				SET payload=$5, version=$6, content_hash=$7, deleted=$8, projected_at=NULL, updated_at=clock_timestamp()
				WHERE tenant_id=$1 AND domain=$2 AND record_key=$3 AND migration_id=$4`, tenantID, domain, record.Key, fence.MigrationID, record.Payload, record.Version, record.Hash, record.Deleted)
		}
		if err != nil {
			return fmt.Errorf("write postgres migration record %q: %w", record.Key, err)
		}
	}
	// A large batch can outlive the lease after the initial row lock. Recheck
	// immediately before commit; any writes above remain uncommitted and are
	// rolled back when this final fence fails.
	if err := verifyTargetFence(nonNilMigrationContext(ctx), tx, fence, tenantID, domain); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit postgres migration target: %w", err)
	}
	return nil
}

func verifyTargetFence(ctx context.Context, tx *sql.Tx, fence LeaseFence, tenantID string, domain Domain) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE data_migrations SET updated_at=clock_timestamp()
		WHERE id=$1 AND lease_owner=$2 AND lease_version=$3
		  AND tenant_id=$4 AND domain=$5
		  AND phase IN ('SNAPSHOT_COPY','CATCH_UP')
		  AND lease_until > clock_timestamp()
		`, fence.MigrationID, fence.Owner, fence.Version, tenantID, domain)
	if err != nil {
		return fmt.Errorf("recheck postgres migration target fence: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect postgres migration target fence: %w", err)
	}
	if rows != 1 {
		return ErrMigrationFence
	}
	return nil
}
