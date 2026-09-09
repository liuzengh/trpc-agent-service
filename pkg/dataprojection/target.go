package dataprojection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// Projector is a domain-specific, idempotent external side effect. Apply may
// be invoked again after a process crash between the external write and the
// PostgreSQL projection marker commit.
type Projector interface {
	Domain() datamigration.Domain
	Apply(context.Context, string, datamigration.Record) error
}

// Target combines the durable migration ledger with a concrete domain
// projector. PostgreSQL serializes each record; the final lease
// recheck prevents a stale worker from publishing a successful marker.
type Target struct {
	db        *sql.DB
	projector Projector
}

func NewTarget(db *sql.DB, projector Projector) (*Target, error) {
	if db == nil || projector == nil || !projectionDomain(projector.Domain()) {
		return nil, fmt.Errorf("%w: projection target", datamigration.ErrMigrationCapability)
	}
	return &Target{db: db, projector: projector}, nil
}

func (t *Target) Upsert(ctx context.Context, tenantID string, domain datamigration.Domain, fence datamigration.LeaseFence, records []datamigration.Record) error {
	if t == nil || t.db == nil || t.projector == nil || domain != t.projector.Domain() || !projectionDomain(domain) {
		return fmt.Errorf("%w: projection domain", datamigration.ErrMigrationCapability)
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: projection tenant", datamigration.ErrInvalidMigration)
	}
	if fence.MigrationID == "" || fence.Owner == "" || fence.Version <= 0 {
		return datamigration.ErrMigrationFence
	}
	records = append([]datamigration.Record(nil), records...)
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return err
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	for i := 1; i < len(records); i++ {
		if records[i-1].Key == records[i].Key {
			return fmt.Errorf("%w: duplicate projection key %q", datamigration.ErrInvalidRecord, records[i].Key)
		}
	}
	ctx = nonNilProjectionContext(ctx)
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin projection target: %w", err)
	}
	defer tx.Rollback()
	if err := checkProjectionFence(ctx, tx, tenantID, domain, fence); err != nil {
		return err
	}
	for _, record := range records {
		result, err := tx.ExecContext(ctx, `INSERT INTO data_migration_records
			(tenant_id,domain,record_key,migration_id,payload,version,content_hash,deleted,projected_at,updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULL,clock_timestamp())
			ON CONFLICT (tenant_id,domain,record_key,migration_id) DO NOTHING`,
			tenantID, domain, record.Key, fence.MigrationID, record.Payload, record.Version, record.Hash, record.Deleted)
		if err != nil {
			return fmt.Errorf("reserve projection record %q: %w", record.Key, err)
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect projection reservation %q: %w", record.Key, err)
		}
		shouldApply := inserted == 1
		if inserted == 0 {
			var existingVersion int64
			var existingHash string
			var existingDeleted bool
			var projectedAt sql.NullTime
			if err := tx.QueryRowContext(ctx, `SELECT version,content_hash,deleted,projected_at FROM data_migration_records
				WHERE tenant_id=$1 AND domain=$2 AND record_key=$3 AND migration_id=$4 FOR UPDATE`,
				tenantID, domain, record.Key, fence.MigrationID).Scan(&existingVersion, &existingHash, &existingDeleted, &projectedAt); err != nil {
				return fmt.Errorf("read existing projection record %q: %w", record.Key, err)
			}
			switch {
			case existingVersion > record.Version:
				continue
			case existingVersion == record.Version:
				if !strings.EqualFold(existingHash, record.Hash) || existingDeleted != record.Deleted {
					return fmt.Errorf("%w: version conflict for projection record %q", datamigration.ErrInvalidRecord, record.Key)
				}
				shouldApply = !projectedAt.Valid
			default:
				result, err = tx.ExecContext(ctx, `UPDATE data_migration_records
					SET payload=$5,version=$6,content_hash=$7,deleted=$8,projected_at=NULL,updated_at=clock_timestamp()
					WHERE tenant_id=$1 AND domain=$2 AND record_key=$3 AND migration_id=$4`,
					tenantID, domain, record.Key, fence.MigrationID, record.Payload, record.Version, record.Hash, record.Deleted)
				if err != nil {
					return fmt.Errorf("advance projection record %q: %w", record.Key, err)
				}
				updated, rowsErr := result.RowsAffected()
				if rowsErr != nil || updated != 1 {
					return fmt.Errorf("%w: advance projection record %q", datamigration.ErrMigrationConflict, record.Key)
				}
				shouldApply = true
			}
		}
		if !shouldApply {
			continue
		}
		if err := t.projector.Apply(ctx, tenantID, record); err != nil {
			return fmt.Errorf("apply %s projection %q: %w", domain, record.Key, err)
		}
		result, err = tx.ExecContext(ctx, `UPDATE data_migration_records SET projected_at=clock_timestamp(),updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND domain=$2 AND record_key=$3 AND migration_id=$4 AND version=$5 AND content_hash=$6 AND deleted=$7 AND projected_at IS NULL`,
			tenantID, domain, record.Key, fence.MigrationID, record.Version, record.Hash, record.Deleted)
		if err != nil {
			return fmt.Errorf("mark projection record %q: %w", record.Key, err)
		}
		marked, err := result.RowsAffected()
		if err != nil || marked != 1 {
			return fmt.Errorf("%w: projection marker for %q", datamigration.ErrMigrationConflict, record.Key)
		}
	}
	if err := recheckProjectionFence(ctx, tx, tenantID, domain, fence); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit projection target: %w", err)
	}
	return nil
}

func checkProjectionFence(ctx context.Context, tx *sql.Tx, tenantID string, domain datamigration.Domain, fence datamigration.LeaseFence) error {
	var marker int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM data_migrations
		WHERE id=$1 AND lease_owner=$2 AND lease_version=$3
		  AND tenant_id=$4 AND domain=$5
		  AND phase IN ('SNAPSHOT_COPY','CATCH_UP')
		  AND lease_until > clock_timestamp()`,
		fence.MigrationID, fence.Owner, fence.Version, tenantID, domain).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return datamigration.ErrMigrationFence
	}
	if err != nil {
		return fmt.Errorf("check projection fence: %w", err)
	}
	return nil
}

func recheckProjectionFence(ctx context.Context, tx *sql.Tx, tenantID string, domain datamigration.Domain, fence datamigration.LeaseFence) error {
	result, err := tx.ExecContext(ctx, `UPDATE data_migrations SET updated_at=clock_timestamp()
		WHERE id=$1 AND lease_owner=$2 AND lease_version=$3
		  AND tenant_id=$4 AND domain=$5
		  AND phase IN ('SNAPSHOT_COPY','CATCH_UP')
		  AND lease_until > clock_timestamp()`,
		fence.MigrationID, fence.Owner, fence.Version, tenantID, domain)
	if err != nil {
		return fmt.Errorf("recheck projection fence: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return datamigration.ErrMigrationFence
	}
	return nil
}

func projectionDomain(domain datamigration.Domain) bool {
	return domain == datamigration.DomainSession || domain == datamigration.DomainKnowledge || domain == datamigration.DomainArtifact
}

var _ datamigration.Target = (*Target)(nil)
