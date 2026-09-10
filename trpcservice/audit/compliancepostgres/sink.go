// Package compliancepostgres exports immutable Audit facts to an independent
// PostgreSQL compliance database.
package compliancepostgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Sink struct{ DB *sql.DB }

func (s Sink) Emit(ctx context.Context, event audit.Event) error {
	if s.DB == nil {
		return runtime.ErrInvariantViolation
	}
	digest, err := audit.Digest(event)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO compliance.audit_event(
tenant_id,audit_id,schema_version,event_json,event_digest,occurred_at) VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT (tenant_id,audit_id) DO NOTHING`, event.TenantID, event.AuditID, event.SchemaVersion, encoded, digest, event.OccurredAt.UTC())
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		if err := verifyDigest(ctx, tx, "compliance.audit_event", event.TenantID, event.AuditID, digest); err != nil {
			return err
		}
	}
	if event.Action == "artifact.quarantine" {
		resourceKind, artifactID, version, ref, parseErr := quarantineCoordinate(event)
		if parseErr != nil {
			return parseErr
		}
		result, err = tx.ExecContext(ctx, `INSERT INTO compliance.quarantine_alert(
tenant_id,audit_id,resource_kind,artifact_id,resource_version,request_id,error_type,resource_ref,event_digest,occurred_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (tenant_id,audit_id) DO NOTHING`,
			event.TenantID, event.AuditID, resourceKind, artifactID, version, event.RequestID, event.ErrorType, ref, digest, event.OccurredAt.UTC())
		if err != nil {
			return err
		}
		inserted, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			if err := verifyDigest(ctx, tx, "compliance.quarantine_alert", event.TenantID, event.AuditID, digest); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func verifyDigest(ctx context.Context, tx *sql.Tx, table, tenantID, auditID, want string) error {
	query := "SELECT event_digest FROM " + table + " WHERE tenant_id=$1 AND audit_id=$2" // table is a closed internal constant.
	var stored string
	if err := tx.QueryRowContext(ctx, query, tenantID, auditID).Scan(&stored); err != nil {
		return err
	}
	if stored != want {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func quarantineCoordinate(event audit.Event) (string, string, int64, string, error) {
	if event.Decision != "alert" || event.ErrorType == "" || len(event.ResourceRefs) != 1 {
		return "", "", 0, "", runtime.ErrInvalidEnvelope
	}
	const prefix = "artifact-quarantine://"
	ref := event.ResourceRefs[0]
	if !strings.HasPrefix(ref, prefix) {
		return "", "", 0, "", runtime.ErrInvalidEnvelope
	}
	parts := strings.Split(strings.TrimPrefix(ref, prefix), "/")
	if len(parts) != 4 || parts[0] != event.TenantID || (parts[1] != "upload" && parts[1] != "retention") || parts[2] == "" {
		return "", "", 0, "", runtime.ErrTenantScope
	}
	version, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || version < 1 {
		return "", "", 0, "", runtime.ErrVersionMismatch
	}
	return parts[1], parts[2], version, ref, nil
}

var _ audit.Sink = Sink{}
