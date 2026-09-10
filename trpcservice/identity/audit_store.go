package identity

import (
	"context"
	"errors"
	"fmt"
)

// RecordAudit stores login-domain events in the shared audit table. The
// platform-level tenant marker is deliberate because these events precede any
// tenant-scoped runtime execution.
func (s *PostgresIdentityStore) RecordAudit(ctx context.Context, action, result, detail string) error {
	if action == "" || result == "" {
		return errors.New("audit action and result are required")
	}
	eventID, err := newSessionID()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, `
INSERT INTO audit_events (id, tenant_id, trace_id, action, result, redacted_detail, created_at)
VALUES ($1, '_platform', $1, $2, $3, $4, NOW())`,
		eventID, action, result, detail)
	if err != nil {
		return fmt.Errorf("record identity audit: %w", err)
	}
	return nil
}
