package storage

import (
	"context"
	"fmt"
	"strings"
)

// Renew extends a PostgreSQL processing claim only for its trace owner.
func (s *PostgresExecutionDedupStore) Renew(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	if err := validateRenewArguments(tenantID, channel, bindingID, messageID, traceID); err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, `
UPDATE messages SET updated_at = NOW()
WHERE tenant_id = $1 AND channel_type = $2 AND binding_id = $3 AND message_id = $4
  AND status = 'processing' AND trace_id = $5`, tenantID, channel, bindingID, messageID, traceID)
	if err != nil {
		return fmt.Errorf("renew execution claim: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read renewed execution claim: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("execution claim is no longer owned by trace")
	}
	return nil
}

// Renew extends an in-memory claim only for its trace owner.
func (s *MemoryExecutionDedupStore) Renew(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRenewArguments(tenantID, channel, bindingID, messageID, traceID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := memoryClaimKey(tenantID, channel, bindingID, messageID)
	claim, ok := s.claims[key]
	if !ok || claim.status != "processing" || claim.traceID != traceID {
		return fmt.Errorf("execution claim is no longer owned by trace")
	}
	claim.updatedAt = s.now().UTC()
	s.claims[key] = claim
	return nil
}

func validateRenewArguments(tenantID, channel, bindingID, messageID, traceID string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(messageID) == "" || strings.TrimSpace(traceID) == "" {
		return fmt.Errorf("renew execution claim requires tenant, channel, binding, message, and trace IDs")
	}
	return nil
}
