package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// SetChannelBindingStatus is the only administrative lifecycle mutation for a
// binding. The revision trigger makes every enable/suspend visible to active
// adapters and admission revalidation.
func (s *Store) SetChannelBindingStatus(
	ctx context.Context,
	tenantID, appID, bindingID string,
	status channels.BindingStatus,
) (channels.Binding, error) {
	if err := s.validate(); err != nil {
		return channels.Binding{}, err
	}
	if tenantID == "" || appID == "" || bindingID == "" {
		return channels.Binding{}, errors.New("tenant_id, app_id, and binding_id are required")
	}
	if status != channels.BindingActive && status != channels.BindingSuspended {
		return channels.Binding{}, errors.New("channel binding status is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return channels.Binding{}, fmt.Errorf("begin channel binding status: %w", err)
	}
	defer func() { rollback(tx) }()
	binding, err := lockChannelBinding(ctx, tx, tenantID, appID, bindingID)
	if err != nil {
		return channels.Binding{}, err
	}
	if status == channels.BindingActive {
		if err := binding.Channel.ValidateProvisionable(); err != nil {
			return channels.Binding{}, err
		}
	}
	if binding.Status == status {
		if err := tx.Commit(ctx); err != nil {
			return channels.Binding{}, fmt.Errorf("commit unchanged channel binding status: %w", err)
		}
		return binding, nil
	}
	connectionStatus := channels.ConnectionNotReady
	if _, err := tx.Exec(ctx, `
UPDATE platform.channel_binding
SET status = $4, connection_status = $5, last_error = CASE WHEN $4 = 'SUSPENDED' THEN 'binding suspended' ELSE '' END
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`, tenantID, appID, bindingID, status, connectionStatus); err != nil {
		return channels.Binding{}, fmt.Errorf("update channel binding status: %w", err)
	}
	updated, err := scanChannelBinding(tx.QueryRow(ctx, channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`, tenantID, appID, bindingID))
	if err != nil {
		return channels.Binding{}, fmt.Errorf("read updated channel binding: %w", err)
	}
	eventType := platformaudit.ChannelEnabled
	if status == channels.BindingSuspended {
		eventType = platformaudit.ChannelSuspended
	}
	if err := recordControlPlaneAuditTx(ctx, tx, controlPlaneAuditEvent(
		ctx, tenantID, appID, "admin", eventType, string(status),
	)); err != nil {
		return channels.Binding{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return channels.Binding{}, fmt.Errorf("commit channel binding status: %w", err)
	}
	return updated, nil
}

// RecordChannelConnection persists only the latest provider connection
// observation. Error text is sanitized before it crosses the storage boundary.
func (s *Store) RecordChannelConnection(
	ctx context.Context,
	tenantID, appID, bindingID string,
	status channels.ConnectionStatus,
	cause error,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if tenantID == "" || appID == "" || bindingID == "" {
		return errors.New("tenant_id, app_id, and binding_id are required")
	}
	if status != channels.ConnectionReady && status != channels.ConnectionNotReady && status != channels.ConnectionDegraded {
		return errors.New("connection status is invalid")
	}
	lastError := ""
	if cause != nil {
		lastError = platformlog.SafeError(cause)
	}
	if _, err := s.pool.Exec(ctx, `
UPDATE platform.channel_binding
SET connection_status = $4,
    last_connected_at = CASE WHEN $4 = 'READY' THEN clock_timestamp() ELSE last_connected_at END,
    last_error = $5
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`, tenantID, appID, bindingID, status, lastError); err != nil {
		return fmt.Errorf("record channel connection: %w", err)
	}
	return nil
}
