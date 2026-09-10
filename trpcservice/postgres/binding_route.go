package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// ResolveBindingByPublicRoute locates one Binding by its opaque public route.
// The route is only a locator; callers must still validate channel and status
// before using the returned snapshot as a trusted source.
func (s *Store) ResolveBindingByPublicRoute(
	ctx context.Context,
	channel channels.Channel,
	publicRouteID string,
) (channels.BindingSnapshot, error) {
	if err := s.validate(); err != nil {
		return channels.BindingSnapshot{}, err
	}
	if err := channel.Validate(); err != nil {
		return channels.BindingSnapshot{}, err
	}
	if err := channels.ValidatePublicRouteID(publicRouteID); err != nil {
		return channels.BindingSnapshot{}, err
	}
	binding, err := scanChannelBinding(s.pool.QueryRow(ctx, channelBindingSelect+`
WHERE public_route_id = $1`, publicRouteID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return channels.BindingSnapshot{}, fmt.Errorf("%w: %w", channels.ErrBindingNotFound, ErrNotFound)
		}
		return channels.BindingSnapshot{}, fmt.Errorf("resolve channel binding by public route: %w", err)
	}
	if binding.Channel != channel {
		return channels.BindingSnapshot{}, channels.ErrBindingChannelMismatch
	}
	return binding.Snapshot(), nil
}

// ListActiveChannelBindings returns the authoritative active bindings used to
// construct one provider client per tenant/application/binding scope.
func (s *Store) ListActiveChannelBindings(
	ctx context.Context,
	channel channels.Channel,
) ([]channels.Binding, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := channel.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, channelBindingSelect+`
WHERE channel = $1 AND status = $2
ORDER BY tenant_id, app_id, binding_id`, channel, channels.BindingActive)
	if err != nil {
		return nil, fmt.Errorf("list active channel bindings: %w", err)
	}
	defer rows.Close()
	bindings := make([]channels.Binding, 0)
	for rows.Next() {
		binding, err := scanChannelBinding(rows)
		if err != nil {
			return nil, fmt.Errorf("scan active channel binding: %w", err)
		}
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("stored active channel binding: %w", err)
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active channel bindings: %w", err)
	}
	slices.SortFunc(bindings, func(left, right channels.Binding) int {
		if left.TenantID != right.TenantID {
			return strings.Compare(left.TenantID, right.TenantID)
		}
		if left.AppID != right.AppID {
			return strings.Compare(left.AppID, right.AppID)
		}
		return strings.Compare(left.BindingID, right.BindingID)
	})
	return bindings, nil
}

// RotateChannelBindingRoute replaces a binding's public route. The database
// trigger advances binding_revision atomically with the route update, and the
// old route is invalid immediately after commit.
func (s *Store) RotateChannelBindingRoute(
	ctx context.Context,
	tenantID, appID, bindingID string,
) (channels.Binding, error) {
	if err := s.validate(); err != nil {
		return channels.Binding{}, err
	}
	if tenantID == "" {
		return channels.Binding{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return channels.Binding{}, errors.New("app_id is required")
	}
	if bindingID == "" {
		return channels.Binding{}, errors.New("binding_id is required")
	}
	publicRouteID, err := channels.NewPublicRouteID()
	if err != nil {
		return channels.Binding{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return channels.Binding{}, fmt.Errorf("begin channel binding route rotation: %w", err)
	}
	defer func() {
		rollback(tx)
	}()

	if _, err := scanChannelBinding(tx.QueryRow(ctx, channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
FOR UPDATE`, tenantID, appID, bindingID)); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return channels.Binding{}, fmt.Errorf("channel binding: %w", ErrNotFound)
		}
		return channels.Binding{}, fmt.Errorf("lock channel binding for route rotation: %w", err)
	}
	if _, err := tx.Exec(
		ctx,
		`UPDATE platform.channel_binding
SET public_route_id = $4
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		tenantID,
		appID,
		bindingID,
		publicRouteID,
	); err != nil {
		return channels.Binding{}, fmt.Errorf("rotate channel binding route: %w", err)
	}
	rotated, err := scanChannelBinding(tx.QueryRow(ctx, channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`, tenantID, appID, bindingID))
	if err != nil {
		return channels.Binding{}, fmt.Errorf("read rotated channel binding: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return channels.Binding{}, fmt.Errorf("commit channel binding route rotation: %w", err)
	}
	return rotated, nil
}

func lockChannelBinding(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID string,
) (channels.Binding, error) {
	binding, err := scanChannelBinding(tx.QueryRow(ctx, channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
FOR UPDATE`, tenantID, appID, bindingID))
	if err != nil {
		return channels.Binding{}, resolveError("channel binding", err)
	}
	return binding, nil
}

const channelBindingSelect = `SELECT
    tenant_id,
    app_id,
    binding_id,
    channel,
    external_account,
    secret_ref,
    public_route_id,
    binding_revision,
    status,
    connection_status,
    last_connected_at,
    last_error
FROM platform.channel_binding`

type channelBindingRow interface {
	Scan(dest ...any) error
}

func scanChannelBinding(row channelBindingRow) (channels.Binding, error) {
	var binding channels.Binding
	var secret []byte
	var publicRouteID pgtype.Text
	var lastConnectedAt *time.Time
	var lastError string
	err := row.Scan(
		&binding.TenantID,
		&binding.AppID,
		&binding.BindingID,
		&binding.Channel,
		&binding.ExternalAccount,
		&secret,
		&publicRouteID,
		&binding.BindingRevision,
		&binding.Status,
		&binding.ConnectionStatus,
		&lastConnectedAt,
		&lastError,
	)
	if err != nil {
		return channels.Binding{}, err
	}
	if err := unmarshalBindingSecretRef(&binding, secret); err != nil {
		return channels.Binding{}, err
	}
	if publicRouteID.Valid {
		binding.PublicRouteID = publicRouteID.String
	}
	binding.LastConnectedAt = lastConnectedAt
	if lastError != "" {
		binding.LastError = platformlog.SafeError(errors.New(lastError))
	}
	return binding, nil
}
