// Package postgres resolves channel send credential metadata from an exact
// immutable ConfigSnapshot version.
package postgres

import (
	"context"
	"database/sql"
	"errors"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/credentials"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Locator struct{ db *sql.DB }

func New(db *sql.DB) *Locator { return &Locator{db: db} }

func (l *Locator) ResolveSendBinding(ctx context.Context, destination channel.ReplyDestination) (credentials.Binding, error) {
	if l == nil || l.db == nil || destination.TenantID == "" || destination.Channel == "" || destination.ChannelBindingID == "" ||
		destination.ExternalAccountID == "" || destination.ConfigVersion < 1 {
		return credentials.Binding{}, runtime.ErrTenantScope
	}
	var result credentials.Binding
	err := l.db.QueryRowContext(ctx, `SELECT tenant_id,config_version,binding_id,channel,external_account_id,send_secret_ref,send_secret_version
FROM channel_binding WHERE tenant_id=$1 AND config_version=$2 AND binding_id=$3
  AND send_secret_ref IS NOT NULL AND send_secret_version IS NOT NULL`,
		destination.TenantID, destination.ConfigVersion, destination.ChannelBindingID).Scan(&result.TenantID, &result.ConfigVersion,
		&result.ChannelBindingID, &result.Channel, &result.ExternalAccountID, &result.SecretRef.Ref, &result.SecretRef.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return credentials.Binding{}, runtime.ErrNotFound
	}
	if err != nil {
		if ctx.Err() != nil {
			return credentials.Binding{}, ctx.Err()
		}
		return credentials.Binding{}, runtime.ErrBackendUnavailable
	}
	if result.Channel != destination.Channel || result.ExternalAccountID != destination.ExternalAccountID {
		return credentials.Binding{}, runtime.ErrTenantScope
	}
	return result, nil
}

var _ credentials.Locator = (*Locator)(nil)
