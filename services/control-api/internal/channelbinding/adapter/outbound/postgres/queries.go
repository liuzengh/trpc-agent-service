package postgresadapter

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

func publicAccount(a application.Aggregate) application.AccountView {
	v := application.AccountView{Account: a.Account, Credentials: domain.PublicCredentialStatus(a.CredentialMetadata())}
	v.ScopeID = ""
	return v
}
func distribution(ctx context.Context, tx pgx.Tx, a application.Aggregate) (string, string, error) {
	if a.Route.Projection == nil {
		return "", "NOT_EMITTED", nil
	}
	id := a.Route.Projection.EventID
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM control_outbox WHERE tenant_id=$1 AND id=$2 AND event_type=$3 AND aggregate_id=$4 AND aggregate_revision=$5`, a.Account.TenantID, id, domain.RouteEventType, a.Account.ID, a.Route.Generation).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", integrity()
	}
	if err != nil {
		return "", "", dbError(err)
	}
	switch status {
	case "PENDING", "IN_FLIGHT", "PUBLISHED", "FAILED":
	default:
		return "", "", integrity()
	}
	return id, status, nil
}
func (s *Store) ReadAccount(ctx context.Context, tenant, id string) (application.AccountDetails, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return application.AccountDetails{}, dbError(err)
	}
	defer rollback(tx)
	a, err := loadAggregate(ctx, tx, tenant, id, s.options.ScopeID, false, false)
	if err != nil {
		return application.AccountDetails{}, err
	}
	event, status, err := distribution(ctx, tx, a)
	if err != nil {
		return application.AccountDetails{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return application.AccountDetails{}, dbError(err)
	}
	rows, err := tx.Query(ctx, `SELECT connection_revision,instance_id,instance_epoch::text,report_sequence,state,reason_code,observed_at,owner_epoch,received_at,COALESCE(receive_mode,'') FROM channel_account_observations WHERE tenant_id=$1 AND account_id=$2 AND received_at>=clock_timestamp()-interval '24 hours' ORDER BY received_at DESC,instance_id,instance_epoch LIMIT 32`, tenant, id)
	if err != nil {
		return application.AccountDetails{}, dbError(err)
	}
	observations := make([]application.ObservationView, 0)
	for rows.Next() {
		var v application.ObservationView
		if err = rows.Scan(&v.ConnectionRevision, &v.InstanceID, &v.InstanceEpoch, &v.ReportSequence, &v.State, &v.ReasonCode, &v.ObservedAt, &v.OwnerEpoch, &v.ReceivedAt, &v.ReceiveMode); err != nil {
			rows.Close()
			return application.AccountDetails{}, dbError(err)
		}
		v.EffectiveState = v.State
		if now.Sub(v.ReceivedAt) >= 90*time.Second || v.ConnectionRevision != a.Account.ConnectionRevision {
			v.EffectiveState = "STALE"
		}
		observations = append(observations, v)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return application.AccountDetails{}, dbError(err)
	}
	result := application.AccountDetails{Account: publicAccount(a), Binding: a.Binding, RouteGeneration: a.Route.Generation, EventID: event, Distribution: status, GatewayApplication: "UNKNOWN", Observations: observations}
	if err = tx.Commit(ctx); err != nil {
		return result, dbError(err)
	}
	return result, nil
}
func (s *Store) ReadBinding(ctx context.Context, tenant, id string) (application.BindingDetails, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return application.BindingDetails{}, dbError(err)
	}
	defer rollback(tx)
	var account string
	err = tx.QueryRow(ctx, `SELECT account_id FROM channel_bindings WHERE tenant_id=$1 AND id=$2`, tenant, id).Scan(&account)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.BindingDetails{}, application.ErrBindingNotFound
	}
	if err != nil {
		return application.BindingDetails{}, dbError(err)
	}
	a, err := loadAggregate(ctx, tx, tenant, account, s.options.ScopeID, false, false)
	if err != nil {
		return application.BindingDetails{}, err
	}
	if a.Binding == nil || a.Binding.ID != id {
		return application.BindingDetails{}, integrity()
	}
	event, status, err := distribution(ctx, tx, a)
	if err != nil {
		return application.BindingDetails{}, err
	}
	result := application.BindingDetails{Binding: *a.Binding, RouteGeneration: a.Route.Generation, EventID: event, Distribution: status, GatewayApplication: "UNKNOWN"}
	if err = tx.Commit(ctx); err != nil {
		return result, dbError(err)
	}
	return result, nil
}
func (s *Store) pageIDs(ctx context.Context, tx pgx.Tx, tenant string, page application.Page, binding bool) ([]string, error) {
	var err error
	page, err = application.NormalizePage(page)
	if err != nil {
		return nil, err
	}
	query := `SELECT id FROM channel_accounts WHERE tenant_id=$1 AND scope_id=$2 AND id>$3 ORDER BY id LIMIT $4`
	if binding {
		query = `SELECT b.id FROM channel_bindings b JOIN channel_accounts a ON a.tenant_id=b.tenant_id AND a.id=b.account_id WHERE b.tenant_id=$1 AND a.scope_id=$2 AND b.id>$3 ORDER BY b.id LIMIT $4`
	}
	rows, err := tx.Query(ctx, query, tenant, s.options.ScopeID, page.After, page.Limit+1)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, dbError(err)
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return nil, dbError(err)
	}
	return ids, nil
}
func (s *Store) ListAccounts(ctx context.Context, tenant string, page application.Page) (application.AccountPage, error) {
	page, err := application.NormalizePage(page)
	if err != nil {
		return application.AccountPage{}, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return application.AccountPage{}, dbError(err)
	}
	defer rollback(tx)
	ids, err := s.pageIDs(ctx, tx, tenant, page, false)
	if err != nil {
		return application.AccountPage{}, err
	}
	result := application.AccountPage{Accounts: []application.AccountView{}}
	if len(ids) > page.Limit {
		ids = ids[:page.Limit]
		result.NextCursor = ids[len(ids)-1]
	}
	for _, id := range ids {
		a, err := loadAggregate(ctx, tx, tenant, id, s.options.ScopeID, false, false)
		if err != nil {
			return result, err
		}
		result.Accounts = append(result.Accounts, publicAccount(a))
	}
	if err = tx.Commit(ctx); err != nil {
		return result, dbError(err)
	}
	return result, nil
}
func (s *Store) ListBindings(ctx context.Context, tenant string, page application.Page) (application.BindingPage, error) {
	page, err := application.NormalizePage(page)
	if err != nil {
		return application.BindingPage{}, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return application.BindingPage{}, dbError(err)
	}
	defer rollback(tx)
	ids, err := s.pageIDs(ctx, tx, tenant, page, true)
	if err != nil {
		return application.BindingPage{}, err
	}
	result := application.BindingPage{Bindings: []domain.Binding{}}
	if len(ids) > page.Limit {
		ids = ids[:page.Limit]
		result.NextCursor = ids[len(ids)-1]
	}
	for _, id := range ids {
		b, err := scanBinding(tx.QueryRow(ctx, `SELECT tenant_id,id,account_id,binding_revision,enabled,deployment_id,revision_number,deployment_revision_id,manifest_ref,manifest_digest,traffic_policy_jsonb,created_by,created_at,updated_at FROM channel_bindings WHERE tenant_id=$1 AND id=$2`, tenant, id))
		if err != nil {
			return result, err
		}
		result.Bindings = append(result.Bindings, b)
	}
	if err = tx.Commit(ctx); err != nil {
		return result, dbError(err)
	}
	return result, nil
}

var _ application.ReadStore = (*Store)(nil)
