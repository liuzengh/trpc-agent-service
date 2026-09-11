package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

const accountColumns = `tenant_id,id,scope_id,provider,provider_account_id,name,description,account_revision,connection_revision,min_route_generation,enabled,config_jsonb,created_by,created_at,updated_at`

func scanAccount(row pgx.Row) (domain.Account, error) {
	var a domain.Account
	var config []byte
	err := row.Scan(&a.TenantID, &a.ID, &a.ScopeID, &a.Provider, &a.ProviderAccountID, &a.Name, &a.Description, &a.Revision, &a.ConnectionRevision, &a.MinRouteGeneration, &a.Enabled, &config, &a.CreatedBy, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, application.ErrAccountNotFound
	}
	if err != nil {
		return a, dbError(err)
	}
	if err = json.Unmarshal(config, &a.Config); err != nil {
		return a, integrity()
	}
	raw, _, err := domain.CanonicalJSON(a.Config)
	if err != nil {
		return a, err
	}
	var canonical any
	if json.Unmarshal(config, &canonical) != nil {
		return a, integrity()
	}
	original, _, err := domain.CanonicalJSON(canonical)
	if err != nil || string(raw) != string(original) {
		return a, integrity()
	}
	if err = a.Validate(); err != nil {
		return a, err
	}
	return a, nil
}
func loadAggregate(ctx context.Context, tx pgx.Tx, tenant, id, scope string, locked, includeCipher bool) (application.Aggregate, error) {
	suffix := ""
	if locked {
		suffix = " FOR UPDATE"
	}
	a, err := scanAccount(tx.QueryRow(ctx, `SELECT `+accountColumns+` FROM channel_accounts WHERE tenant_id=$1 AND id=$2 AND scope_id=$3`+suffix, tenant, id, scope))
	if err != nil {
		return application.Aggregate{}, err
	}
	result := application.Aggregate{Account: a}
	var projection []byte
	var digest *string
	err = tx.QueryRow(ctx, `SELECT tenant_id,account_id,generation,projection_jsonb,payload_digest FROM channel_account_route_states WHERE tenant_id=$1 AND account_id=$2`+suffix, tenant, id).Scan(&result.Route.TenantID, &result.Route.AccountID, &result.Route.Generation, &projection, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, integrity()
	}
	if err != nil {
		return result, dbError(err)
	}
	if result.Route.Generation != a.MinRouteGeneration {
		return result, integrity()
	}
	if result.Route.Generation > 0 {
		if digest == nil {
			return result, integrity()
		}
		p, err := domain.ValidateStoredRoute(projection, *digest)
		if err != nil {
			return result, err
		}
		if p.Route.Generation != result.Route.Generation || p.Route.AccountID != a.ID || p.Route.Provider != a.Provider || p.Enabled && p.Route.TenantID != a.TenantID {
			return result, integrity()
		}
		result.Route.Projection = &p
	} else if projection != nil || digest != nil {
		return result, integrity()
	}
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT tenant_id,id,account_id,binding_revision,enabled,deployment_id,revision_number,deployment_revision_id,manifest_ref,manifest_digest,traffic_policy_jsonb,created_by,created_at,updated_at FROM channel_bindings WHERE tenant_id=$1 AND account_id=$2`+suffix, tenant, id))
	if err == nil {
		result.Binding = &binding
	} else if !errors.Is(err, application.ErrBindingNotFound) {
		return result, err
	}
	columns := `tenant_id,account_id,provider,purpose,id,credential_version,configured`
	if includeCipher {
		columns += `,COALESCE(key_id,''),ciphertext`
	}
	rows, err := tx.Query(ctx, `SELECT `+columns+` FROM channel_account_credentials WHERE tenant_id=$1 AND account_id=$2 ORDER BY purpose`, tenant, id)
	if err != nil {
		return result, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var c domain.CredentialRecord
		dest := []any{&c.TenantID, &c.AccountID, &c.Provider, &c.Meta.Purpose, &c.Meta.ID, &c.Meta.Version, &c.Meta.Configured}
		if includeCipher {
			dest = append(dest, &c.KeyID, &c.Ciphertext)
		}
		if err = rows.Scan(dest...); err != nil {
			return result, dbError(err)
		}
		if c.TenantID != tenant || c.AccountID != id || c.Provider != a.Provider || c.Meta.Version > a.ConnectionRevision {
			return result, integrity()
		}
		result.Credentials = append(result.Credentials, c)
	}
	if err = rows.Err(); err != nil {
		return result, dbError(err)
	}
	if err = domain.ValidateCredentialSet(a.Provider, result.CredentialMetadata(), a.Enabled, a.Config.ReceiveMode); err != nil {
		return result, integrity()
	}
	if result.Binding == nil && result.Route.Generation != 0 || result.Binding != nil && result.Route.Generation == 0 {
		return result, integrity()
	}
	if result.Binding != nil {
		if err = result.Binding.Target.Validate(tenant); err != nil {
			return result, err
		}
		if result.Binding.Traffic != nil && result.Binding.Traffic.Validate(tenant, result.Binding.Target) != nil {
			return result, integrity()
		}
		if result.Route.Projection.Enabled != (a.Enabled && result.Binding.Enabled) {
			return result, integrity()
		}
		if result.Route.Projection.Enabled {
			r := result.Route.Projection.Route
			t := result.Binding.Target
			if r.BindingID != result.Binding.ID || r.DeploymentRevisionID != t.DeploymentRevisionID || r.ManifestRef != t.ManifestID || r.ManifestDigest != t.ManifestDigest || !reflect.DeepEqual(r.Traffic, result.Binding.Traffic) {
				return result, integrity()
			}
		}
	}
	return result, nil
}
func scanBinding(row pgx.Row) (domain.Binding, error) {
	var b domain.Binding
	var traffic []byte
	err := row.Scan(&b.TenantID, &b.ID, &b.AccountID, &b.Revision, &b.Enabled, &b.Target.DeploymentID, &b.Target.RevisionNumber, &b.Target.DeploymentRevisionID, &b.Target.ManifestID, &b.Target.ManifestDigest, &traffic, &b.CreatedBy, &b.CreatedAt, &b.UpdatedAt)
	b.Target.TenantID = b.TenantID
	if errors.Is(err, pgx.ErrNoRows) {
		return b, application.ErrBindingNotFound
	}
	if err != nil {
		return b, dbError(err)
	}
	if traffic != nil {
		b.Traffic = &domain.TrafficRollout{}
		if json.Unmarshal(traffic, b.Traffic) != nil {
			return b, integrity()
		}
	}
	if !domain.ValidID(b.ID) || !domain.ValidVersion(b.Revision) || b.Target.Validate(b.TenantID) != nil || b.Traffic != nil && b.Traffic.Validate(b.TenantID, b.Target) != nil {
		return b, integrity()
	}
	return b, nil
}
func (t *writeTx) LoadAccount(ctx context.Context, id string) (application.Aggregate, error) {
	if _, ok := t.loaded[id]; ok {
		return application.Aggregate{}, integrity()
	}
	a, err := loadAggregate(ctx, t.tx, t.scope.Actor.TenantID, id, t.scope.ScopeID, true, true)
	if err != nil {
		return a, err
	}
	t.loaded[id] = cloneAggregate(a)
	return a, nil
}
func (t *writeTx) LoadBinding(ctx context.Context, id string) (application.Aggregate, error) {
	var accountID string
	err := t.tx.QueryRow(ctx, `SELECT account_id FROM channel_bindings WHERE tenant_id=$1 AND id=$2`, t.scope.Actor.TenantID, id).Scan(&accountID)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.Aggregate{}, application.ErrBindingNotFound
	}
	if err != nil {
		return application.Aggregate{}, dbError(err)
	}
	a, err := t.LoadAccount(ctx, accountID)
	if err != nil {
		return a, err
	}
	if a.Binding == nil || a.Binding.ID != id {
		return a, integrity()
	}
	return a, nil
}
func (s *Store) GetAccount(ctx context.Context, tenant, id string) (application.Aggregate, error) {
	return s.readAggregate(ctx, tenant, id, false)
}
func (s *Store) GetBinding(ctx context.Context, tenant, id string) (application.Aggregate, error) {
	return s.readAggregate(ctx, tenant, id, true)
}
func (s *Store) readAggregate(ctx context.Context, tenant, id string, binding bool) (application.Aggregate, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return application.Aggregate{}, dbError(err)
	}
	defer rollback(tx)
	if binding {
		var account string
		err = tx.QueryRow(ctx, `SELECT account_id FROM channel_bindings WHERE tenant_id=$1 AND id=$2`, tenant, id).Scan(&account)
		if errors.Is(err, pgx.ErrNoRows) {
			return application.Aggregate{}, application.ErrBindingNotFound
		}
		if err != nil {
			return application.Aggregate{}, dbError(err)
		}
		id = account
	}
	a, err := loadAggregate(ctx, tx, tenant, id, s.options.ScopeID, false, false)
	if err != nil {
		return a, err
	}
	if err = tx.Commit(ctx); err != nil {
		return a, dbError(err)
	}
	return a, nil
}
