package postgresadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"slices"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

func cloneAggregate(a application.Aggregate) application.Aggregate {
	a.Credentials = slices.Clone(a.Credentials)
	for i := range a.Credentials {
		a.Credentials[i].Ciphertext = bytes.Clone(a.Credentials[i].Ciphertext)
	}
	if a.Binding != nil {
		b := *a.Binding
		if b.Traffic != nil {
			traffic := *b.Traffic
			traffic.CanarySubjects = slices.Clone(traffic.CanarySubjects)
			b.Traffic = &traffic
		}
		a.Binding = &b
	}
	if a.Route.Projection != nil {
		p := *a.Route.Projection
		if p.Route.Traffic != nil {
			traffic := *p.Route.Traffic
			traffic.CanarySubjects = slices.Clone(traffic.CanarySubjects)
			p.Route.Traffic = &traffic
		}
		a.Route.Projection = &p
	}
	return a
}
func validateAggregate(a application.Aggregate) error {
	if err := a.Account.Validate(); err != nil {
		return err
	}
	if a.Route.TenantID != a.Account.TenantID || a.Route.AccountID != a.Account.ID || a.Route.Generation != a.Account.MinRouteGeneration {
		return integrity()
	}
	if err := domain.ValidateCredentialSet(a.Account.Provider, a.CredentialMetadata(), a.Account.Enabled, a.Account.Config.ReceiveMode); err != nil {
		return err
	}
	for _, c := range a.Credentials {
		if c.TenantID != a.Account.TenantID || c.AccountID != a.Account.ID || c.Provider != a.Account.Provider || c.Meta.Version > a.Account.ConnectionRevision {
			return integrity()
		}
		if c.Meta.Configured {
			if c.KeyID == "" || len(c.Ciphertext) < 30 || len(c.Ciphertext) > 16413 {
				return integrity()
			}
		} else if c.KeyID != "" || c.Ciphertext != nil {
			return integrity()
		}
	}
	if a.Binding != nil {
		b := a.Binding
		if b.TenantID != a.Account.TenantID || b.AccountID != a.Account.ID || !domain.ValidID(b.ID) || !domain.ValidVersion(b.Revision) {
			return integrity()
		}
		if err := b.Target.Validate(a.Account.TenantID); err != nil {
			return err
		}
		if b.Traffic != nil {
			if err := b.Traffic.Validate(a.Account.TenantID, b.Target); err != nil {
				return err
			}
		}
	}
	return nil
}
func validateChange(old, a application.Aggregate, exists bool) error {
	if err := validateAggregate(a); err != nil {
		return err
	}
	if !exists {
		if a.Account.Revision != 1 || a.Account.ConnectionRevision != 1 || a.Account.Enabled || a.Account.MinRouteGeneration != 0 || a.Binding != nil || a.Route.Projection != nil {
			return integrity()
		}
		for _, c := range a.Credentials {
			if c.Meta.Version != 1 || (slices.Contains(domain.RequiredPurposes(a.Account.Provider, a.Account.Config.ReceiveMode), c.Meta.Purpose) && !c.Meta.Configured) {
				return integrity()
			}
		}
		return nil
	}
	o, n := old.Account, a.Account
	if o.ID != n.ID || o.TenantID != n.TenantID || o.ScopeID != n.ScopeID || o.Provider != n.Provider || o.ProviderAccountID != n.ProviderAccountID || o.Config.WebhookPath != n.Config.WebhookPath || o.Config.BotID != n.Config.BotID || o.CreatedBy != n.CreatedBy || !o.CreatedAt.Equal(n.CreatedAt) {
		return integrity()
	}
	modeChanged := o.Config.ReceiveMode != n.Config.ReceiveMode
	if modeChanged && (o.Enabled || n.Enabled) {
		return integrity()
	}
	accountChanged := modeChanged || o.Name != n.Name || o.Description != n.Description || o.Enabled != n.Enabled
	credentialChanged := false
	for _, before := range old.Credentials {
		var after *domain.CredentialRecord
		for i := range a.Credentials {
			if a.Credentials[i].Meta.Purpose == before.Meta.Purpose {
				after = &a.Credentials[i]
				break
			}
		}
		if after == nil || after.Meta.ID != before.Meta.ID {
			return integrity()
		}
		changed := before.Meta != after.Meta || before.KeyID != after.KeyID || !bytes.Equal(before.Ciphertext, after.Ciphertext)
		if changed {
			if before.Meta.Version == domain.MaxVersion || after.Meta.Version != before.Meta.Version+1 {
				return integrity()
			}
			credentialChanged = true
		} else if after.Meta.Version != before.Meta.Version {
			return integrity()
		}
	}
	if accountChanged || credentialChanged {
		if o.Revision == domain.MaxVersion || n.Revision != o.Revision+1 {
			return integrity()
		}
	} else if n.Revision != o.Revision {
		return integrity()
	}
	if modeChanged || o.Enabled != n.Enabled || credentialChanged {
		if o.ConnectionRevision == domain.MaxVersion || n.ConnectionRevision != o.ConnectionRevision+1 {
			return integrity()
		}
	} else if n.ConnectionRevision != o.ConnectionRevision {
		return integrity()
	}
	if old.Binding != nil {
		if a.Binding == nil || old.Binding.ID != a.Binding.ID || old.Binding.TenantID != a.Binding.TenantID || old.Binding.AccountID != a.Binding.AccountID || old.Binding.CreatedBy != a.Binding.CreatedBy || !old.Binding.CreatedAt.Equal(a.Binding.CreatedAt) {
			return integrity()
		}
		changed := old.Binding.Target != a.Binding.Target || old.Binding.Enabled != a.Binding.Enabled || !reflect.DeepEqual(old.Binding.Traffic, a.Binding.Traffic)
		if changed {
			if old.Binding.Revision == domain.MaxVersion || a.Binding.Revision != old.Binding.Revision+1 {
				return integrity()
			}
		} else if a.Binding.Revision != old.Binding.Revision {
			return integrity()
		}
	} else if a.Binding != nil && (a.Binding.Revision != 1 || a.Binding.Enabled) {
		return integrity()
	}
	candidate := a.Account
	candidate.MinRouteGeneration = old.Route.Generation
	eventID := "evt_unused"
	if a.Route.Projection != nil {
		eventID = a.Route.Projection.EventID
	}
	expected, _, err := domain.AdvanceRoute(old.Route, candidate, a.Binding, eventID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, a.Route) {
		return integrity()
	}
	return nil
}
func (t *writeTx) Save(ctx context.Context, a application.Aggregate) error {
	if t.saved {
		return integrity()
	}
	if a.Account.ScopeID != t.scope.ScopeID || a.Account.TenantID != t.scope.Actor.TenantID {
		return application.ErrPermissionDenied
	}
	old, exists := t.loaded[a.Account.ID]
	if err := validateChange(old, a, exists); err != nil {
		return err
	}
	if !exists {
		if t.catalog.count >= domain.MaxAccounts {
			return &domain.Error{Code: "CHANNEL_LIMIT_EXCEEDED", Field: "/accounts"}
		}
		var count int
		if err := t.tx.QueryRow(ctx, `SELECT count(*) FROM channel_accounts WHERE tenant_id=$1 AND scope_id=$2`, a.Account.TenantID, a.Account.ScopeID).Scan(&count); err != nil {
			return dbError(err)
		}
		if count >= t.store.options.MaxTenantAccounts {
			return &domain.Error{Code: "CHANNEL_LIMIT_EXCEEDED", Field: "/accounts"}
		}
	}
	config, err := json.Marshal(a.Account.Config)
	if err != nil {
		return integrity()
	}
	values := []any{a.Account.TenantID, a.Account.ID, a.Account.ScopeID, a.Account.Provider, a.Account.ProviderAccountID, a.Account.Name, a.Account.Description, a.Account.Revision, a.Account.ConnectionRevision, a.Account.MinRouteGeneration, a.Account.Enabled, config, a.Account.CreatedBy, a.Account.CreatedAt, a.Account.UpdatedAt}
	if !exists {
		_, err = t.tx.Exec(ctx, `INSERT INTO channel_accounts(`+accountColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, values...)
	} else {
		var tag interface{ RowsAffected() int64 }
		tag, err = t.tx.Exec(ctx, `UPDATE channel_accounts SET name=$6,description=$7,account_revision=$8,connection_revision=$9,min_route_generation=$10,enabled=$11,config_jsonb=$12,updated_at=$15 WHERE tenant_id=$1 AND id=$2 AND scope_id=$3 AND provider=$4 AND provider_account_id=$5 AND created_by=$13 AND created_at=$14`, values...)
		if err == nil && tag.RowsAffected() != 1 {
			return integrity()
		}
	}
	if err != nil {
		return dbError(err)
	}
	for _, c := range a.Credentials {
		var key any
		if c.Meta.Configured {
			key = c.KeyID
		}
		tag, err := t.tx.Exec(ctx, `INSERT INTO channel_account_credentials(tenant_id,account_id,provider,purpose,id,credential_version,configured,key_id,ciphertext) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(tenant_id,account_id,purpose) DO UPDATE SET credential_version=EXCLUDED.credential_version,configured=EXCLUDED.configured,key_id=EXCLUDED.key_id,ciphertext=EXCLUDED.ciphertext WHERE channel_account_credentials.id=EXCLUDED.id`, c.TenantID, c.AccountID, c.Provider, c.Meta.Purpose, c.Meta.ID, c.Meta.Version, c.Meta.Configured, key, c.Ciphertext)
		if err == nil && tag.RowsAffected() != 1 {
			return integrity()
		}
		if err != nil {
			return dbError(err)
		}
	}
	if a.Binding != nil {
		b := a.Binding
		var traffic any
		if b.Traffic != nil {
			traffic, err = json.Marshal(b.Traffic)
			if err != nil {
				return integrity()
			}
		}
		_, err = t.tx.Exec(ctx, `INSERT INTO channel_bindings(tenant_id,id,account_id,binding_revision,enabled,deployment_id,revision_number,deployment_revision_id,manifest_ref,manifest_digest,traffic_policy_jsonb,created_by,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(tenant_id,id) DO UPDATE SET binding_revision=EXCLUDED.binding_revision,enabled=EXCLUDED.enabled,deployment_id=EXCLUDED.deployment_id,revision_number=EXCLUDED.revision_number,deployment_revision_id=EXCLUDED.deployment_revision_id,manifest_ref=EXCLUDED.manifest_ref,manifest_digest=EXCLUDED.manifest_digest,traffic_policy_jsonb=EXCLUDED.traffic_policy_jsonb,updated_at=EXCLUDED.updated_at`, b.TenantID, b.ID, b.AccountID, b.Revision, b.Enabled, b.Target.DeploymentID, b.Target.RevisionNumber, b.Target.DeploymentRevisionID, b.Target.ManifestID, b.Target.ManifestDigest, traffic, b.CreatedBy, b.CreatedAt, b.UpdatedAt)
		if err != nil {
			return dbError(err)
		}
	}
	var projection any
	var digest any
	if a.Route.Projection != nil {
		raw, d, err := a.Route.Projection.Encode()
		if err != nil {
			return err
		}
		projection = []byte(raw)
		digest = d
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO channel_account_route_states(tenant_id,account_id,generation,projection_jsonb,payload_digest) VALUES($1,$2,$3,$4,$5) ON CONFLICT(tenant_id,account_id) DO UPDATE SET generation=EXCLUDED.generation,projection_jsonb=EXCLUDED.projection_jsonb,payload_digest=EXCLUDED.payload_digest`, a.Account.TenantID, a.Account.ID, a.Route.Generation, projection, digest)
	if err != nil {
		return dbError(err)
	}
	if a.Route.Generation > old.Route.Generation {
		p := a.Route.Projection
		_, err = t.tx.Exec(ctx, `INSERT INTO control_outbox(tenant_id,id,aggregate_type,aggregate_id,aggregate_revision,event_type,schema_version,payload_jsonb,payload_digest,status,available_at,created_at,updated_at) VALUES($1,$2,'ChannelAccountRoute',$3,$4,$5,'1',$6,$7,'PENDING',clock_timestamp(),clock_timestamp(),clock_timestamp())`, a.Account.TenantID, p.EventID, a.Account.ID, a.Route.Generation, domain.RouteEventType, projection, digest)
		if err != nil {
			return dbError(err)
		}
	}
	t.saved = true
	t.snapshotChanged = !exists || old.Account != a.Account
	if !exists {
		t.createdCount = 1
	}
	return nil
}
