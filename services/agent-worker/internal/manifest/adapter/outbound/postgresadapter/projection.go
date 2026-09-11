package postgresadapter

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	values "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
)

type Projection struct{ pool *pgxpool.Pool }

var _ application.Projection = (*Projection)(nil)

func New(pool *pgxpool.Pool) *Projection { return &Projection{pool: pool} }
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func (p *Projection) Apply(ctx context.Context, m domain.Publication, capacity int) error {
	if m.EventID == "" || m.TenantID == "" || m.ManifestID == "" || m.DeploymentRevisionID == "" || !values.DigestValid(m.ContentDigest) || !values.DigestValid(m.EventDigest) || values.Digest(m.Envelope) != m.EnvelopeDigest || capacity < 1 {
		return values.ErrInvalid
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	// The bounded apply transaction serializes immutable identity/capacity checks.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(731004287)`); err != nil {
		return err
	}
	var oldDigest, oldTenant, oldID string
	err = tx.QueryRow(ctx, `SELECT event_digest,tenant_id,manifest_id FROM manifest_receipts WHERE event_id=$1`, m.EventID).Scan(&oldDigest, &oldTenant, &oldID)
	conflict := false
	if err == nil {
		if oldDigest == m.EventDigest {
			return tx.Commit(ctx)
		}
		conflict = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	// Read every implicated identity. A crossed publication (X/revision-Y)
	// must invalidate both X and Y, not whichever row happens to sort first.
	type identity struct{ tenant, id, revision string }
	identities := []identity{{m.TenantID, m.ManifestID, m.DeploymentRevisionID}}
	rows, err := tx.Query(ctx, `SELECT tenant_id,manifest_id,deployment_revision_id,envelope_digest,conflicted FROM runtime_manifests WHERE manifest_id=$1 OR (tenant_id=$2 AND deployment_revision_id=$3) OR (tenant_id=$4 AND manifest_id=$5)`, m.ManifestID, m.TenantID, m.DeploymentRevisionID, oldTenant, oldID)
	if err != nil {
		return err
	}
	exists := false
	for rows.Next() {
		var previous identity
		var digest string
		var poisoned bool
		if err = rows.Scan(&previous.tenant, &previous.id, &previous.revision, &digest, &poisoned); err != nil {
			rows.Close()
			return err
		}
		identities = append(identities, previous)
		if previous.tenant == m.TenantID && previous.id == m.ManifestID && previous.revision == m.DeploymentRevisionID && digest == m.EnvelopeDigest {
			exists = true
		} else {
			conflict = true
		}
		conflict = conflict || poisoned
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	var poisoned bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manifest_conflict_identities WHERE (identity_kind='MANIFEST' AND tenant_id='' AND identity_value=$1) OR (identity_kind='REVISION' AND tenant_id=$2 AND identity_value=$3))`, m.ManifestID, m.TenantID, m.DeploymentRevisionID).Scan(&poisoned); err != nil {
		return err
	}
	conflict = conflict || poisoned
	if conflict {
		if _, err = tx.Exec(ctx, `INSERT INTO manifest_conflicts(conflict_id,event_id,digest,reason) VALUES($1,$2,$3,'IDENTITY_CONFLICT') ON CONFLICT DO NOTHING`, values.StableID("mcf", m.EventID+"\x00"+m.EventDigest), m.EventID, m.EventDigest); err != nil {
			return err
		}
		for _, id := range identities {
			if _, err = tx.Exec(ctx, `INSERT INTO manifest_conflict_identities(identity_kind,tenant_id,identity_value) VALUES('MANIFEST','',$1),('REVISION',$2,$3) ON CONFLICT DO NOTHING`, id.id, id.tenant, id.revision); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `UPDATE runtime_manifests SET conflicted=true WHERE manifest_id=$1 OR (tenant_id=$2 AND deployment_revision_id=$3)`, id.id, id.tenant, id.revision); err != nil {
				return err
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
		return domain.ErrConflict
	}
	if !exists {
		var n int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM runtime_manifests`).Scan(&n); err != nil {
			return err
		}
		if n >= capacity {
			return domain.ErrCapacity
		}
		if _, err = tx.Exec(ctx, `INSERT INTO runtime_manifests(tenant_id,manifest_id,deployment_revision_id,content_digest,envelope_digest,envelope) VALUES($1,$2,$3,$4,$5,$6)`, m.TenantID, m.ManifestID, m.DeploymentRevisionID, m.ContentDigest, m.EnvelopeDigest, m.Envelope); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO manifest_receipts(event_id,event_digest,tenant_id,manifest_id) VALUES($1,$2,$3,$4)`, m.EventID, m.EventDigest, m.TenantID, m.ManifestID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (p *Projection) Read(ctx context.Context, tenant, id string) (domain.Publication, error) {
	var m domain.Publication
	var conflict bool
	err := p.pool.QueryRow(ctx, `SELECT tenant_id,manifest_id,deployment_revision_id,content_digest,envelope_digest,envelope,conflicted FROM runtime_manifests WHERE tenant_id=$1 AND manifest_id=$2`, tenant, id).Scan(&m.TenantID, &m.ManifestID, &m.DeploymentRevisionID, &m.ContentDigest, &m.EnvelopeDigest, &m.Envelope, &conflict)
	if errors.Is(err, pgx.ErrNoRows) {
		// A conflict involving an absent manifest must not become an indefinite
		// "waiting for publication", nor become executable on later replay.
		if err = p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manifest_conflict_identities WHERE identity_kind='MANIFEST' AND tenant_id='' AND identity_value=$1)`, id).Scan(&conflict); err != nil {
			return m, err
		}
		if conflict {
			return m, domain.ErrConflict
		}
		return m, domain.ErrMissing
	}
	if err != nil {
		return m, err
	}
	if conflict || values.Digest(m.Envelope) != m.EnvelopeDigest {
		return m, domain.ErrConflict
	}
	return m, nil
}
