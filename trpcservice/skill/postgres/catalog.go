// Package postgres persists the immutable Skill publication catalog.
package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
)

type DB interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type Catalog struct{ db DB }

func New(db DB) *Catalog { return &Catalog{db: db} }
func (c *Catalog) Stage(ctx context.Context, in skill.Package) (skill.Package, error) {
	if c == nil || c.db == nil {
		return skill.Package{}, runtime.ErrCapabilityUnsupported
	}
	value, err := skill.ValidatePackage(in)
	if err != nil {
		return skill.Package{}, err
	}
	var out skill.Package
	err = c.db.QueryRowContext(ctx, `INSERT INTO skill_catalog(tenant_id,skill_id,skill_version,content_digest,relative_path,state)
VALUES($1,$2,$3,$4,$5,'staged') ON CONFLICT(tenant_id,skill_id,skill_version) DO NOTHING
RETURNING tenant_id,skill_id,skill_version,content_digest,relative_path`, value.TenantID, value.SkillID, value.Version, value.ContentDigest, value.RelativePath).Scan(&out.TenantID, &out.SkillID, &out.Version, &out.ContentDigest, &out.RelativePath)
	if errors.Is(err, sql.ErrNoRows) {
		// A Skill package is immutable. Retrying Stage must therefore read the
		// existing package rather than issue a no-op UPDATE: the schema trigger
		// correctly reserves UPDATE for a record-version advancing transition.
		err = c.db.QueryRowContext(ctx, `SELECT tenant_id,skill_id,skill_version,content_digest,relative_path
FROM skill_catalog WHERE tenant_id=$1 AND skill_id=$2 AND skill_version=$3`, value.TenantID, value.SkillID, value.Version).
			Scan(&out.TenantID, &out.SkillID, &out.Version, &out.ContentDigest, &out.RelativePath)
	}
	if err != nil {
		return skill.Package{}, translate(err)
	}
	if out != value {
		return skill.Package{}, runtime.ErrIdempotencyCollision
	}
	return out, nil
}
func (c *Catalog) Publish(ctx context.Context, tenant, id string, version int64) (skill.Package, error) {
	if c == nil || c.db == nil || tenant == "" || id == "" || version < 1 {
		return skill.Package{}, runtime.ErrInvalidEnvelope
	}
	var out skill.Package
	err := c.db.QueryRowContext(ctx, `UPDATE skill_catalog SET state='published',published_at=now(),record_version=record_version+1 WHERE tenant_id=$1 AND skill_id=$2 AND skill_version=$3 AND state='staged' RETURNING tenant_id,skill_id,skill_version,content_digest,relative_path`, tenant, id, version).Scan(&out.TenantID, &out.SkillID, &out.Version, &out.ContentDigest, &out.RelativePath)
	if err == nil {
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return skill.Package{}, translate(err)
	}
	return c.Resolve(ctx, tenant, id, version)
}
func (c *Catalog) Resolve(ctx context.Context, tenant, id string, version int64) (skill.Package, error) {
	if c == nil || c.db == nil || tenant == "" || id == "" || version < 1 {
		return skill.Package{}, runtime.ErrInvalidEnvelope
	}
	var out skill.Package
	err := c.db.QueryRowContext(ctx, `SELECT tenant_id,skill_id,skill_version,content_digest,relative_path FROM skill_catalog WHERE tenant_id=$1 AND skill_id=$2 AND skill_version=$3 AND state='published'`, tenant, id, version).Scan(&out.TenantID, &out.SkillID, &out.Version, &out.ContentDigest, &out.RelativePath)
	if err != nil {
		return skill.Package{}, translate(err)
	}
	return out, nil
}
func translate(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	return err
}

var _ skill.LifecycleCatalog = (*Catalog)(nil)
