package skill

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

var ErrStoreUnavailable = errors.New("技能上传存储未启用，请检查 PostgreSQL 和 schema 32")
var ErrUploadLimit = errors.New("当前工作空间已达到 128 个上传版本的限制")

type ManagedDescriptor struct {
	Descriptor
	TenantID   string    `json:"tenant_id"`
	Status     string    `json:"status"`
	Revision   int64     `json:"revision"`
	CreatedBy  string    `json:"created_by"`
	ReviewedBy string    `json:"reviewed_by"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
type Store struct{ db *sql.DB }
type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func NewStore(repository any) *Store {
	p, ok := repository.(interface{ SQLDB() *sql.DB })
	if !ok || p.SQLDB() == nil {
		return nil
	}
	return &Store{p.SQLDB()}
}
func (s *Store) sql(ctx context.Context) sqlExecutor {
	if tx := database.Transaction(ctx, s.db); tx != nil {
		return tx
	}
	return s.db
}

const managedColumns = `tenant_id,name,version,checksum,description,(octet_length(script)>0),status,revision,created_by,reviewed_by,created_at,updated_at`

type sqlScanner interface{ Scan(...any) error }

func scanManaged(row sqlScanner) (ManagedDescriptor, error) {
	var d ManagedDescriptor
	err := row.Scan(&d.TenantID, &d.Name, &d.Version, &d.Checksum, &d.Description, &d.Executable, &d.Status, &d.Revision, &d.CreatedBy, &d.ReviewedBy, &d.CreatedAt, &d.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return d, controlplane.ErrNotFound
	}
	if err != nil {
		return d, ErrStoreUnavailable
	}
	d.Source = "uploaded"
	return d, nil
}
func (s *Store) Upload(ctx context.Context, tenant, actor string, input Upload) (ManagedDescriptor, error) {
	if s == nil {
		return ManagedDescriptor{}, ErrStoreUnavailable
	}
	b, err := readUpload(input)
	if err != nil {
		return ManagedDescriptor{}, err
	}
	err = database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if _, err := s.sql(ctx).ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "skill-upload:"+tenant); err != nil {
			return ErrStoreUnavailable
		}
		var count int
		if err := s.sql(ctx).QueryRowContext(ctx, `SELECT count(*) FROM skill_bundle WHERE tenant_id=$1 AND NOT(name=$2 AND version=$3)`, tenant, input.Name, input.Version).Scan(&count); err != nil {
			return ErrStoreUnavailable
		}
		if count >= 128 {
			return ErrUploadLimit
		}
		_, err := s.sql(ctx).ExecContext(ctx, `INSERT INTO skill_bundle(tenant_id,name,version,checksum,description,markdown,script,created_by) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(tenant_id,name,version) DO NOTHING`, tenant, b.descriptor.Name, b.descriptor.Version, b.descriptor.Checksum, b.descriptor.Description, string(b.markdown), string(b.script), actor)
		if err != nil {
			return ErrStoreUnavailable
		}
		return nil
	})
	if err != nil {
		return ManagedDescriptor{}, err
	}
	d, err := s.Get(ctx, tenant, input.Name, input.Version)
	if err != nil {
		return d, err
	}
	if d.Checksum != b.descriptor.Checksum {
		return ManagedDescriptor{}, controlplane.ErrConflict
	}
	return d, nil
}
func (s *Store) Get(ctx context.Context, tenant, name, version string) (ManagedDescriptor, error) {
	if s == nil {
		return ManagedDescriptor{}, ErrStoreUnavailable
	}
	return scanManaged(s.sql(ctx).QueryRowContext(ctx, `SELECT `+managedColumns+` FROM skill_bundle WHERE tenant_id=$1 AND name=$2 AND version=$3`, tenant, name, version))
}
func (s *Store) List(ctx context.Context, tenant, after string, approved bool) ([]ManagedDescriptor, string, error) {
	out := []ManagedDescriptor{}
	if s == nil {
		return out, "", nil
	}
	rows, err := s.sql(ctx).QueryContext(ctx, `SELECT `+managedColumns+` FROM skill_bundle WHERE tenant_id=$1 AND (name||'@'||version)>$2 AND (NOT $3 OR status='approved') ORDER BY name||'@'||version LIMIT 101`, tenant, after, approved)
	if err != nil {
		return nil, "", ErrStoreUnavailable
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		d, e := scanManaged(rows)
		if e != nil {
			return nil, "", e
		}
		if len(out) == 100 {
			return out, key(out[99].Name, out[99].Version), nil
		}
		out = append(out, d)
	}
	if rows.Err() != nil {
		return nil, "", ErrStoreUnavailable
	}
	return out, "", nil
}
func (s *Store) Review(ctx context.Context, tenant, name, version, status, actor string, expected int64) (ManagedDescriptor, error) {
	if s == nil {
		return ManagedDescriptor{}, ErrStoreUnavailable
	}
	if (status != "approved" && status != "revoked") || expected < 1 {
		return ManagedDescriptor{}, ErrUpload
	}
	result, err := s.sql(ctx).ExecContext(ctx, `UPDATE skill_bundle SET status=$4,reviewed_by=$5,revision=revision+1,updated_at=now() WHERE tenant_id=$1 AND name=$2 AND version=$3 AND revision=$6`, tenant, name, version, status, actor, expected)
	if err != nil {
		return ManagedDescriptor{}, ErrStoreUnavailable
	}
	n, err := result.RowsAffected()
	if err != nil {
		return ManagedDescriptor{}, ErrStoreUnavailable
	}
	if n != 1 {
		return ManagedDescriptor{}, controlplane.ErrConflict
	}
	return s.Get(ctx, tenant, name, version)
}
func (s *Store) Inspect(ctx context.Context, tenant, name, version string) (ManagedDescriptor, string, string, error) {
	d, err := s.Get(ctx, tenant, name, version)
	if err != nil {
		return d, "", "", err
	}
	var md, script string
	if s.sql(ctx).QueryRowContext(ctx, `SELECT markdown,script FROM skill_bundle WHERE tenant_id=$1 AND name=$2 AND version=$3`, tenant, name, version).Scan(&md, &script) != nil {
		return d, "", "", ErrStoreUnavailable
	}
	return d, md, script, nil
}
func (s *Store) approved(ctx context.Context, tenant string, ref Ref) (bundle, error) {
	if s == nil {
		return bundle{}, ErrDenied
	}
	var md, script, checksum string
	err := s.sql(ctx).QueryRowContext(ctx, `SELECT markdown,script,checksum FROM skill_bundle WHERE tenant_id=$1 AND name=$2 AND version=$3 AND status='approved'`, tenant, ref.Name, ref.Version).Scan(&md, &script, &checksum)
	if errors.Is(err, sql.ErrNoRows) {
		return bundle{}, ErrDenied
	}
	if err != nil {
		return bundle{}, ErrStoreUnavailable
	}
	if checksum != ref.Checksum {
		return bundle{}, ErrDenied
	}
	b, err := parseBundle(ref.Name, ref.Version, []byte(md), []byte(script))
	if err != nil || b.descriptor.Checksum != checksum {
		return bundle{}, ErrDenied
	}
	return b, nil
}
