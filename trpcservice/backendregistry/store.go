package backendregistry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credentials"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

var ErrUnavailable = errors.New("存储连接管理未启用，请检查 PostgreSQL、schema 32 和加密主密钥")
var connectionID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Connection struct {
	TenantID  string          `json:"tenant_id"`
	ID        string          `json:"connection_id"`
	Name      string          `json:"name"`
	Resource  string          `json:"resource_type"`
	Backend   string          `json:"backend_type"`
	Settings  Settings        `json:"settings"`
	CreatedBy string          `json:"created_by"`
	CreatedAt time.Time       `json:"created_at"`
	Config    json.RawMessage `json:"-"`
	SecretRef string          `json:"-"`
}

type Store struct {
	db         *sql.DB
	vault      *credentials.Vault
	authorizer secret.Authorizer
}
type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func New(repo any, vault *credentials.Vault, authorizer secret.Authorizer) *Store {
	p, ok := repo.(interface{ SQLDB() *sql.DB })
	if !ok || p.SQLDB() == nil || vault == nil {
		return nil
	}
	return &Store{p.SQLDB(), vault, authorizer}
}
func (s *Store) sql(ctx context.Context) executor {
	if tx := database.Transaction(ctx, s.db); tx != nil {
		return tx
	}
	return s.db
}
func (s *Store) Create(ctx context.Context, c Connection, values Secrets, existing string) (Connection, error) {
	if s == nil {
		return Connection{}, ErrUnavailable
	}
	if c.TenantID == "" || strings.TrimSpace(c.Name) == "" || len(c.Name) > 100 || strings.ContainsAny(c.Name, "\r\n\x00") || c.CreatedBy == "" {
		return Connection{}, ErrInvalid
	}
	if c.ID == "" {
		c.ID = "storage-" + uuid.NewString()
	}
	if !connectionID.MatchString(c.ID) {
		return Connection{}, ErrInvalid
	}
	raw, value, err := Build(c.Resource, c.Backend, c.Settings, values, existing)
	if err != nil {
		return Connection{}, err
	}
	if existing != "" {
		if !strings.HasPrefix(existing, "env://") || s.authorizer == nil || s.authorizer.Authorize(ctx, c.TenantID, c.Resource, existing) != nil {
			return Connection{}, secret.ErrForbidden
		}
	}
	c.Config = raw
	c.SecretRef = existing
	err = database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if value != "" {
			ref, e := s.vault.Put(ctx, c.TenantID, []string{c.Resource}, value)
			if e != nil {
				return ErrUnavailable
			}
			c.SecretRef = ref
		}
		settings, _ := json.Marshal(c.Settings)
		_, e := s.sql(ctx).ExecContext(ctx, `INSERT INTO backend_connection(tenant_id,connection_id,display_name,resource_type,backend_type,config,settings,credential_ref,created_by) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, c.TenantID, c.ID, c.Name, c.Resource, c.Backend, string(raw), string(settings), c.SecretRef, c.CreatedBy)
		if e != nil {
			var pg *pgconn.PgError
			if errors.As(e, &pg) && pg.Code == "23505" {
				return controlplane.ErrConflict
			}
			return ErrUnavailable
		}
		return nil
	})
	if err != nil {
		return Connection{}, err
	}
	return s.Get(ctx, c.TenantID, c.ID)
}

// ValidateBinding prevents the legacy binding API from sending an encrypted
// connection credential to an attacker-chosen destination or resource type.
func (s *Store) ValidateBinding(ctx context.Context, b controlplane.BackendBinding) error {
	if s == nil {
		return secret.ErrForbidden
	}
	var allowed bool
	err := s.sql(ctx).QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM backend_connection WHERE tenant_id=$1 AND credential_ref=$2 AND resource_type=$3 AND backend_type=$4 AND config=$5::jsonb)`, b.TenantID, b.SecretRef, b.ResourceType, b.BackendType, string(b.Config)).Scan(&allowed)
	if err != nil {
		return ErrUnavailable
	}
	if !allowed {
		return secret.ErrForbidden
	}
	return nil
}

const columns = `tenant_id,connection_id,display_name,resource_type,backend_type,settings,created_by,created_at,config,credential_ref`

type scanner interface{ Scan(...any) error }

func scan(row scanner) (Connection, error) {
	var c Connection
	var settings []byte
	err := row.Scan(&c.TenantID, &c.ID, &c.Name, &c.Resource, &c.Backend, &settings, &c.CreatedBy, &c.CreatedAt, &c.Config, &c.SecretRef)
	if errors.Is(err, sql.ErrNoRows) {
		return c, controlplane.ErrNotFound
	}
	if err != nil || json.Unmarshal(settings, &c.Settings) != nil {
		return Connection{}, ErrUnavailable
	}
	return c, nil
}
func (s *Store) Get(ctx context.Context, tenant, id string) (Connection, error) {
	if s == nil {
		return Connection{}, ErrUnavailable
	}
	return scan(s.sql(ctx).QueryRowContext(ctx, `SELECT `+columns+` FROM backend_connection WHERE tenant_id=$1 AND connection_id=$2`, tenant, id))
}
func (s *Store) List(ctx context.Context, tenant, after string) ([]Connection, string, error) {
	out := []Connection{}
	if s == nil {
		return out, "", nil
	}
	rows, err := s.sql(ctx).QueryContext(ctx, `SELECT `+columns+` FROM backend_connection WHERE tenant_id=$1 AND connection_id>$2 ORDER BY connection_id LIMIT 101`, tenant, after)
	if err != nil {
		return nil, "", ErrUnavailable
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		c, e := scan(rows)
		if e != nil {
			return nil, "", e
		}
		if len(out) == 100 {
			return out, out[99].ID, nil
		}
		out = append(out, c)
	}
	if rows.Err() != nil {
		return nil, "", ErrUnavailable
	}
	return out, "", nil
}
