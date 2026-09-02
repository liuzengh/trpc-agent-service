package llm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	gosql "github.com/go-sql-driver/mysql"
)

// mysqlStore persists endpoints in the model_endpoints table. api_key_ref is
// stored as the raw key value for now; the secret store lands in Phase 8.
type mysqlStore struct {
	db *sql.DB
}

const endpointCols = "endpoint_id, scope, tenant_id, name, provider, endpoint_type, base_url, model_name, api_key_ref"

// normalizeType maps an empty endpoint type to chat (backward compatible).
func normalizeType(t string) string {
	if t == "" {
		return EndpointTypeChat
	}
	return t
}

func (s *mysqlStore) Create(ctx context.Context, ep Endpoint) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO model_endpoints (endpoint_id, scope, tenant_id, name, provider, endpoint_type, base_url, model_name, api_key_ref)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ep.ID, ep.Scope, nullString(ep.TenantID), ep.Name, NormalizeProvider(ep.Provider), normalizeType(ep.Type), ep.BaseURL, ep.ModelName, ep.APIKey)
	if isDuplicate(err) {
		return fmt.Errorf("llm: endpoint %q already exists", ep.ID)
	}
	return err
}

func (s *mysqlStore) Update(ctx context.Context, ep Endpoint) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE model_endpoints
		 SET scope = ?, tenant_id = ?, name = ?, provider = ?, endpoint_type = ?, base_url = ?, model_name = ?, api_key_ref = ?
		 WHERE endpoint_id = ? AND is_deleted = 0`,
		ep.Scope, nullString(ep.TenantID), ep.Name, NormalizeProvider(ep.Provider), normalizeType(ep.Type), ep.BaseURL, ep.ModelName, ep.APIKey, ep.ID)
	if err != nil {
		return err
	}
	return rowsAffectedNotFound(res, ep.ID)
}

func (s *mysqlStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE model_endpoints SET is_deleted = 1 WHERE endpoint_id = ? AND is_deleted = 0`, id)
	if err != nil {
		return err
	}
	return rowsAffectedNotFound(res, id)
}

func (s *mysqlStore) Get(ctx context.Context, id string) (Endpoint, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+endpointCols+` FROM model_endpoints WHERE endpoint_id = ? AND is_deleted = 0`, id)
	return scanEndpoint(row)
}

func (s *mysqlStore) List(ctx context.Context) ([]Endpoint, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+endpointCols+` FROM model_endpoints WHERE is_deleted = 0 ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]Endpoint, 0)
	for rows.Next() {
		ep, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ep)
	}
	return out, rows.Err()
}

func (s *mysqlStore) Upsert(ctx context.Context, ep Endpoint) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO model_endpoints (endpoint_id, scope, tenant_id, name, provider, endpoint_type, base_url, model_name, api_key_ref)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   scope = VALUES(scope), tenant_id = VALUES(tenant_id), name = VALUES(name),
		   provider = VALUES(provider), endpoint_type = VALUES(endpoint_type),
		   base_url = VALUES(base_url), model_name = VALUES(model_name),
		   api_key_ref = VALUES(api_key_ref), is_deleted = 0`,
		ep.ID, ep.Scope, nullString(ep.TenantID), ep.Name, NormalizeProvider(ep.Provider), normalizeType(ep.Type), ep.BaseURL, ep.ModelName, ep.APIKey)
	return err
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanEndpoint(sc scanner) (Endpoint, error) {
	var (
		ep       Endpoint
		tenantID sql.NullString
	)
	if err := sc.Scan(&ep.ID, &ep.Scope, &tenantID, &ep.Name, &ep.Provider, &ep.Type, &ep.BaseURL, &ep.ModelName, &ep.APIKey); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Endpoint{}, ErrEndpointNotFound
		}
		return Endpoint{}, err
	}
	ep.TenantID = tenantID.String
	return ep, nil
}

func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func isDuplicate(err error) bool {
	var my *gosql.MySQLError
	return errors.As(err, &my) && my.Number == 1062
}

func rowsAffectedNotFound(res sql.Result, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", ErrEndpointNotFound, id)
	}
	return nil
}
