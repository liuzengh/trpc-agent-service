// Package llmstore persists model endpoints in MySQL. It implements
// domain/llm.Store; shared SQL helpers come from sqlutil.
package llmstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/asset"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/sqlutil"
)

// mysqlStore persists endpoints in the model_endpoints table. Only the
// credential-store reference (api_key_ref) is stored; a plaintext API key is
// rejected so it can never sit at rest in the durable store.
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLRegistry returns a registry persisting endpoints in MySQL (table
// model_endpoints) while keeping model instances cached in memory.
func NewMySQLRegistry(db *sql.DB, factory llm.ModelFactory) *llm.Registry {
	return llm.NewRegistryWithStore(&mysqlStore{db: db}, factory)
}

const endpointCols = "endpoint_id, scope, tenant_id, name, provider, endpoint_type, base_url, model_name, api_key_ref, created_by, visibility"

// normalizeType maps an empty endpoint type to chat (backward compatible).
func normalizeType(t string) string {
	if t == "" {
		return llm.EndpointTypeChat
	}
	return t
}

// rejectPlaintext refuses an endpoint carrying a plaintext API key with no
// credential-store reference: the durable store must never hold plaintext at
// rest. Callers convert plaintext to a reference via llm.Registry (which
// stores it in the credential store) before reaching here.
func rejectPlaintext(ep llm.Endpoint) error {
	if ep.APIKey != "" && ep.APIKeyRef == "" {
		return fmt.Errorf("llm: endpoint %q carries a plaintext api_key; set api_key_ref (or configure a credential store)", ep.ID)
	}
	return nil
}

func (s *mysqlStore) Create(ctx context.Context, ep llm.Endpoint) error {
	if err := rejectPlaintext(ep); err != nil {
		return err
	}
	ep.Visibility = asset.VisibilityOrDefault(ep.Visibility)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO model_endpoints (endpoint_id, scope, tenant_id, name, provider, endpoint_type, base_url, model_name, api_key_ref, created_by, visibility)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ep.ID, ep.Scope, sqlutil.Null(ep.TenantID), ep.Name, llm.NormalizeProvider(ep.Provider), normalizeType(ep.Type), ep.BaseURL, ep.ModelName, ep.APIKeyRef,
		sqlutil.Null(ep.CreatedBy), ep.Visibility)
	if sqlutil.IsDuplicate(err) {
		return fmt.Errorf("%w: %q", llm.ErrEndpointExists, ep.ID)
	}
	return err
}

func (s *mysqlStore) Update(ctx context.Context, ep llm.Endpoint) error {
	if err := rejectPlaintext(ep); err != nil {
		return err
	}
	// created_by is set at creation and never rewritten: ownership does not move.
	res, err := s.db.ExecContext(ctx,
		`UPDATE model_endpoints
		 SET scope = ?, tenant_id = ?, name = ?, provider = ?, endpoint_type = ?, base_url = ?, model_name = ?, api_key_ref = ?, visibility = ?
		 WHERE endpoint_id = ? AND is_deleted = 0`,
		ep.Scope, sqlutil.Null(ep.TenantID), ep.Name, llm.NormalizeProvider(ep.Provider), normalizeType(ep.Type), ep.BaseURL, ep.ModelName, ep.APIKeyRef,
		asset.VisibilityOrDefault(ep.Visibility), ep.ID)
	if err != nil {
		return err
	}
	return sqlutil.RowsAffected(res, llm.ErrEndpointNotFound, ep.ID)
}

func (s *mysqlStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE model_endpoints SET is_deleted = 1 WHERE endpoint_id = ? AND is_deleted = 0`, id)
	if err != nil {
		return err
	}
	return sqlutil.RowsAffected(res, llm.ErrEndpointNotFound, id)
}

func (s *mysqlStore) Get(ctx context.Context, id string) (llm.Endpoint, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+endpointCols+` FROM model_endpoints WHERE endpoint_id = ? AND is_deleted = 0`, id)
	return scanEndpoint(row)
}

func (s *mysqlStore) List(ctx context.Context) ([]llm.Endpoint, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+endpointCols+` FROM model_endpoints WHERE is_deleted = 0 ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]llm.Endpoint, 0)
	for rows.Next() {
		ep, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ep)
	}
	return out, rows.Err()
}

func (s *mysqlStore) Upsert(ctx context.Context, ep llm.Endpoint) error {
	if err := rejectPlaintext(ep); err != nil {
		return err
	}
	ep.Visibility = asset.VisibilityOrDefault(ep.Visibility)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO model_endpoints (endpoint_id, scope, tenant_id, name, provider, endpoint_type, base_url, model_name, api_key_ref, created_by, visibility)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   scope = VALUES(scope), tenant_id = VALUES(tenant_id), name = VALUES(name),
		   provider = VALUES(provider), endpoint_type = VALUES(endpoint_type),
		   base_url = VALUES(base_url), model_name = VALUES(model_name),
		   api_key_ref = VALUES(api_key_ref), visibility = VALUES(visibility), is_deleted = 0`,
		ep.ID, ep.Scope, sqlutil.Null(ep.TenantID), ep.Name, llm.NormalizeProvider(ep.Provider), normalizeType(ep.Type), ep.BaseURL, ep.ModelName, ep.APIKeyRef,
		sqlutil.Null(ep.CreatedBy), ep.Visibility)
	return err
}

func scanEndpoint(sc sqlutil.RowScanner) (llm.Endpoint, error) {
	var (
		ep        llm.Endpoint
		tenantID  sql.NullString
		createdBy sql.NullString
	)
	if err := sc.Scan(&ep.ID, &ep.Scope, &tenantID, &ep.Name, &ep.Provider, &ep.Type, &ep.BaseURL, &ep.ModelName, &ep.APIKeyRef,
		&createdBy, &ep.Visibility); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return llm.Endpoint{}, llm.ErrEndpointNotFound
		}
		return llm.Endpoint{}, err
	}
	ep.TenantID = tenantID.String
	ep.CreatedBy = createdBy.String
	ep.Visibility = asset.VisibilityOrDefault(ep.Visibility)
	return ep, nil
}
