//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package tenant

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pq "github.com/lib/pq"
)

var (
	// ErrTenantRepositoryUnavailable indicates that the SQL repository was not
	// fully initialized. Public methods return this stable error instead of
	// dereferencing a nil receiver or database handle.
	ErrTenantRepositoryUnavailable = errors.New("tenant repository unavailable")
	// ErrTenantNotFound indicates the tenant was not found.
	ErrTenantNotFound = errors.New("tenant not found")
	// ErrTenantAlreadyExists indicates the tenant ID is already taken.
	ErrTenantAlreadyExists = errors.New("tenant already exists")
	// ErrTenantConflict indicates an optimistic configuration version conflict.
	ErrTenantConflict = errors.New("tenant configuration version conflict")
	// ErrInvalidTenantConfig indicates a request that cannot be executed by the
	// installed production capability set.
	ErrInvalidTenantConfig = errors.New("invalid tenant configuration")
	// ErrScopedTenantListingUnsupported prevents a scoped principal from
	// silently falling back to a broad credential-bearing repository read.
	ErrScopedTenantListingUnsupported = errors.New("scoped tenant listing is not supported by repository")
)

// Repository defines the tenant persistence interface.
type Repository interface {
	// Create creates a new tenant.
	Create(ctx context.Context, tenant *Tenant) error

	// GetByID retrieves a tenant by ID.
	GetByID(ctx context.Context, tenantID string) (*Tenant, error)

	// GetByWebhookToken retrieves a tenant by webhook token.
	GetByWebhookToken(ctx context.Context, token string) (*Tenant, error)

	// List lists all tenants with optional status filter.
	List(ctx context.Context, status TenantStatus) ([]*Tenant, error)

	// Update updates an existing tenant.
	Update(ctx context.Context, tenant *Tenant) error

	// Delete marks a tenant as deleted.
	Delete(ctx context.Context, tenantID string) error

	// Close closes the repository and releases resources.
	Close() error
}

// ScopedRepository is an optional capability used by least-privilege control
// plane reads. Implementations must constrain the SQL query by the supplied
// tenant IDs before decoding configuration; filtering after a broad read can
// expose other tenants' encrypted credentials to the process.
type ScopedRepository interface {
	ListByIDs(ctx context.Context, status TenantStatus, tenantIDs []string) ([]*Tenant, error)
}

// StatusRepository is the least-privilege repository capability used by
// queue consumers that only need to enforce the tenant lifecycle fence. It
// must not decode the tenant configuration or materialize any credentials.
type StatusRepository interface {
	GetStatus(ctx context.Context, tenantID string) (TenantStatus, error)
}

// SQLRepository implements Repository using SQL database.
type SQLRepository struct {
	db *sql.DB
}

// NewSQLRepository creates a new SQL-based tenant repository.
func NewSQLRepository(driverName, dataSourceName string) (*SQLRepository, error) {
	if driverName != "postgres" {
		return nil, fmt.Errorf("tenant control-plane repository supports postgres only, got %q", driverName)
	}
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &SQLRepository{db: db}, nil
}

// Create creates a new tenant.
func (r *SQLRepository) Create(ctx context.Context, tenant *Tenant) error {
	if err := r.requireDB(); err != nil {
		return err
	}
	if tenant == nil {
		return fmt.Errorf("%w: tenant is required", ErrInvalidTenantConfig)
	}
	actor, err := auditActorFromContext(ctx)
	if err != nil {
		return err
	}
	configJSON, err := json.Marshal(map[string]interface{}{
		"agents":     tenant.Agents,
		"models":     tenant.Models,
		"toolPolicy": tenant.ToolPolicy,
		"channels":   tenant.Channels,
		"storage":    tenant.Storage,
		"governance": tenant.Governance,
		"budget":     tenant.Budget,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	query := `
		INSERT INTO tenants (id, name, status, config, config_version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	now := time.Now()
	configVersion := tenant.ConfigVersion
	if configVersion == 0 {
		configVersion = 1
	}

	ctx = repositoryContext(ctx)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tenant create: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, query,
		tenant.ID,
		tenant.Name,
		tenant.Status,
		configJSON,
		configVersion,
		now,
		now,
	)

	if err != nil {
		// Check for unique constraint violation
		if isDuplicateError(err) {
			return ErrTenantAlreadyExists
		}
		return fmt.Errorf("failed to insert tenant: %w", err)
	}

	// Insert channel bindings
	for index := range tenant.Channels {
		if err := insertChannelBinding(ctx, tx, tenant.ID, index, &tenant.Channels[index]); err != nil {
			return err
		}
	}
	if err := writeTenantAudit(ctx, tx, tenant.ID, actor, "tenant.create", map[string]interface{}{
		"config_version": configVersion,
		"status":         tenant.Status,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tenant create: %w", err)
	}
	tenant.ConfigVersion = configVersion
	tenant.CreatedAt = now
	tenant.UpdatedAt = now
	return nil
}

// GetByID retrieves a tenant by ID.
func (r *SQLRepository) GetByID(ctx context.Context, tenantID string) (*Tenant, error) {
	return r.getByID(ctx, tenantID, false)
}

// GetStatus returns only the lifecycle status needed by queue admission. It
// intentionally avoids selecting the JSON configuration, so Consumer does
// not load or decrypt model, storage, or channel credentials merely to reject
// a suspended or deleted tenant.
func (r *SQLRepository) GetStatus(ctx context.Context, tenantID string) (TenantStatus, error) {
	if err := r.requireDB(); err != nil {
		return "", err
	}
	ctx = repositoryContext(ctx)
	var status TenantStatus
	err := r.db.QueryRowContext(ctx, `SELECT status FROM tenants WHERE id = $1`, tenantID).Scan(&status)
	if err == sql.ErrNoRows {
		return "", ErrTenantNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to query tenant status: %w", err)
	}
	return status, nil
}

func (r *SQLRepository) getByID(ctx context.Context, tenantID string, activeOnly bool) (*Tenant, error) {
	if err := r.requireDB(); err != nil {
		return nil, err
	}
	ctx = repositoryContext(ctx)
	query := `
		SELECT id, name, status, config, config_version, created_at, updated_at
		FROM tenants
		WHERE id = $1`
	args := []interface{}{tenantID}
	if activeOnly {
		query += " AND status = $2"
		args = append(args, TenantStatusActive)
	} else {
		query += " AND status != $2"
		args = append(args, TenantStatusDeleted)
	}

	var tenant Tenant
	var configJSON []byte

	err := r.db.QueryRowContext(ctx, query, args...).Scan(
		&tenant.ID,
		&tenant.Name,
		&tenant.Status,
		&configJSON,
		&tenant.ConfigVersion,
		&tenant.CreatedAt,
		&tenant.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query tenant: %w", err)
	}

	if err := decodeTenantConfig(configJSON, &tenant); err != nil {
		return nil, err
	}

	return &tenant, nil
}

// GetByWebhookToken retrieves a tenant by the opaque callback route key.
//
// The historical method name is retained for source compatibility. After
// migration 026, webhook_token is never a lookup fallback: it may contain
// legacy/provider material on an installation that has not been migrated, and
// accepting it here would turn that material into an ingress capability.
func (r *SQLRepository) GetByWebhookToken(ctx context.Context, token string) (*Tenant, error) {
	if err := r.requireDB(); err != nil {
		return nil, err
	}
	ctx = repositoryContext(ctx)
	query := `
		SELECT t.id, t.name, t.status, t.config, t.config_version,
		       t.created_at, t.updated_at, tc.channel_type, tc.channel_index,
		       tc.webhook_key
		FROM tenants t
		JOIN tenant_channels tc ON t.id = tc.tenant_id
		WHERE tc.webhook_key = $1
		  AND t.status = $2
	`

	var value Tenant
	var configJSON []byte
	var channelType, webhookKey string
	var channelIndex int
	err := r.db.QueryRowContext(ctx, query, token, TenantStatusActive).Scan(
		&value.ID, &value.Name, &value.Status, &configJSON, &value.ConfigVersion,
		&value.CreatedAt, &value.UpdatedAt, &channelType, &channelIndex, &webhookKey,
	)

	if err == sql.ErrNoRows {
		return nil, ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query tenant by webhook token: %w", err)
	}

	if err := decodeTenantConfig(configJSON, &value); err != nil {
		return nil, err
	}
	if channelIndex < 0 || channelIndex >= len(value.Channels) || value.Channels[channelIndex].Type != channelType {
		return nil, ErrTenantNotFound
	}
	// The route key is the only value accepted from the callback lookup table;
	// provider verification credentials remain encrypted in tenant config.
	value.Channels[channelIndex].WebhookKey = webhookKey
	value.Channels[channelIndex].EnsureAccountID()
	return &value, nil
}

// List lists all tenants with optional status filter.
func (r *SQLRepository) List(ctx context.Context, status TenantStatus) ([]*Tenant, error) {
	if err := r.requireDB(); err != nil {
		return nil, err
	}
	ctx = repositoryContext(ctx)
	query := `
		SELECT id, name, status, config, config_version, created_at, updated_at
		FROM tenants
	`
	args := []interface{}{}

	if status != "" {
		query += " WHERE status = $1"
		args = append(args, status)
	}

	query += " ORDER BY created_at DESC"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query tenants: %w", err)
	}
	defer rows.Close()

	var tenants []*Tenant
	for rows.Next() {
		var tenant Tenant
		var configJSON []byte

		if err := rows.Scan(
			&tenant.ID,
			&tenant.Name,
			&tenant.Status,
			&configJSON,
			&tenant.ConfigVersion,
			&tenant.CreatedAt,
			&tenant.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan tenant: %w", err)
		}

		if err := decodeTenantConfig(configJSON, &tenant); err != nil {
			return nil, err
		}

		tenants = append(tenants, &tenant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while iterating tenants: %w", err)
	}

	return tenants, nil
}

// ListByIDs lists only the requested tenants. The ID predicate is applied in
// SQL so a scoped operator never causes unrelated tenant configurations to be
// loaded and decrypted in the process.
func (r *SQLRepository) ListByIDs(ctx context.Context, status TenantStatus, tenantIDs []string) ([]*Tenant, error) {
	if err := r.requireDB(); err != nil {
		return nil, err
	}
	if len(tenantIDs) == 0 {
		return []*Tenant{}, nil
	}
	ctx = repositoryContext(ctx)
	query := `
		SELECT id, name, status, config, config_version, created_at, updated_at
		FROM tenants
		WHERE id = ANY($1)`
	args := []interface{}{pq.Array(tenantIDs)}
	if status != "" {
		query += " AND status = $2"
		args = append(args, status)
	}
	query += " ORDER BY created_at DESC"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query scoped tenants: %w", err)
	}
	defer rows.Close()

	var tenants []*Tenant
	for rows.Next() {
		var tenant Tenant
		var configJSON []byte
		if err := rows.Scan(
			&tenant.ID,
			&tenant.Name,
			&tenant.Status,
			&configJSON,
			&tenant.ConfigVersion,
			&tenant.CreatedAt,
			&tenant.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan scoped tenant: %w", err)
		}
		if err := decodeTenantConfig(configJSON, &tenant); err != nil {
			return nil, err
		}
		tenants = append(tenants, &tenant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while iterating scoped tenants: %w", err)
	}
	return tenants, nil
}

func decodeTenantConfig(configJSON []byte, value *Tenant) error {
	var config map[string]json.RawMessage
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return fmt.Errorf("failed to unmarshal tenant config: %w", err)
	}
	fields := []struct {
		name   string
		target interface{}
	}{
		{"agents", &value.Agents},
		{"models", &value.Models},
		{"toolPolicy", &value.ToolPolicy},
		{"channels", &value.Channels},
		{"storage", &value.Storage},
		{"governance", &value.Governance},
		{"budget", &value.Budget},
	}
	for _, field := range fields {
		data, ok := config[field.name]
		if !ok {
			continue
		}
		if err := json.Unmarshal(data, field.target); err != nil {
			return fmt.Errorf("failed to decode tenant config field %s: %w", field.name, err)
		}
	}
	return nil
}

// Update updates an existing tenant.
func (r *SQLRepository) Update(ctx context.Context, tenant *Tenant) error {
	if err := r.requireDB(); err != nil {
		return err
	}
	if tenant == nil {
		return fmt.Errorf("%w: tenant is required", ErrInvalidTenantConfig)
	}
	actor, err := auditActorFromContext(ctx)
	if err != nil {
		return err
	}
	configJSON, err := json.Marshal(map[string]interface{}{
		"agents":     tenant.Agents,
		"models":     tenant.Models,
		"toolPolicy": tenant.ToolPolicy,
		"channels":   tenant.Channels,
		"storage":    tenant.Storage,
		"governance": tenant.Governance,
		"budget":     tenant.Budget,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	query := `
		UPDATE tenants
		SET name = $1, status = $2, config = $3, updated_at = $4,
		    config_version = config_version + 1
		WHERE id = $5 AND config_version = $6 AND status <> $7
		RETURNING config_version
	`

	updatedAt := time.Now()

	ctx = repositoryContext(ctx)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tenant update: %w", err)
	}
	defer tx.Rollback()

	// Lock the same row changed by migration cutover before checking its current
	// typed storage binding. A stale snapshot cannot overwrite a completed route.
	var currentConfig []byte
	err = tx.QueryRowContext(ctx, `
		SELECT config FROM tenants
		WHERE id = $1 AND config_version = $2 AND status <> $3
		FOR UPDATE`, tenant.ID, tenant.ConfigVersion, TenantStatusDeleted).Scan(&currentConfig)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrTenantConflict
	}
	if err != nil {
		return fmt.Errorf("failed to lock tenant storage binding: %w", err)
	}
	var current struct {
		Storage StorageConfig `json:"storage"`
	}
	if err := json.Unmarshal(currentConfig, &current); err != nil {
		return fmt.Errorf("%w: persisted storage binding cannot be decoded", ErrInvalidTenantConfig)
	}
	if err := validateStorageBindingUpdate(current.Storage, tenant.Storage); err != nil {
		return err
	}

	var nextConfigVersion int64
	err = tx.QueryRowContext(ctx, query,
		tenant.Name,
		tenant.Status,
		configJSON,
		updatedAt,
		tenant.ID,
		tenant.ConfigVersion,
		TenantStatusDeleted,
	).Scan(&nextConfigVersion)

	if errors.Is(err, sql.ErrNoRows) {
		return ErrTenantConflict
	}
	if err != nil {
		return fmt.Errorf("failed to update tenant: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_channels WHERE tenant_id=$1`, tenant.ID); err != nil {
		return fmt.Errorf("failed to replace tenant channel bindings: %w", err)
	}
	for index := range tenant.Channels {
		if err := insertChannelBinding(ctx, tx, tenant.ID, index, &tenant.Channels[index]); err != nil {
			return err
		}
	}
	if err := writeTenantAudit(ctx, tx, tenant.ID, actor, "tenant.update", map[string]interface{}{
		"config_version": nextConfigVersion,
		"status":         tenant.Status,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tenant update: %w", err)
	}
	tenant.ConfigVersion = nextConfigVersion
	tenant.UpdatedAt = updatedAt
	return nil
}

// Delete marks a tenant as deleted.
func (r *SQLRepository) Delete(ctx context.Context, tenantID string) error {
	if err := r.requireDB(); err != nil {
		return err
	}
	actor, err := auditActorFromContext(ctx)
	if err != nil {
		return err
	}
	query := `
		UPDATE tenants
		SET status = $1, updated_at = $2,
		    config_version = config_version + 1
		WHERE id = $3 AND status <> $4
	`

	ctx = repositoryContext(ctx)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tenant delete: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, query, TenantStatusDeleted, time.Now(), tenantID, TenantStatusDeleted)
	if err != nil {
		return fmt.Errorf("failed to delete tenant: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrTenantNotFound
	}
	if err := writeTenantAudit(ctx, tx, tenantID, actor, "tenant.delete", map[string]interface{}{
		"status": TenantStatusDeleted,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tenant delete: %w", err)
	}
	return nil
}

func writeTenantAudit(ctx context.Context, tx *sql.Tx, tenantID, actor, action string, details interface{}) error {
	payload, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal tenant audit details: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO control_plane_audit
			(tenant_id, actor, action, resource_type, resource_id, details)
		VALUES ($1, $2, $3, 'tenant', $4, $5)`, tenantID, actor, action, tenantID, payload); err != nil {
		return fmt.Errorf("failed to write tenant control-plane audit: %w", err)
	}
	return nil
}

// Close closes the repository.
func (r *SQLRepository) Close() error {
	if err := r.requireDB(); err != nil {
		return err
	}
	return r.db.Close()
}

// PingContext verifies the database connection is usable.
func (r *SQLRepository) PingContext(ctx context.Context) error {
	if err := r.requireDB(); err != nil {
		return err
	}
	return r.db.PingContext(repositoryContext(ctx))
}

func (r *SQLRepository) requireDB() error {
	if r == nil || r.db == nil {
		return ErrTenantRepositoryUnavailable
	}
	return nil
}

func repositoryContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

type contextExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// insertChannelBinding inserts a channel binding through the caller's transaction.
func insertChannelBinding(ctx context.Context, executor contextExecer, tenantID string, channelIndex int, channel *ChannelBinding) error {
	if isNilTenantDependency(executor) {
		return ErrTenantRepositoryUnavailable
	}
	if channel == nil {
		return fmt.Errorf("%w: channel binding is required", ErrInvalidTenantConfig)
	}
	ctx = repositoryContext(ctx)
	configJSON, err := json.Marshal(channel.Config)
	if err != nil {
		return fmt.Errorf("failed to marshal channel config: %w", err)
	}

	query := `
		INSERT INTO tenant_channels
			(tenant_id, channel_type, channel_index, webhook_token, webhook_key, config, created_at)
		VALUES ($1, $2, $3, $4, $4, $5, $6)
	`

	webhookKey := channel.EffectiveWebhookKey()
	if webhookKey == "" {
		return fmt.Errorf("channel binding webhook key is required")
	}
	_, err = executor.ExecContext(ctx, query,
		tenantID,
		channel.Type,
		channelIndex,
		webhookKey,
		configJSON,
		time.Now(),
	)

	if err != nil {
		return fmt.Errorf("failed to insert channel binding: %w", err)
	}

	return nil
}

// isDuplicateError checks if the error is a duplicate key error.
func isDuplicateError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// PostgreSQL
	if contains([]string{"duplicate key value"}, errStr) {
		return true
	}
	// MySQL
	if contains([]string{"Duplicate entry"}, errStr) {
		return true
	}
	return false
}

func contains(patterns []string, text string) bool {
	for _, pattern := range patterns {
		if len(text) >= len(pattern) {
			for i := 0; i <= len(text)-len(pattern); i++ {
				if text[i:i+len(pattern)] == pattern {
					return true
				}
			}
		}
	}
	return false
}
