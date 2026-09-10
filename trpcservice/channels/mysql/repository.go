// Package mysql provides the MySQL implementation of the Channel
// Binding repository and candidate capability index.
package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

const (
	mysqlCandidateTTL  = channels.DefaultCandidateTTL
	mysqlMaxCandidates = 4096
)

type candidateRecord struct {
	tenantID  string
	bindingID string
	context   channels.CandidateBindingContext
}

// ChannelRepository persists Channel Bindings and owns short-lived candidate
// capabilities. The capability cache is intentionally process-local; the
// durable Binding version/digest check makes a restart fail closed.
type ChannelRepository struct {
	db          *sql.DB
	candidateMu sync.Mutex
	candidates  map[string]candidateRecord
}

var _ channels.Repository = (*ChannelRepository)(nil)
var _ channels.CandidateIndex = (*ChannelRepository)(nil)
var _ channels.CandidateConsumer = (*ChannelRepository)(nil)

// List returns a stable page of Channel Bindings belonging to one tenant.
func (r *ChannelRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*channels.Binding, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if r == nil || r.db == nil {
		return nil, "", ErrStorage
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeListCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT binding_id FROM channel_binding WHERE tenant_id = ? ORDER BY binding_id`, tenantID)
	if err != nil {
		return nil, "", ErrStorage
	}
	var bindingIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, "", ErrStorage
		}
		bindingIDs = append(bindingIDs, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, "", ErrStorage
	}
	_ = rows.Close()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*channels.Binding, 0)
	for _, id := range bindingIDs {
		value, err := r.Get(ctx, tenantID, id)
		if err != nil {
			return nil, "", ErrStorage
		}
		if status != "" && string(value.Status) != status {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.BindingID+" "+value.BindingKey+" "+string(value.Channel)+" "+value.ProviderAccountID), query) {
			continue
		}
		items = append(items, value)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].BindingID < items[j].BindingID })
	if offset >= len(items) {
		return []*channels.Binding{}, "", nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = fmt.Sprintf("%d", end)
	}
	return items[offset:end], next, nil
}

func decodeListCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	var offset int
	if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return offset, nil
}

// NewRepository creates a Binding repository and candidate index.
func NewRepository(db *sql.DB) *ChannelRepository {
	return &ChannelRepository{db: db, candidates: make(map[string]candidateRecord)}
}

// Create persists a channel binding.
func (r *ChannelRepository) Create(ctx context.Context, input channels.CreateInput) (*channels.Binding, channels.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, channels.ChangeEvent{}, ErrStorage
	}
	value, err := channels.NewBinding(input)
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	event, err := channels.PrepareCreatedChange(*value, input.Metadata)
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	protocol, err := encodeProtocol(value.Protocol)
	if err != nil {
		return nil, channels.ChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `INSERT INTO channel_binding (
		tenant_id, binding_id, binding_key, channel, provider_account_id,
		public_route_key_digest, app_id, secret_ref, protocol_config, schema_version,
		status, version, config_digest, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.TenantID, value.BindingID,
		value.BindingKey, string(value.Channel), value.ProviderAccountID, value.PublicRouteKeyDigest,
		value.AppID, value.SecretRef, protocol, channels.SchemaVersionV1, string(value.Status), value.Version,
		value.ConfigDigest, value.CreatedAt, value.UpdatedAt); err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO channel_binding_change_outbox (
		event_type, tenant_id, binding_id, previous_status, current_status, previous_digest,
		current_digest, actor_type, actor_id, reason, correlation_id, previous_version,
		next_version, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(event.EventType), event.TenantID,
		event.BindingID, nullableText(string(event.PreviousStatus)), string(event.CurrentStatus),
		nullableText(event.PreviousDigest), event.CurrentDigest, event.ActorType, event.ActorID,
		event.Reason, event.CorrelationID, event.PreviousVersion, event.NextVersion, event.OccurredAt)
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	var eventID int64
	eventID, err = eventResult.LastInsertId()
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	stored, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = ? AND binding_id = ?`, value.TenantID, value.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	committed, err := scanChannelEvent(tx.QueryRowContext(ctx, channelEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	return stored, committed, nil
}

// Get loads a channel binding within a tenant.
func (r *ChannelRepository) Get(ctx context.Context, tenantID, bindingID string) (*channels.Binding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	value, err := scanChannelBinding(r.db.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = ? AND binding_id = ?`, tenantID, bindingID))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", channels.ErrNotFound, bindingID)
		}
		return nil, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	return value, nil
}

// UpdateConfiguration applies an expected-version binding update.
func (r *ChannelRepository) UpdateConfiguration(ctx context.Context, input channels.UpdateConfigurationInput) (*channels.Binding, channels.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, channels.ChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = ? AND binding_id = ? FOR UPDATE`, input.TenantID, input.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	updated, event, err := channels.PrepareConfigurationChange(*current, input, monotonicNow(current.UpdatedAt))
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	protocol, err := encodeProtocol(updated.Protocol)
	if err != nil {
		return nil, channels.ChangeEvent{}, ErrStorage
	}
	result, err := tx.ExecContext(ctx, `UPDATE channel_binding SET provider_account_id = ?,
		public_route_key_digest = ?, app_id = ?, secret_ref = ?, protocol_config = ?,
		schema_version = ?, config_digest = ?, version = version + 1, updated_at = ?
		WHERE tenant_id = ? AND binding_id = ? AND version = ?`, updated.ProviderAccountID,
		updated.PublicRouteKeyDigest, updated.AppID, updated.SecretRef, protocol, channels.SchemaVersionV1,
		updated.ConfigDigest, updated.UpdatedAt, updated.TenantID, updated.BindingID, input.ExpectedVersion)
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, channels.ChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", channels.ErrConflict, input.ExpectedVersion, current.Version)
	}
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO channel_binding_change_outbox (
		event_type, tenant_id, binding_id, previous_status, current_status, previous_digest,
		current_digest, actor_type, actor_id, reason, correlation_id, previous_version,
		next_version, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(event.EventType), event.TenantID,
		event.BindingID, nullableText(string(event.PreviousStatus)), string(event.CurrentStatus),
		nullableText(event.PreviousDigest), event.CurrentDigest, event.ActorType, event.ActorID,
		event.Reason, event.CorrelationID, event.PreviousVersion, event.NextVersion, event.OccurredAt)
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	var eventID int64
	eventID, err = eventResult.LastInsertId()
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	stored, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = ? AND binding_id = ?`, input.TenantID, input.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	committed, err := scanChannelEvent(tx.QueryRowContext(ctx, channelEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	return stored, committed, nil
}

// TransitionStatus changes a binding status with optimistic concurrency.
func (r *ChannelRepository) TransitionStatus(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, channels.ChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = ? AND binding_id = ? FOR UPDATE`, input.TenantID, input.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	updated, event, err := channels.PrepareStatusChange(*current, input, monotonicNow(current.UpdatedAt))
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE channel_binding SET status = ?, version = version + 1, updated_at = ?
		WHERE tenant_id = ? AND binding_id = ? AND version = ?`, string(updated.Status), updated.UpdatedAt,
		input.TenantID, input.BindingID, input.ExpectedVersion)
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, channels.ChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", channels.ErrConflict, input.ExpectedVersion, current.Version)
	}
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO channel_binding_change_outbox (
		event_type, tenant_id, binding_id, previous_status, current_status, previous_digest,
		current_digest, actor_type, actor_id, reason, correlation_id, previous_version,
		next_version, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(event.EventType), event.TenantID,
		event.BindingID, nullableText(string(event.PreviousStatus)), string(event.CurrentStatus),
		nullableText(event.PreviousDigest), event.CurrentDigest, event.ActorType, event.ActorID,
		event.Reason, event.CorrelationID, event.PreviousVersion, event.NextVersion, event.OccurredAt)
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	var eventID int64
	eventID, err = eventResult.LastInsertId()
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	stored, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = ? AND binding_id = ?`, input.TenantID, input.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	committed, err := scanChannelEvent(tx.QueryRowContext(ctx, channelEventSelect+` WHERE event_id = ?`, eventID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	return stored, committed, nil
}

// Activate enables a channel binding.
func (r *ChannelRepository) Activate(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	input.NextStatus = channels.StatusActive
	return r.TransitionStatus(ctx, input)
}

// Suspend pauses a channel binding.
func (r *ChannelRepository) Suspend(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	input.NextStatus = channels.StatusSuspended
	return r.TransitionStatus(ctx, input)
}

// Resume re-enables a suspended channel binding.
func (r *ChannelRepository) Resume(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	input.NextStatus = channels.StatusActive
	return r.TransitionStatus(ctx, input)
}

// Disable permanently disables a channel binding.
func (r *ChannelRepository) Disable(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	input.NextStatus = channels.StatusDisabled
	return r.TransitionStatus(ctx, input)
}

// LookupCandidates finds tenant bindings matching a verified route.
func (r *ChannelRepository) LookupCandidates(ctx context.Context, channel channels.Channel, routeDigest string) ([]channels.CandidateBindingContext, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if channel.Validate() != nil || channels.ValidatePublicRouteKeyDigest(routeDigest) != nil {
		return nil, channels.ErrCandidateUnavailable
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT tenant_id, binding_id, version, config_digest
		FROM channel_binding
		WHERE channel = ? AND public_route_key_digest = ? AND status = 'active'
		ORDER BY tenant_id, binding_id
	`, string(channel), routeDigest)
	if err != nil {
		return nil, mapDBError(ctx, err, channels.ErrCandidateUnavailable, channels.ErrCandidateUnavailable, channels.ErrCandidateUnavailable, channels.ErrCandidateUnavailable)
	}
	defer func() { _ = rows.Close() }()
	type rowValue struct {
		tenantID, bindingID string
		version             int64
		digest              string
	}
	var values []rowValue
	for rows.Next() {
		var value rowValue
		if err := rows.Scan(&value.tenantID, &value.bindingID, &value.version, &value.digest); err != nil {
			return nil, channels.ErrCandidateUnavailable
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, channels.ErrCandidateUnavailable
	}
	if len(values) == 0 {
		return nil, channels.ErrCandidateUnavailable
	}
	now := time.Now().UTC()
	if len(values)+r.candidateCount() > mysqlMaxCandidates {
		return nil, channels.ErrCandidateUnavailable
	}
	contexts := make([]channels.CandidateBindingContext, 0, len(values))
	records := make([]candidateRecord, 0, len(values))
	for _, value := range values {
		token, err := newCandidateToken()
		if err != nil {
			return nil, channels.ErrCandidateUnavailable
		}
		candidate, err := channels.NewCandidateBindingContextFromInput(channels.CandidateBindingInput{
			Channel: channel, PublicRouteKeyDigest: routeDigest, BindingVersion: value.version,
			ConfigDigest: value.digest, Purpose: channels.PurposeWebhookVerification,
			CandidateToken: token, IssuedAt: now, ExpiresAt: now.Add(mysqlCandidateTTL),
		})
		if err != nil {
			return nil, channels.ErrCandidateUnavailable
		}
		contexts = append(contexts, candidate.Clone())
		records = append(records, candidateRecord{tenantID: value.tenantID, bindingID: value.bindingID, context: candidate})
	}
	r.candidateMu.Lock()
	for _, record := range records {
		r.candidates[record.context.CandidateToken] = record
	}
	r.candidateMu.Unlock()
	return contexts, nil
}

// ConsumeCandidate atomically consumes a one-time verified candidate.
func (r *ChannelRepository) ConsumeCandidate(ctx context.Context, candidate channels.CandidateBindingContext) (*channels.Binding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	if candidate.Validate(time.Time{}) != nil {
		return nil, channels.ErrCandidateUnavailable
	}
	r.candidateMu.Lock()
	record, ok := r.candidates[candidate.CandidateToken]
	if ok && !sameCandidate(record.context, candidate) {
		ok = false
	}
	if ok {
		delete(r.candidates, candidate.CandidateToken)
	}
	r.candidateMu.Unlock()
	if !ok || !time.Now().UTC().Before(record.context.ExpiresAt) {
		return nil, channels.ErrCandidateUnavailable
	}
	binding, err := r.Get(ctx, record.tenantID, record.bindingID)
	if err != nil {
		return nil, channels.ErrCandidateUnavailable
	}
	if binding.Status != channels.StatusActive || binding.Version != record.context.BindingVersion ||
		binding.ConfigDigest != record.context.ConfigDigest || binding.Channel != record.context.Channel ||
		binding.PublicRouteKeyDigest != record.context.PublicRouteKeyDigest {
		return nil, channels.ErrCandidateUnavailable
	}
	return binding, nil
}

const channelSelect = `SELECT tenant_id, binding_id, binding_key, channel,
       provider_account_id, public_route_key_digest, app_id, secret_ref,
       protocol_config, schema_version, status, version, config_digest,
       created_at, updated_at FROM channel_binding`

func scanChannelBinding(row rowScanner) (*channels.Binding, error) {
	var value channels.Binding
	var channel, status string
	var schemaVersion int
	var protocol []byte
	if err := row.Scan(&value.TenantID, &value.BindingID, &value.BindingKey, &channel,
		&value.ProviderAccountID, &value.PublicRouteKeyDigest, &value.AppID, &value.SecretRef,
		&protocol, &schemaVersion, &status, &value.Version, &value.ConfigDigest,
		&value.CreatedAt, &value.UpdatedAt); err != nil {
		return nil, err
	}
	value.Channel = channels.Channel(channel)
	value.Status = channels.Status(status)
	if schemaVersion != channels.SchemaVersionV1 {
		return nil, ErrStorage
	}
	if err := decodeProtocol(protocol, &value.Protocol); err != nil {
		return nil, ErrStorage
	}
	value.CreatedAt = asUTC(value.CreatedAt)
	value.UpdatedAt = asUTC(value.UpdatedAt)
	if err := value.Validate(); err != nil {
		return nil, ErrStorage
	}
	clone := value.Clone()
	return &clone, nil
}

const channelEventSelect = `SELECT event_type, tenant_id, binding_id,
       previous_status, current_status, previous_digest, current_digest,
       actor_type, actor_id, reason, correlation_id, previous_version,
       next_version, occurred_at FROM channel_binding_change_outbox`

func scanChannelEvent(row rowScanner) (channels.ChangeEvent, error) {
	var event channels.ChangeEvent
	var eventType, currentStatus string
	var previousStatus, previousDigest sql.NullString
	if err := row.Scan(&eventType, &event.TenantID, &event.BindingID, &previousStatus,
		&currentStatus, &previousDigest, &event.CurrentDigest, &event.ActorType,
		&event.ActorID, &event.Reason, &event.CorrelationID, &event.PreviousVersion,
		&event.NextVersion, &event.OccurredAt); err != nil {
		return channels.ChangeEvent{}, err
	}
	event.EventType = channels.EventType(eventType)
	if previousStatus.Valid {
		event.PreviousStatus = channels.Status(previousStatus.String)
	}
	event.CurrentStatus = channels.Status(currentStatus)
	if previousDigest.Valid {
		event.PreviousDigest = previousDigest.String
	}
	event.OccurredAt = asUTC(event.OccurredAt)
	return event, nil
}

func (r *ChannelRepository) candidateCount() int {
	r.candidateMu.Lock()
	defer r.candidateMu.Unlock()
	now := time.Now().UTC()
	for token, record := range r.candidates {
		if !now.Before(record.context.ExpiresAt) {
			delete(r.candidates, token)
		}
	}
	return len(r.candidates)
}

func newCandidateToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func sameCandidate(left, right channels.CandidateBindingContext) bool {
	return left.Channel == right.Channel && left.PublicRouteKeyDigest == right.PublicRouteKeyDigest &&
		left.BindingVersion == right.BindingVersion && left.ConfigDigest == right.ConfigDigest &&
		left.Purpose == right.Purpose && left.CandidateToken == right.CandidateToken &&
		left.IssuedAt.Equal(right.IssuedAt) && left.ExpiresAt.Equal(right.ExpiresAt)
}
