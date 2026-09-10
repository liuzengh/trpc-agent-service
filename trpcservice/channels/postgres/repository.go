// Package postgres provides the PostgreSQL implementation of the Channel
// Binding repository and candidate capability index.
package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

// List returns a stable page of Channel Bindings belonging to one tenant.
func (r *ChannelRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*channels.Binding, string, error) {
	if r == nil || r.db == nil {
		return nil, "", ErrStorage
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset := 0
	if cursor != "" {
		if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
			return nil, "", fmt.Errorf("invalid cursor")
		}
	}
	rows, err := r.db.QueryContext(ctx, `SELECT binding_id FROM public.channel_binding WHERE tenant_id=$1 ORDER BY binding_id`, tenantID)
	if err != nil {
		return nil, "", ErrStorage
	}
	bindingIDs, err := scanBindingIDs(rows)
	if err != nil {
		return nil, "", ErrStorage
	}
	items := make([]*channels.Binding, 0)
	q := strings.ToLower(strings.TrimSpace(query))
	for _, id := range bindingIDs {
		value, err := r.Get(ctx, tenantID, id)
		if err != nil {
			return nil, "", ErrStorage
		}
		if status != "" && string(value.Status) != status {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(value.BindingID+" "+value.BindingKey+" "+string(value.Channel)+" "+value.ProviderAccountID), q) {
			continue
		}
		items = append(items, value)
	}
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

func scanBindingIDs(rows *sql.Rows) ([]string, error) {
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	return ids, rows.Close()
}

const (
	postgresCandidateTTL  = channels.DefaultCandidateTTL
	postgresMaxCandidates = 4096
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
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_create_channel_binding(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
		)
	`, value.TenantID, value.BindingID, value.BindingKey, string(value.Channel),
		value.ProviderAccountID, value.PublicRouteKeyDigest, value.AppID, value.SecretRef,
		protocol, channels.SchemaVersionV1, string(value.Status), value.Version, value.ConfigDigest,
		value.CreatedAt, value.UpdatedAt, event.EventType, event.PreviousStatus,
		event.CurrentStatus, event.PreviousDigest, event.CurrentDigest, event.ActorType,
		event.ActorID, event.Reason, event.CorrelationID).Scan(&eventID)
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	stored, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = $1 AND binding_id = $2`, value.TenantID, value.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	committed, err := scanChannelEvent(tx.QueryRowContext(ctx, channelEventSelect+` WHERE event_id = $1`, eventID))
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
	value, err := scanChannelBinding(r.db.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = $1 AND binding_id = $2`, tenantID, bindingID))
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
	current, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = $1 AND binding_id = $2 FOR UPDATE`, input.TenantID, input.BindingID))
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
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_update_channel_binding(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19, $20, $21
		)
	`, updated.TenantID, updated.BindingID, input.ExpectedVersion, updated.ProviderAccountID,
		updated.PublicRouteKeyDigest, updated.AppID, updated.SecretRef, protocol,
		channels.SchemaVersionV1, updated.ConfigDigest, updated.UpdatedAt, event.EventType,
		event.PreviousStatus, event.CurrentStatus, event.PreviousDigest, event.CurrentDigest,
		event.ActorType, event.ActorID, event.Reason, event.CorrelationID, updated.Channel).Scan(&eventID)
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	stored, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = $1 AND binding_id = $2`, input.TenantID, input.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	committed, err := scanChannelEvent(tx.QueryRowContext(ctx, channelEventSelect+` WHERE event_id = $1`, eventID))
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
	current, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = $1 AND binding_id = $2 FOR UPDATE`, input.TenantID, input.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	_, _, err = channels.PrepareStatusChange(*current, input, monotonicNow(current.UpdatedAt))
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_transition_channel_binding_status(
			$1, $2, $3, $4, $5, $6, $7, $8
		)
	`, input.TenantID, input.BindingID, input.ExpectedVersion, string(input.NextStatus),
		strings.TrimSpace(input.Metadata.ActorType), strings.TrimSpace(input.Metadata.ActorID),
		strings.TrimSpace(input.Metadata.Reason), strings.TrimSpace(input.Metadata.CorrelationID)).Scan(&eventID)
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	stored, err := scanChannelBinding(tx.QueryRowContext(ctx, channelSelect+` WHERE tenant_id = $1 AND binding_id = $2`, input.TenantID, input.BindingID))
	if err != nil {
		return nil, channels.ChangeEvent{}, mapDBError(ctx, err, channels.ErrNotFound, channels.ErrDuplicateKey, channels.ErrConflict, channels.ErrInvalid)
	}
	committed, err := scanChannelEvent(tx.QueryRowContext(ctx, channelEventSelect+` WHERE event_id = $1`, eventID))
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
		FROM public.channel_binding
		WHERE channel = $1 AND public_route_key_digest = $2 AND status = 'active'
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
	if len(values)+r.candidateCount() > postgresMaxCandidates {
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
			CandidateToken: token, IssuedAt: now, ExpiresAt: now.Add(postgresCandidateTTL),
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
       created_at, updated_at FROM public.channel_binding`

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
       next_version, occurred_at FROM public.channel_binding_change_outbox`

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
