// Package inmemory provides a concurrent, non-durable Channel Binding
// Repository and candidate index for development and offline tests.
package inmemory

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

// List returns a stable page of channel bindings in one tenant.
func (r *InMemoryRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*channels.Binding, string, error) {
	if err := checkContext(ctx); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if err := r.rlock(ctx); err != nil {
		return nil, "", err
	}
	defer r.runlock()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*channels.Binding, 0)
	for scope, value := range r.byID {
		if scope.tenantID != tenantID || (status != "" && string(value.Status) != status) {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.BindingID+" "+value.BindingKey+" "+string(value.Channel)+" "+value.ProviderAccountID), query) {
			continue
		}
		items = append(items, cloneBinding(value))
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

func decodeCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	var offset int
	if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return offset, nil
}

const defaultCandidateTTL = channels.DefaultCandidateTTL

// DefaultMaxCandidates bounds retained public candidate capabilities in the
// local repository. Production indexes must apply their own capacity policy.
const DefaultMaxCandidates = 4096

type bindingScope struct {
	tenantID  string
	bindingID string
}

type bindingKeyScope struct {
	tenantID   string
	bindingKey string
}

type routeScope struct {
	channel channels.Channel
	digest  string
}

type accountScope struct {
	channel           channels.Channel
	providerAccountID string
}

type candidateRecord struct {
	scope   bindingScope
	context channels.CandidateBindingContext
}

// Options configures the local candidate lifetime and test clock.
type Options struct {
	CandidateTTL  time.Duration
	Clock         func() time.Time
	MaxCandidates int
}

// InMemoryRepository is a single-process Repository and CandidateIndex. It
// does not provide durability or cross-node consistency.
type InMemoryRepository struct {
	mu             contextRWMutex
	clock          func() time.Time
	candidateTTL   time.Duration
	maxCandidates  int
	byID           map[bindingScope]*channels.Binding
	byKey          map[bindingKeyScope]string
	routeIndex     map[routeScope]map[bindingScope]struct{}
	activeAccounts map[accountScope]bindingScope
	candidates     map[string]candidateRecord
}

// NewInMemoryRepository creates an empty repository. A zero Options value uses
// a 30-second candidate TTL, the default candidate capacity, and the UTC wall
// clock. Options are applied in order; later positive values and non-nil clocks
// override earlier ones. Zero and negative values leave the defaults unchanged.
func NewInMemoryRepository(options ...Options) *InMemoryRepository {
	configuration := Options{CandidateTTL: defaultCandidateTTL, Clock: func() time.Time { return time.Now().UTC() }, MaxCandidates: DefaultMaxCandidates}
	for _, option := range options {
		if option.CandidateTTL > 0 {
			configuration.CandidateTTL = option.CandidateTTL
		}
		if option.Clock != nil {
			configuration.Clock = option.Clock
		}
		if option.MaxCandidates > 0 {
			configuration.MaxCandidates = option.MaxCandidates
		}
	}
	if configuration.CandidateTTL > channels.MaxCandidateLifetime {
		configuration.CandidateTTL = channels.MaxCandidateLifetime
	}
	return &InMemoryRepository{
		clock: configuration.Clock, candidateTTL: configuration.CandidateTTL, maxCandidates: configuration.MaxCandidates,
		byID: make(map[bindingScope]*channels.Binding), byKey: make(map[bindingKeyScope]string),
		routeIndex:     make(map[routeScope]map[bindingScope]struct{}),
		activeAccounts: make(map[accountScope]bindingScope), candidates: make(map[string]candidateRecord),
	}
}

// NewRepository is the concise constructor for the InMemory implementation.
func NewRepository(options ...Options) *InMemoryRepository { return NewInMemoryRepository(options...) }

var _ channels.Repository = (*InMemoryRepository)(nil)
var _ channels.CandidateIndex = (*InMemoryRepository)(nil)

// Create validates and atomically stores a Binding and its creation event.
func (r *InMemoryRepository) Create(ctx context.Context, input channels.CreateInput) (*channels.Binding, channels.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	binding, err := channels.NewBindingAt(input, r.nowUTC())
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	event, err := channels.PrepareCreatedChange(*binding, input.Metadata)
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	scope := bindingScope{tenantID: binding.TenantID, bindingID: binding.BindingID}
	if _, exists := r.byID[scope]; exists {
		return nil, channels.ChangeEvent{}, fmt.Errorf("%w: generated binding identity collision", channels.ErrDuplicateKey)
	}
	keyScope := bindingKeyScope{tenantID: binding.TenantID, bindingKey: binding.BindingKey}
	if _, exists := r.byKey[keyScope]; exists {
		return nil, channels.ChangeEvent{}, channels.ErrDuplicateKey
	}
	if binding.Status == channels.StatusActive && r.activeAccountInUseLocked(*binding, scope) {
		return nil, channels.ChangeEvent{}, fmt.Errorf("%w: active provider account already belongs to a binding", channels.ErrDuplicateKey)
	}
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	stored := binding.Clone()
	r.byID[scope] = &stored
	r.byKey[keyScope] = binding.BindingID
	r.addIndexesLocked(stored)
	return cloneBinding(binding), event, nil
}

// Get returns a defensive copy scoped by both tenant and Binding identity.
func (r *InMemoryRepository) Get(ctx context.Context, tenantID, bindingID string) (*channels.Binding, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.rlock(ctx); err != nil {
		return nil, err
	}
	defer r.runlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	binding, exists := r.byID[bindingScope{tenantID: tenantID, bindingID: bindingID}]
	if !exists {
		return nil, channels.ErrNotFound
	}
	return cloneBinding(binding), nil
}

// UpdateConfiguration atomically replaces mutable configuration and emits an
// event. A changed route digest invalidates future candidates through version
// and digest matching at consumption time.
func (r *InMemoryRepository) UpdateConfiguration(ctx context.Context, input channels.UpdateConfigurationInput) (*channels.Binding, channels.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	scope := bindingScope{tenantID: input.TenantID, bindingID: input.BindingID}
	current, exists := r.byID[scope]
	if !exists {
		return nil, channels.ChangeEvent{}, channels.ErrNotFound
	}
	updated, event, err := channels.PrepareConfigurationChange(*current, input, r.nowUTC())
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	if updated.Status == channels.StatusActive && r.activeAccountInUseLocked(updated, scope) {
		return nil, channels.ChangeEvent{}, fmt.Errorf("%w: active provider account already belongs to a binding", channels.ErrDuplicateKey)
	}
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	r.removeIndexesLocked(*current)
	stored := updated.Clone()
	r.byID[scope] = &stored
	r.addIndexesLocked(stored)
	return cloneBinding(&updated), event, nil
}

// TransitionStatus atomically applies a lifecycle transition and emits an
// event. Activating an account enforces the global active account invariant.
func (r *InMemoryRepository) TransitionStatus(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	scope := bindingScope{tenantID: input.TenantID, bindingID: input.BindingID}
	current, exists := r.byID[scope]
	if !exists {
		return nil, channels.ChangeEvent{}, channels.ErrNotFound
	}
	updated, event, err := channels.PrepareStatusChange(*current, input, r.nowUTC())
	if err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	if updated.Status == channels.StatusActive && r.activeAccountInUseLocked(updated, scope) {
		return nil, channels.ChangeEvent{}, fmt.Errorf("%w: active provider account already belongs to a binding", channels.ErrDuplicateKey)
	}
	if err := checkContext(ctx); err != nil {
		return nil, channels.ChangeEvent{}, err
	}
	r.removeIndexesLocked(*current)
	stored := updated.Clone()
	r.byID[scope] = &stored
	r.addIndexesLocked(stored)
	return cloneBinding(&updated), event, nil
}

// Activate moves a draft or suspended Binding into the active candidate index.
func (r *InMemoryRepository) Activate(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	input.NextStatus = channels.StatusActive
	return r.TransitionStatus(ctx, input)
}

// Suspend removes an active Binding from the candidate index while retaining
// its configuration.
func (r *InMemoryRepository) Suspend(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	input.NextStatus = channels.StatusSuspended
	return r.TransitionStatus(ctx, input)
}

// Resume activates a suspended Binding after the expected-version check.
func (r *InMemoryRepository) Resume(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	input.NextStatus = channels.StatusActive
	return r.TransitionStatus(ctx, input)
}

// Disable moves a Binding to the terminal state and removes all inbound
// candidate indexes.
func (r *InMemoryRepository) Disable(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	input.NextStatus = channels.StatusDisabled
	return r.TransitionStatus(ctx, input)
}

// LookupCandidates discovers active candidates by channel and route digest.
// It returns only opaque contexts and uses one generic unavailable error for
// missing, invalid, or inactive routes.
func (r *InMemoryRepository) LookupCandidates(ctx context.Context, channel channels.Channel, routeDigest string) ([]channels.CandidateBindingContext, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if channel.Validate() != nil || channels.ValidatePublicRouteKeyDigest(routeDigest) != nil {
		return nil, channels.ErrCandidateUnavailable
	}
	if err := r.lock(ctx); err != nil {
		return nil, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	now := r.nowUTC()
	r.pruneCandidatesLocked(now)
	scopes := make([]bindingScope, 0)
	for scope := range r.routeIndex[routeScope{channel: channel, digest: routeDigest}] {
		binding, exists := r.byID[scope]
		if exists && binding.Status == channels.StatusActive {
			scopes = append(scopes, scope)
		}
	}
	if len(scopes) == 0 {
		return nil, channels.ErrCandidateUnavailable
	}
	if len(scopes) > r.maxCandidates-len(r.candidates) {
		return nil, channels.ErrCandidateUnavailable
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].tenantID == scopes[j].tenantID {
			return scopes[i].bindingID < scopes[j].bindingID
		}
		return scopes[i].tenantID < scopes[j].tenantID
	})
	contexts := make([]channels.CandidateBindingContext, 0, len(scopes))
	records := make([]candidateRecord, 0, len(scopes))
	for _, scope := range scopes {
		binding := r.byID[scope]
		token, err := newCandidateToken()
		if err != nil {
			return nil, channels.ErrCandidateUnavailable
		}
		candidate, err := channels.NewCandidateBindingContextFromInput(channels.CandidateBindingInput{
			Channel: channel, PublicRouteKeyDigest: routeDigest, BindingVersion: binding.Version,
			ConfigDigest: binding.ConfigDigest, Purpose: channels.PurposeWebhookVerification,
			CandidateToken: token, IssuedAt: now, ExpiresAt: now.Add(r.candidateTTL),
		})
		if err != nil {
			return nil, channels.ErrCandidateUnavailable
		}
		contexts = append(contexts, candidate.Clone())
		records = append(records, candidateRecord{scope: scope, context: candidate})
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	for _, record := range records {
		r.candidates[record.context.CandidateToken] = record
	}
	return contexts, nil
}

// ConsumeCandidate is the trusted handoff used by a resolver implementation.
// It consumes exactly one opaque candidate token and only then returns the
// internal Binding copy to that resolver. Public callers should use
// CandidateIndex.LookupCandidates instead.
func (r *InMemoryRepository) ConsumeCandidate(ctx context.Context, candidate channels.CandidateBindingContext) (*channels.Binding, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if candidate.CandidateToken == "" {
		return nil, channels.ErrCandidateUnavailable
	}
	if candidate.Validate(time.Time{}) != nil {
		return nil, channels.ErrCandidateUnavailable
	}
	if err := r.lock(ctx); err != nil {
		return nil, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	record, exists := r.candidates[candidate.CandidateToken]
	if !exists || !sameCandidate(record.context, candidate) {
		return nil, channels.ErrCandidateUnavailable
	}
	now := r.nowUTC()
	if !now.Before(record.context.ExpiresAt) || now.Before(record.context.IssuedAt) {
		delete(r.candidates, candidate.CandidateToken)
		return nil, channels.ErrCandidateUnavailable
	}
	binding, exists := r.byID[record.scope]
	if !exists || binding.Status != channels.StatusActive || binding.Version != record.context.BindingVersion || binding.ConfigDigest != record.context.ConfigDigest || binding.Channel != record.context.Channel || binding.PublicRouteKeyDigest != record.context.PublicRouteKeyDigest {
		delete(r.candidates, candidate.CandidateToken)
		return nil, channels.ErrCandidateUnavailable
	}
	delete(r.candidates, candidate.CandidateToken)
	return cloneBinding(binding), nil
}

func (r *InMemoryRepository) nowUTC() time.Time {
	now := r.clock()
	if now.IsZero() {
		now = time.Now()
	}
	return now.UTC()
}

func (r *InMemoryRepository) addIndexesLocked(binding channels.Binding) {
	if binding.Status != channels.StatusActive {
		return
	}
	scope := bindingScope{tenantID: binding.TenantID, bindingID: binding.BindingID}
	account := accountScope{channel: binding.Channel, providerAccountID: binding.ProviderAccountID}
	r.activeAccounts[account] = scope
	route := routeScope{channel: binding.Channel, digest: binding.PublicRouteKeyDigest}
	byRoute := r.routeIndex[route]
	if byRoute == nil {
		byRoute = make(map[bindingScope]struct{})
		r.routeIndex[route] = byRoute
	}
	byRoute[scope] = struct{}{}
}

func (r *InMemoryRepository) removeIndexesLocked(binding channels.Binding) {
	if binding.Status != channels.StatusActive {
		return
	}
	scope := bindingScope{tenantID: binding.TenantID, bindingID: binding.BindingID}
	account := accountScope{channel: binding.Channel, providerAccountID: binding.ProviderAccountID}
	if owner, exists := r.activeAccounts[account]; exists && owner == scope {
		delete(r.activeAccounts, account)
	}
	route := routeScope{channel: binding.Channel, digest: binding.PublicRouteKeyDigest}
	byRoute := r.routeIndex[route]
	delete(byRoute, scope)
	if len(byRoute) == 0 {
		delete(r.routeIndex, route)
	}
}

func (r *InMemoryRepository) activeAccountInUseLocked(binding channels.Binding, except bindingScope) bool {
	owner, exists := r.activeAccounts[accountScope{channel: binding.Channel, providerAccountID: binding.ProviderAccountID}]
	return exists && owner != except
}

func (r *InMemoryRepository) pruneCandidatesLocked(now time.Time) {
	for token, record := range r.candidates {
		if !now.Before(record.context.ExpiresAt) {
			delete(r.candidates, token)
		}
	}
}

func sameCandidate(left, right channels.CandidateBindingContext) bool {
	return left.Channel == right.Channel && left.PublicRouteKeyDigest == right.PublicRouteKeyDigest && left.BindingVersion == right.BindingVersion && left.ConfigDigest == right.ConfigDigest && left.Purpose == right.Purpose && left.CandidateToken == right.CandidateToken && left.IssuedAt.Equal(right.IssuedAt) && left.ExpiresAt.Equal(right.ExpiresAt)
}

func newCandidateToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func cloneBinding(binding *channels.Binding) *channels.Binding {
	if binding == nil {
		return nil
	}
	clone := binding.Clone()
	return &clone
}

func (r *InMemoryRepository) lock(ctx context.Context) error  { return r.mu.lock(ctx) }
func (r *InMemoryRepository) unlock()                         { r.mu.unlock() }
func (r *InMemoryRepository) rlock(ctx context.Context) error { return r.mu.rlock(ctx) }
func (r *InMemoryRepository) runlock()                        { r.mu.runlock() }
