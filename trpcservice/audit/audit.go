// Package audit defines tenant-scoped business audit and usage contracts.
// It deliberately has no dependency on a telemetry or database provider.
package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

// SchemaVersion identifies the audit event schema.
const SchemaVersion = 1

// Audit errors describe invalid, out-of-scope, missing, or conflicting records.
var (
	// ErrInvalid reports malformed audit input.
	ErrInvalid     = errors.New("invalid audit event")
	ErrTenantScope = errors.New("audit tenant scope violation")
	ErrNotFound    = errors.New("audit event not found")
	ErrConflict    = errors.New("audit event conflict")
)

// EventType identifies the audited business operation.
type EventType string

// Event type constants identify supported audit records.
const (
	// EventControlPlaneChanged records a control-plane mutation.
	EventControlPlaneChanged EventType = "control_plane.changed"
	// EventExecutionStarted records admission into runner execution.
	EventExecutionStarted EventType = "execution.started"
	// EventExecutionCompleted records successful execution completion.
	EventExecutionCompleted EventType = "execution.completed"
	// EventExecutionFailed records terminal execution failure.
	EventExecutionFailed EventType = "execution.failed"
	// EventExecutionCanceled records caller cancellation.
	EventExecutionCanceled EventType = "execution.canceled"
	// EventExecutionTimedOut records an execution deadline expiring.
	EventExecutionTimedOut EventType = "execution.timed_out"
	// EventExecutionFallback records use of a fallback provider.
	EventExecutionFallback EventType = "execution.fallback"
	// EventCanarySelected records that the App's tenant-wide candidate revision
	// was selected for one execution before the execution-started fact.
	EventCanarySelected EventType = "execution.canary_selected"
	// EventToolAllowed records permission to invoke a tool.
	EventToolAllowed EventType = "tool.allowed"
	// EventToolDenied records a rejected tool invocation.
	EventToolDenied EventType = "tool.denied"
	// EventToolApprovalRequired records a tool invocation awaiting approval.
	EventToolApprovalRequired EventType = "tool.approval_required"
	// EventToolExecuted records a completed tool invocation.
	EventToolExecuted EventType = "tool.executed"
	// EventIMAuthorizationAllowed records an authorized channel message.
	EventIMAuthorizationAllowed EventType = "im.authorization_allowed"
	// EventIMAuthorizationDenied records a rejected channel message.
	EventIMAuthorizationDenied EventType = "im.authorization_denied"
	// EventIMIngressAccepted records channel message admission.
	EventIMIngressAccepted EventType = "im.ingress_accepted"
	// EventIMIngressDuplicate records suppression of duplicate channel input.
	EventIMIngressDuplicate EventType = "im.ingress_duplicate"
	// EventIMDeliverySent records successful reply delivery.
	EventIMDeliverySent EventType = "im.delivery_sent"
	// EventIMDeliveryRetryScheduled records a deferred delivery retry.
	EventIMDeliveryRetryScheduled EventType = "im.delivery_retry_scheduled"
	// EventIMDeliveryDeadLettered records terminal reply delivery failure.
	EventIMDeliveryDeadLettered EventType = "im.delivery_dead_lettered"
	// EventIMDeliveryReconciled records a provider delivery status check.
	EventIMDeliveryReconciled EventType = "im.delivery_reconciled"
	// EventBudgetRejected records admission denied by a tenant budget.
	EventBudgetRejected EventType = "budget.rejected"
	// EventContentRedacted records content removed by redaction policy.
	EventContentRedacted EventType = "content.redacted"
	// EventAuditIncomplete records an incomplete audit lifecycle.
	EventAuditIncomplete EventType = "audit_incomplete"
)

// Decision records the outcome of an authorization decision.
type Decision string

// Decision constants identify authorization outcomes.
const (
	// DecisionAllow permits the requested operation.
	DecisionAllow            Decision = "allow"
	DecisionDeny             Decision = "deny"
	DecisionApprovalRequired Decision = "approval_required"
	DecisionAccepted         Decision = "accepted"
	DecisionDuplicate        Decision = "duplicate"
	DecisionRejected         Decision = "rejected"
)

// ExecutionResult records the execution outcome.
type ExecutionResult string

// Execution result constants identify terminal outcomes.
const (
	// ResultSuccess indicates successful execution.
	ResultSuccess  ExecutionResult = "success"
	ResultFailure  ExecutionResult = "failure"
	ResultCanceled ExecutionResult = "canceled"
	ResultTimeout  ExecutionResult = "timeout"
	ResultRejected ExecutionResult = "rejected"
)

// ErrorType classifies a failed operation.
type ErrorType string

// Error type constants identify failure classes.
const (
	// ErrorCanceled indicates cancellation.
	ErrorCanceled        ErrorType = "canceled"
	ErrorTimeout         ErrorType = "timeout"
	ErrorInvalid         ErrorType = "invalid"
	ErrorUnauthenticated ErrorType = "unauthenticated"
	ErrorRateLimited     ErrorType = "rate_limited"
	ErrorDuplicate       ErrorType = "duplicate"
	ErrorUnavailable     ErrorType = "unavailable"
	ErrorStorage         ErrorType = "storage"
	ErrorModel           ErrorType = "model"
	ErrorTool            ErrorType = "tool"
	ErrorProvider        ErrorType = "provider_error"
	ErrorBudget          ErrorType = "budget"
	ErrorRedacted        ErrorType = "redacted"
	ErrorConflict        ErrorType = "conflict"
)

// Usage contains bounded token, cost, and provider usage details.
type Usage struct {
	InputTokens      *int64
	OutputTokens     *int64
	ModelCostMinor   *int64
	ToolCostMinor    *int64
	Currency         string
	BudgetUsedTokens *int64
	BudgetUsedMinor  *int64
	ExecutionResult  ExecutionResult
	Provider         string
	Model            string
}

// Clone returns an isolated copy of the usage details.
func (u *Usage) Clone() *Usage {
	if u == nil {
		return nil
	}
	copy := *u
	copy.InputTokens = cloneInt64(u.InputTokens)
	copy.OutputTokens = cloneInt64(u.OutputTokens)
	copy.ModelCostMinor = cloneInt64(u.ModelCostMinor)
	copy.ToolCostMinor = cloneInt64(u.ToolCostMinor)
	copy.BudgetUsedTokens = cloneInt64(u.BudgetUsedTokens)
	copy.BudgetUsedMinor = cloneInt64(u.BudgetUsedMinor)
	return &copy
}

// Event is the immutable, tenant-scoped audit record.
type Event struct {
	SchemaVersion   int
	EventID         string
	EventType       EventType
	TenantID        string
	Channel         string
	UserID          string
	SessionID       string
	AgentAppID      string
	Revision        *int64
	ModelProfileID  string
	ToolName        string
	Decision        Decision
	LatencyMS       *int64
	ErrorType       string
	Cost            *Usage
	RequestID       string
	TraceID         string
	CorrelationID   string
	ActorType       string
	ActorID         string
	Reason          string
	PreviousVersion *int64
	NextVersion     *int64
	OccurredAt      time.Time
}

// Clone returns an isolated copy of the event.
func (e Event) Clone() Event {
	e.Cost = e.Cost.Clone()
	e.Revision = cloneInt64(e.Revision)
	e.LatencyMS = cloneInt64(e.LatencyMS)
	e.PreviousVersion = cloneInt64(e.PreviousVersion)
	e.NextVersion = cloneInt64(e.NextVersion)
	return e
}

// Validate checks the event contract and security boundaries.
func (e Event) Validate() error {
	if !e.validIdentity() {
		return ErrInvalid
	}
	if !e.validFields() || !e.validVersions() || !e.validControlPlaneMetadata() {
		return ErrInvalid
	}
	if !e.validMetadata() {
		return ErrInvalid
	}
	return nil
}

func (e Event) validIdentity() bool {
	return e.SchemaVersion == SchemaVersion && e.EventID == clean(e.EventID) && e.TenantID == clean(e.TenantID) && clean(e.EventID) != "" && clean(e.TenantID) != "" && validEventType(e.EventType) && !e.OccurredAt.IsZero() && e.OccurredAt.Location() == time.UTC
}

func (e Event) validFields() bool {
	for _, value := range []string{e.EventID, e.TenantID, e.Channel, e.UserID, e.SessionID, e.AgentAppID, e.ModelProfileID, e.ToolName, e.ErrorType, e.RequestID, e.TraceID, e.CorrelationID, e.ActorType, e.ActorID, e.Reason} {
		if hasControl(value) {
			return false
		}
	}
	return !(e.LatencyMS != nil && *e.LatencyMS < 0 || e.Revision != nil && *e.Revision < 0)
}

func (e Event) validVersions() bool {
	if (e.PreviousVersion == nil) != (e.NextVersion == nil) {
		return false
	}
	return e.PreviousVersion == nil || *e.PreviousVersion >= 0 && *e.PreviousVersion != math.MaxInt64 && *e.NextVersion == *e.PreviousVersion+1
}

func (e Event) validControlPlaneMetadata() bool {
	return e.EventType != EventControlPlaneChanged || clean(e.ActorType) != "" && clean(e.ActorID) != "" && clean(e.Reason) != "" && clean(e.CorrelationID) != "" && e.PreviousVersion != nil
}

func (e Event) validMetadata() bool {
	return len([]rune(strings.TrimSpace(e.Reason))) <= 1000 && (e.Decision == "" || validDecision(e.Decision)) && (e.ErrorType == "" || validErrorType(e.ErrorType)) && !containsSensitive(e.EventID, e.TenantID, e.Channel, e.UserID, e.SessionID, e.AgentAppID, e.ModelProfileID, e.ToolName, e.ErrorType, e.RequestID, e.TraceID, e.CorrelationID, e.ActorType, e.ActorID, e.Reason) && (e.Cost == nil || e.Cost.Validate() == nil)
}

// Validate checks usage values and bounded metadata.
func (u Usage) Validate() error {
	for _, value := range []*int64{u.InputTokens, u.OutputTokens, u.ModelCostMinor, u.ToolCostMinor, u.BudgetUsedTokens, u.BudgetUsedMinor} {
		if value != nil && *value < 0 {
			return ErrInvalid
		}
	}
	if (u.ModelCostMinor != nil || u.ToolCostMinor != nil || u.BudgetUsedMinor != nil) && !validCurrency(u.Currency) || u.ExecutionResult != "" && !validResult(u.ExecutionResult) {
		return ErrInvalid
	}
	for _, value := range []string{u.Currency, u.Provider, u.Model} {
		if hasControl(value) || strings.Contains(value, "://") || containsSensitive(value) {
			return ErrInvalid
		}
	}
	return nil
}

// Digest returns the stable SHA-256 digest of the validated event.
func (e Event) Digest() (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}
	b, err := json.Marshal(e.Clone())
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// AppendResult describes an append operation and its deduplication result.
type AppendResult struct {
	Event     Event
	Duplicate bool
	Digest    string
}

// Query selects audit events by type and time range.
type Query struct {
	EventTypes []EventType
	Since      time.Time
	Until      time.Time
}

// GroupBy selects a bounded usage aggregation dimension.
type GroupBy string

// Group constants identify supported aggregation dimensions.
const (
	GroupTenant   GroupBy = "tenant"
	GroupApp      GroupBy = "app"
	GroupChannel  GroupBy = "channel"
	GroupProvider GroupBy = "provider"
	GroupModel    GroupBy = "model"
)

// UsageQuery selects usage totals and their grouping dimensions.
type UsageQuery struct {
	Query   Query
	GroupBy []GroupBy
}

// UsageTotal is an aggregated usage summary.
type UsageTotal struct {
	TenantID         string
	AppID            string
	Channel          string
	Provider         string
	Model            string
	InputTokens      int64
	OutputTokens     int64
	ModelCostMinor   int64
	ToolCostMinor    int64
	BudgetUsedTokens int64
	BudgetUsedMinor  int64
	EventCount       int64
	Currency         string
}

// Writer persists audit events.
type Writer interface {
	Append(context.Context, Event) (AppendResult, error)
}

// Reader retrieves audit events.
type Reader interface {
	Get(context.Context, string) (Event, error)
	List(context.Context, Query) ([]Event, error)
}

// Aggregator computes bounded usage totals.
type Aggregator interface {
	AggregateUsage(context.Context, UsageQuery) ([]UsageTotal, error)
}

// Backend owns shared in-memory audit records.
type Backend struct {
	mu     sync.RWMutex
	events map[eventKey]record
}
type record struct {
	event  Event
	digest string
}
type eventKey struct {
	tenantID string
	eventID  string
}

// Store provides tenant-scoped in-memory audit persistence.
type Store struct {
	tenantID string
	backend  *Backend
}

// NewInMemory creates an isolated in-memory audit store.
func NewInMemory(tenantID string) (*Store, error) {
	return NewInMemoryWithBackend(tenantID, &Backend{})
}

// NewInMemoryWithBackend creates a store sharing backend ownership.
func NewInMemoryWithBackend(tenantID string, backend *Backend) (*Store, error) {
	tenantID = clean(tenantID)
	if tenantID == "" || hasControl(tenantID) || backend == nil {
		return nil, ErrInvalid
	}
	backend.mu.Lock()
	if backend.events == nil {
		backend.events = map[eventKey]record{}
	}
	backend.mu.Unlock()
	return &Store{tenantID: tenantID, backend: backend}, nil
}
func (s *Store) scope(tenantID string) error {
	if s == nil || s.backend == nil || s.tenantID == "" || tenantID != s.tenantID {
		return ErrTenantScope
	}
	return nil
}
func check(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	return ctx.Err()
}

// Append validates and stores an event with idempotent deduplication.
func (s *Store) Append(ctx context.Context, event Event) (AppendResult, error) {
	if err := check(ctx); err != nil {
		return AppendResult{}, err
	}
	if err := s.scope(event.TenantID); err != nil {
		return AppendResult{}, err
	}
	digest, err := event.Digest()
	if err != nil {
		return AppendResult{}, err
	}
	event = event.Clone()
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	if err := check(ctx); err != nil {
		return AppendResult{}, err
	}
	key := eventKey{tenantID: event.TenantID, eventID: event.EventID}
	if existing, ok := s.backend.events[key]; ok {
		if existing.digest != digest {
			return AppendResult{}, ErrConflict
		}
		return AppendResult{Event: existing.event.Clone(), Duplicate: true, Digest: digest}, nil
	}
	s.backend.events[key] = record{event: event, digest: digest}
	return AppendResult{Event: event.Clone(), Digest: digest}, nil
}

// Get retrieves an event by ID.
func (s *Store) Get(ctx context.Context, eventID string) (Event, error) {
	if err := check(ctx); err != nil {
		return Event{}, err
	}
	if s == nil || s.scope(s.tenantID) != nil || clean(eventID) == "" {
		return Event{}, ErrTenantScope
	}
	s.backend.mu.RLock()
	defer s.backend.mu.RUnlock()
	value, ok := s.backend.events[eventKey{tenantID: s.tenantID, eventID: clean(eventID)}]
	if !ok {
		return Event{}, ErrNotFound
	}
	return value.event.Clone(), nil
}

// List retrieves events matching query.
func (s *Store) List(ctx context.Context, query Query) ([]Event, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.scope(s.tenantID) != nil {
		return nil, ErrTenantScope
	}
	types := map[EventType]struct{}{}
	for _, typ := range query.EventTypes {
		if !validEventType(typ) {
			return nil, ErrInvalid
		}
		types[typ] = struct{}{}
	}
	s.backend.mu.RLock()
	values := make([]Event, 0)
	for _, item := range s.backend.events {
		event := item.event
		if event.TenantID != s.tenantID {
			continue
		}
		if len(types) > 0 {
			if _, ok := types[event.EventType]; !ok {
				continue
			}
		}
		if !query.Since.IsZero() && event.OccurredAt.Before(query.Since) {
			continue
		}
		if !query.Until.IsZero() && event.OccurredAt.After(query.Until) {
			continue
		}
		values = append(values, event.Clone())
	}
	s.backend.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool {
		if values[i].OccurredAt.Equal(values[j].OccurredAt) {
			return values[i].EventID < values[j].EventID
		}
		return values[i].OccurredAt.Before(values[j].OccurredAt)
	})
	return values, nil
}

// AggregateUsage computes bounded usage totals.
func (s *Store) AggregateUsage(ctx context.Context, query UsageQuery) ([]UsageTotal, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	for _, group := range query.GroupBy {
		if !validGroup(group) {
			return nil, ErrInvalid
		}
	}
	events, err := s.List(ctx, query.Query)
	if err != nil {
		return nil, err
	}
	groups := map[string]*UsageTotal{}
	for _, event := range events {
		if event.Cost == nil {
			continue
		}
		key := aggregateKey(event, query.GroupBy)
		total := groups[key]
		if total == nil {
			total = &UsageTotal{TenantID: s.tenantID, AppID: event.AgentAppID, Channel: event.Channel, Provider: event.Cost.Provider, Model: event.Cost.Model, Currency: event.Cost.Currency}
			groups[key] = total
		}
		addUsage(total, event.Cost)
	}
	result := make([]UsageTotal, 0, len(groups))
	for _, total := range groups {
		result = append(result, *total)
	}
	sort.Slice(result, func(i, j int) bool {
		return aggregateKeyTotal(result[i], query.GroupBy) < aggregateKeyTotal(result[j], query.GroupBy)
	})
	return result, nil
}
func addUsage(total *UsageTotal, usage *Usage) {
	total.EventCount++
	if usage.InputTokens != nil {
		total.InputTokens += *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		total.OutputTokens += *usage.OutputTokens
	}
	if usage.ModelCostMinor != nil {
		total.ModelCostMinor += *usage.ModelCostMinor
	}
	if usage.ToolCostMinor != nil {
		total.ToolCostMinor += *usage.ToolCostMinor
	}
	if usage.BudgetUsedTokens != nil {
		total.BudgetUsedTokens += *usage.BudgetUsedTokens
	}
	if usage.BudgetUsedMinor != nil {
		total.BudgetUsedMinor += *usage.BudgetUsedMinor
	}
}
func aggregateKey(event Event, groups []GroupBy) string {
	parts := make([]string, 0, len(groups)+1)
	parts = append(parts, event.Cost.Currency)
	for _, group := range groups {
		switch group {
		case GroupTenant:
			parts = append(parts, event.TenantID)
		case GroupApp:
			parts = append(parts, event.AgentAppID)
		case GroupChannel:
			parts = append(parts, event.Channel)
		case GroupProvider:
			parts = append(parts, event.Cost.Provider)
		case GroupModel:
			parts = append(parts, event.Cost.Model)
		}
	}
	return strings.Join(parts, "\x00")
}
func aggregateKeyTotal(total UsageTotal, groups []GroupBy) string {
	parts := make([]string, 0, len(groups)+1)
	parts = append(parts, total.Currency)
	for _, group := range groups {
		switch group {
		case GroupTenant:
			parts = append(parts, total.TenantID)
		case GroupApp:
			parts = append(parts, total.AppID)
		case GroupChannel:
			parts = append(parts, total.Channel)
		case GroupProvider:
			parts = append(parts, total.Provider)
		case GroupModel:
			parts = append(parts, total.Model)
		}
	}
	return strings.Join(parts, "\x00")
}
func clean(value string) string { return strings.TrimSpace(value) }
func hasControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
func validEventType(value EventType) bool {
	switch value {
	case EventControlPlaneChanged, EventExecutionStarted, EventExecutionCompleted, EventExecutionFailed, EventExecutionCanceled, EventExecutionTimedOut, EventExecutionFallback, EventCanarySelected, EventToolAllowed, EventToolDenied, EventToolApprovalRequired, EventToolExecuted, EventIMAuthorizationAllowed, EventIMAuthorizationDenied, EventIMIngressAccepted, EventIMIngressDuplicate, EventIMDeliverySent, EventIMDeliveryRetryScheduled, EventIMDeliveryDeadLettered, EventIMDeliveryReconciled, EventBudgetRejected, EventContentRedacted, EventAuditIncomplete:
		return true
	}
	return false
}
func validDecision(value Decision) bool {
	switch value {
	case DecisionAllow, DecisionDeny, DecisionApprovalRequired, DecisionAccepted, DecisionDuplicate, DecisionRejected:
		return true
	}
	return false
}
func validResult(value ExecutionResult) bool {
	switch value {
	case ResultSuccess, ResultFailure, ResultCanceled, ResultTimeout, ResultRejected:
		return true
	}
	return false
}

func validErrorType(value string) bool {
	switch ErrorType(value) {
	case ErrorCanceled, ErrorTimeout, ErrorInvalid, ErrorUnauthenticated, ErrorRateLimited, ErrorDuplicate, ErrorUnavailable, ErrorStorage, ErrorModel, ErrorTool, ErrorProvider, ErrorBudget, ErrorRedacted, ErrorConflict:
		return true
	default:
		return false
	}
}

func containsSensitive(values ...string) bool {
	for _, value := range values {
		lower := strings.ToLower(value)
		if containsSensitivePhrase(lower) || containsSensitiveWords(lower) {
			return true
		}
	}
	return false
}

func containsSensitivePhrase(value string) bool {
	for _, phrase := range []string{"://", "authorization", "bearer ", "api_key", "api-key", "token=", "secret=", "secret_ref", "password=", "dsn=", "provider error"} {
		if strings.Contains(value, phrase) {
			return true
		}
	}
	return false
}

func containsSensitiveWords(value string) bool {
	// Opaque identifiers may contain arbitrary letter fragments separated by
	// punctuation or digits. Treating every non-letter as a word boundary makes
	// random ULID fragments such as "dsn" look like sensitive metadata. Natural
	// language sensitive words are still detected when separated by whitespace;
	// explicit assignment/phrase forms are handled by containsSensitivePhrase.
	words := strings.Fields(strings.ToLower(value))
	for i, word := range words {
		word = strings.Trim(word, ".,:;!?()[]{}<>\"'`")
		if i+1 < len(words) {
			next := strings.Trim(words[i+1], ".,:;!?()[]{}<>\"'`")
			if word == "api" && next == "key" {
				return true
			}
		}
		if word == "bearer" || word == "authorization" || word == "token" || word == "secret" || word == "password" || word == "dsn" {
			return true
		}
	}
	return false
}
func validCurrency(value string) bool {
	_, ok := iso4217[value]
	return ok
}

var iso4217 = map[string]struct{}{
	"AED": {}, "AFN": {}, "ALL": {}, "AMD": {}, "ANG": {}, "AOA": {}, "ARS": {}, "AUD": {}, "AWG": {}, "AZN": {}, "BAM": {}, "BBD": {}, "BDT": {}, "BGN": {}, "BHD": {}, "BIF": {}, "BMD": {}, "BND": {}, "BOB": {}, "BOV": {}, "BRL": {}, "BSD": {}, "BTN": {}, "BWP": {}, "BYN": {}, "BZD": {}, "CAD": {}, "CDF": {}, "CHE": {}, "CHF": {}, "CHW": {}, "CLF": {}, "CLP": {}, "CNY": {}, "COP": {}, "COU": {}, "CRC": {}, "CUC": {}, "CUP": {}, "CVE": {}, "CZK": {}, "DJF": {}, "DKK": {}, "DOP": {}, "DZD": {}, "EGP": {}, "ERN": {}, "ETB": {}, "EUR": {}, "FJD": {}, "FKP": {}, "GBP": {}, "GEL": {}, "GHS": {}, "GIP": {}, "GMD": {}, "GNF": {}, "GTQ": {}, "GYD": {}, "HKD": {}, "HNL": {}, "HTG": {}, "HUF": {}, "IDR": {}, "ILS": {}, "INR": {}, "IQD": {}, "IRR": {}, "ISK": {}, "JMD": {}, "JOD": {}, "JPY": {}, "KES": {}, "KGS": {}, "KHR": {}, "KMF": {}, "KPW": {}, "KRW": {}, "KWD": {}, "KYD": {}, "KZT": {}, "LAK": {}, "LBP": {}, "LKR": {}, "LRD": {}, "LSL": {}, "LYD": {}, "MAD": {}, "MDL": {}, "MGA": {}, "MKD": {}, "MMK": {}, "MNT": {}, "MOP": {}, "MRU": {}, "MUR": {}, "MVR": {}, "MWK": {}, "MXN": {}, "MXV": {}, "MYR": {}, "MZN": {}, "NAD": {}, "NGN": {}, "NIO": {}, "NOK": {}, "NPR": {}, "NZD": {}, "OMR": {}, "PAB": {}, "PEN": {}, "PGK": {}, "PHP": {}, "PKR": {}, "PLN": {}, "PYG": {}, "QAR": {}, "RON": {}, "RSD": {}, "RUB": {}, "RWF": {}, "SAR": {}, "SBD": {}, "SCR": {}, "SDG": {}, "SEK": {}, "SGD": {}, "SHP": {}, "SLE": {}, "SLL": {}, "SOS": {}, "SRD": {}, "SSP": {}, "STN": {}, "SVC": {}, "SYP": {}, "SZL": {}, "THB": {}, "TJS": {}, "TMT": {}, "TND": {}, "TOP": {}, "TRY": {}, "TTD": {}, "TWD": {}, "TZS": {}, "UAH": {}, "UGX": {}, "USD": {}, "USN": {}, "UYI": {}, "UYU": {}, "UYW": {}, "UZS": {}, "VED": {}, "VES": {}, "VND": {}, "VUV": {}, "WST": {}, "XAF": {}, "XAG": {}, "XAU": {}, "XBA": {}, "XBB": {}, "XBC": {}, "XBD": {}, "XCD": {}, "XDR": {}, "XOF": {}, "XPD": {}, "XPF": {}, "XPT": {}, "XSU": {}, "XTS": {}, "XUA": {}, "XXX": {}, "YER": {}, "ZAR": {}, "ZMW": {}, "ZWG": {},
}

func validGroup(value GroupBy) bool {
	switch value {
	case GroupTenant, GroupApp, GroupChannel, GroupProvider, GroupModel:
		return true
	default:
		return false
	}
}
func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

var _ Writer = (*Store)(nil)
var _ Reader = (*Store)(nil)
var _ Aggregator = (*Store)(nil)
