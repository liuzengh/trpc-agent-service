// Package gateway defines the tenant-aware request ingress boundary.
package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/attribute"
)

// TenantSource identifies the trusted boundary that supplied tenant routing.
type TenantSource string

const (
	// TenantSourceAuthenticatedClaims means tenant routing came from authenticated claims.
	TenantSourceAuthenticatedClaims TenantSource = "authenticated_claims"
	// TenantSourceVerifiedChannelBinding means tenant routing came from a channel
	// binding whose route and tenant scope were validated. Provider protocol
	// verification remains the adapter's responsibility.
	TenantSourceVerifiedChannelBinding TenantSource = "verified_channel_binding"
)

var (
	// ErrAdmitterRequired means a Gateway has no atomic admission backend.
	ErrAdmitterRequired = errors.New("admitter is required")
	// ErrAdmissionIdentityRequired means the ingress did not provide an
	// identity that can be revalidated by the admission transaction.
	ErrAdmissionIdentityRequired = errors.New("admission identity resolver is required")
	// ErrIdempotencyConflict means one idempotency key was reused with another
	// normalized request payload.
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with existing request")
	// ErrUnsupportedAdmissionSource means the selected admission backend does
	// not yet implement the request's trusted source type.
	ErrUnsupportedAdmissionSource = errors.New("admission source is unsupported")
	// ErrChannelInputRequired means a verified channel admission did not carry
	// the normalized provider-neutral message required by Inbox admission.
	ErrChannelInputRequired = errors.New("channel input is required for channel admission")
	// ErrAdmissionDraining means backend migration is draining accepted work and
	// new requests must be retried after the advertised maintenance window.
	ErrAdmissionDraining = errors.New("request admission is draining for data migration")
	// ErrChannelBindingSnapshotStale means a channel binding snapshot no longer
	// matches the authoritative Binding row.
	ErrChannelBindingSnapshotStale = errors.New("channel binding snapshot is stale")
	// ErrInvalidArtifactRef means a client supplied an invalid pinned artifact
	// reference. It is distinct from admission backend failures so protocol
	// adapters can return a client error instead of a server error.
	ErrInvalidArtifactRef = errors.New("invalid artifact ref")
	// ErrAdmissionRateLimited means the shared admission rate has been
	// exhausted. It is intentionally separate from backend availability so
	// transports can return a retryable overload response.
	ErrAdmissionRateLimited = errors.New("admission rate limit exceeded")
	// ErrAdmissionConcurrencyLimit means this Gateway instance has no local
	// admission slot available. The slot covers only the admission path and
	// is released after the durable admission call returns.
	ErrAdmissionConcurrencyLimit = errors.New("admission concurrency limit exceeded")
	// ErrAdmissionQuotaExceeded means the authoritative tenant/application
	// period quota rejected a new execution.
	ErrAdmissionQuotaExceeded = errors.New("admission period quota exceeded")
)

// AdmissionRateLimitError carries the retry delay returned by the shared
// limiter without exposing its implementation to protocol adapters.
type AdmissionRateLimitError struct {
	RetryAfter time.Duration
}

func (e *AdmissionRateLimitError) Error() string {
	if e == nil || e.RetryAfter <= 0 {
		return ErrAdmissionRateLimited.Error()
	}
	return fmt.Sprintf("%s; retry after %s", ErrAdmissionRateLimited, e.RetryAfter)
}

func (e *AdmissionRateLimitError) Unwrap() error { return ErrAdmissionRateLimited }

// AdmissionRateLimiter is the shared ingress admission budget. Production
// implementations use Redis so separate Gateway nodes consume one budget.
type AdmissionRateLimiter interface {
	Allow(context.Context, AdmissionIdentity) error
}

// AdmissionConcurrency bounds work performed by one Gateway process while it
// is validating, pinning, preparing, and durably admitting a request.
type AdmissionConcurrency struct {
	slots chan struct{}
}

// NewAdmissionConcurrency creates a process-local admission semaphore.
func NewAdmissionConcurrency(limit int) (*AdmissionConcurrency, error) {
	if limit <= 0 {
		return nil, errors.New("admission concurrency limit must be positive")
	}
	return &AdmissionConcurrency{slots: make(chan struct{}, limit)}, nil
}

// Acquire takes one local slot without waiting behind an overloaded request.
// The returned release function must be called exactly once when the admission
// path returns.
func (c *AdmissionConcurrency) Acquire() (func(), error) {
	if c == nil || c.slots == nil {
		return func() {}, nil
	}
	select {
	case c.slots <- struct{}{}:
		return func() { <-c.slots }, nil
	default:
		return nil, ErrAdmissionConcurrencyLimit
	}
}

// Message is the normalized user input passed from gateway to workers.
type Message struct {
	Text         string
	ArtifactRefs []string
}

// ParseArtifactRef validates and splits one pinned artifact reference.
func ParseArtifactRef(ref string) (string, int, error) {
	if !utf8.ValidString(ref) {
		return "", 0, errors.New("artifact ref is not valid utf-8")
	}
	if !strings.HasPrefix(ref, "artifact://") {
		return "", 0, errors.New("artifact ref must use artifact://")
	}
	rest := strings.TrimPrefix(ref, "artifact://")
	separator := strings.LastIndex(rest, "@")
	if separator <= 0 || separator == len(rest)-1 {
		return "", 0, errors.New("artifact ref must pin a version")
	}
	name := rest[:separator]
	if strings.ContainsAny(name, "@\x00\r\n\t") || strings.Contains(name, "..") {
		return "", 0, errors.New("artifact ref name is invalid")
	}
	version, err := strconv.Atoi(rest[separator+1:])
	if err != nil || version < 0 {
		return "", 0, errors.New("artifact ref version is invalid")
	}
	return name, version, nil
}

// Validate checks whether Message contains a platform-approved user payload.
func (m Message) Validate() error {
	if m.Text == "" && len(m.ArtifactRefs) == 0 {
		return errors.New("message content is required")
	}
	if !utf8.ValidString(m.Text) {
		return errors.New("message text is not valid utf-8")
	}
	for index, ref := range m.ArtifactRefs {
		if _, _, err := ParseArtifactRef(ref); err != nil {
			return fmt.Errorf("%w %d: %w", ErrInvalidArtifactRef, index, err)
		}
	}
	return nil
}

// TenantResolver supplies tenant routing from an authentication or verification boundary.
// Implementations must not derive tenant identity from unverified external payload fields.
type TenantResolver interface {
	ResolveTenant(ctx context.Context) (tenant.RuntimeContext, TenantSource, error)
}

// CredentialDigest is a non-reversible API key digest carried to admission.
// It contains no raw credential material.
type CredentialDigest [sha256.Size]byte

// AdmissionIdentity is the trusted request identity that the admission
// transaction must revalidate against authoritative storage.
type AdmissionIdentity struct {
	Tenant           tenant.RuntimeContext
	Source           TenantSource
	SourceID         string
	CredentialDigest CredentialDigest
	// PublicRouteID and BindingRevision identify the Binding snapshot that
	// supplied a channel-binding identity. They are ignored for authenticated
	// claims and revalidated by the PostgreSQL admission transaction.
	PublicRouteID            string
	BindingRevision          int64
	configVersionPinned      bool
	channelBindingProvenance *channelBindingProvenance
	channelMappingPending    bool
}

// channelBindingProvenance is intentionally package-private. A channel
// binding identity must be produced by the gateway's route-bound constructor;
// field validation alone is not a trusted provenance check. The provenance
// binds every exported identity field so copying a valid identity and
// changing its scope or session cannot reuse it.
type channelBindingProvenance struct {
	runtimeContext   tenant.RuntimeContext
	source           TenantSource
	sourceID         string
	credentialDigest CredentialDigest
	publicRouteID    string
	bindingRevision  int64
	mappingPending   bool
	configPinned     bool
}

// Validate checks the trusted source and identity fields required for atomic
// admission. A normal config version is advisory; a channel pin is marked by
// private provenance and must be honored by the authoritative backend.
func (i AdmissionIdentity) Validate() error {
	return i.validate(false)
}

// ValidateForChannelInput checks a trusted channel identity whose sender and
// session principals will be resolved by the admission transaction.
func (i AdmissionIdentity) ValidateForChannelInput() error {
	return i.validate(true)
}

func (i AdmissionIdentity) validate(allowPendingMapping bool) error {
	if i.channelMappingPending && allowPendingMapping {
		if err := validatePendingRuntimeContext(i.Tenant); err != nil {
			return err
		}
	} else if err := i.Tenant.Validate(); err != nil {
		return err
	}
	if !validTenantSource(i.Source) {
		return errors.New("tenant source is invalid")
	}
	if i.SourceID == "" {
		return errors.New("source_id is required")
	}
	if i.Source == TenantSourceAuthenticatedClaims && i.CredentialDigest == (CredentialDigest{}) {
		return errors.New("credential digest is required")
	}
	if i.Source == TenantSourceVerifiedChannelBinding {
		provenance := i.channelBindingProvenance
		if provenance == nil {
			return errors.New("channel binding provenance is required")
		}
		if provenance.runtimeContext != i.Tenant ||
			provenance.source != i.Source ||
			provenance.sourceID != i.SourceID ||
			provenance.credentialDigest != i.CredentialDigest ||
			provenance.publicRouteID != i.PublicRouteID ||
			provenance.bindingRevision != i.BindingRevision ||
			provenance.mappingPending != i.channelMappingPending {
			return errors.New("channel binding provenance does not match identity")
		}
		if provenance.configPinned != i.configVersionPinned {
			return errors.New("channel binding config pin provenance does not match identity")
		}
		if err := channels.Channel(i.Tenant.Channel).Validate(); err != nil {
			return fmt.Errorf("channel binding channel: %w", err)
		}
		if i.Tenant.BindingID == "" {
			return errors.New("binding_id is required for channel binding source")
		}
		if i.SourceID != i.Tenant.BindingID {
			return errors.New("source_id must equal binding_id for channel binding source")
		}
		if i.PublicRouteID != "" {
			if err := channels.ValidatePublicRouteID(i.PublicRouteID); err != nil {
				return fmt.Errorf("channel binding route: %w", err)
			}
		}
		if i.BindingRevision <= 0 {
			return errors.New("binding_revision must be positive for channel binding source")
		}
	}
	return nil
}

// ConfigVersionPinned reports whether Gateway selected the immutable config
// version for a channel request before any attachment was materialized.
func (i AdmissionIdentity) ConfigVersionPinned() bool {
	return i.configVersionPinned
}

func validatePendingRuntimeContext(context tenant.RuntimeContext) error {
	if context.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if context.AppID == "" {
		return errors.New("app_id is required")
	}
	if context.Channel == "" {
		return errors.New("channel is required")
	}
	if context.BindingID == "" {
		return errors.New("binding_id is required")
	}
	if context.SessionID == "" {
		return errors.New("session_id is required")
	}
	if context.TraceID == "" {
		return errors.New("trace_id is required")
	}
	return nil
}

// AdmissionIdentityResolver exposes an authenticated identity to the Gateway
// without exposing raw credentials or trusting request payload tenant fields.
type AdmissionIdentityResolver interface {
	ResolveAdmissionIdentity(ctx context.Context) (AdmissionIdentity, error)
}

// Request is a gateway input whose tenant routing is supplied by a trusted resolver.
type Request struct {
	RequestID      string
	IdempotencyKey string
	Tenant         TenantResolver
	Message        Message
	ChannelInput   *channels.ChannelInput
}

// AdmissionRequest is the normalized command submitted to the atomic
// admission backend.
type AdmissionRequest struct {
	RequestID      string
	IdempotencyKey string
	Identity       AdmissionIdentity
	Message        Message
	ChannelInput   *channels.ChannelInput
	TraceParent    string
	TraceState     string
}

// Validate checks the fields that must be stable before the admission
// transaction allocates a turn or writes an execution.
func (r AdmissionRequest) Validate() error {
	if r.RequestID == "" {
		return errors.New("request_id is required")
	}
	if r.IdempotencyKey == "" {
		return errors.New("idempotency_key is required")
	}
	if r.ChannelInput == nil {
		if r.channelMappingPending() {
			return errors.New("channel mapping input is required")
		}
		if err := r.Identity.Validate(); err != nil {
			return fmt.Errorf("admission identity: %w", err)
		}
		if err := r.Message.Validate(); err != nil {
			return fmt.Errorf("message: %w", err)
		}
		return nil
	}
	if err := r.Identity.ValidateForChannelInput(); err != nil {
		return fmt.Errorf("admission identity: %w", err)
	}
	if err := r.ChannelInput.Validate(); err != nil {
		return fmt.Errorf("channel input: %w", err)
	}
	if r.Identity.Source != TenantSourceVerifiedChannelBinding {
		return ErrUnsupportedAdmissionSource
	}
	if r.ChannelInput.TenantID != r.Identity.Tenant.TenantID ||
		r.ChannelInput.AppID != r.Identity.Tenant.AppID ||
		r.ChannelInput.Channel != channels.Channel(r.Identity.Tenant.Channel) ||
		r.ChannelInput.BindingID != r.Identity.Tenant.BindingID ||
		r.ChannelInput.BindingRevision != r.Identity.BindingRevision {
		return ErrChannelBindingScopeMismatch
	}
	if r.Message.Text != r.ChannelInput.Text ||
		!slices.Equal(r.Message.ArtifactRefs, r.ChannelInput.ArtifactRefs) {
		return errors.New("channel input message does not match gateway message")
	}
	// Unsupported provider payloads are durably classified by PostgreSQL so
	// they can be rejected without creating an execution. Validate executable
	// content here, while allowing an empty normalized payload to reach that
	// classification path.
	if r.Message.Text != "" || len(r.Message.ArtifactRefs) > 0 {
		if err := r.Message.Validate(); err != nil {
			return fmt.Errorf("message: %w", err)
		}
	}
	return nil
}

func (r AdmissionRequest) channelMappingPending() bool {
	return r.Identity.channelMappingPending
}

// AdmissionStatus describes the durable result of a channel admission.
type AdmissionStatus string

const (
	// AdmissionStatusAdmitted means an execution and dispatch record were
	// committed.
	AdmissionStatusAdmitted AdmissionStatus = "ADMITTED"
	// AdmissionStatusRejected means a verified but unsupported channel input was
	// durably recorded without an execution.
	AdmissionStatusRejected AdmissionStatus = "REJECTED"
)

// AdmissionResult describes the committed admission identity returned to an
// ingress. Replayed is true when an identical idempotent request already
// existed.
type AdmissionResult struct {
	RequestID     string
	ConfigVersion string
	TurnSeq       int64
	Replayed      bool
	Status        AdmissionStatus
}

// Validate checks the result returned by an admission backend.
func (r AdmissionResult) Validate() error {
	if r.RequestID == "" {
		return errors.New("request_id is required")
	}
	switch r.Status {
	case "", AdmissionStatusAdmitted:
		if r.ConfigVersion == "" {
			return errors.New("config_version is required")
		}
		if r.TurnSeq <= 0 {
			return errors.New("turn_seq must be positive")
		}
	case AdmissionStatusRejected:
		if r.ConfigVersion != "" || r.TurnSeq != 0 {
			return errors.New("rejected admission cannot contain execution fields")
		}
	default:
		return errors.New("admission status is invalid")
	}
	return nil
}

// Admitter atomically accepts a normalized request into the authoritative
// execution store.
type Admitter interface {
	Admit(ctx context.Context, request AdmissionRequest) (AdmissionResult, error)
}

// ChannelConfigVersionPinner selects the immutable config version used by a
// channel admission. It is implemented by the authoritative admission store.
type ChannelConfigVersionPinner interface {
	PinChannelConfig(context.Context, AdmissionRequest) (string, error)
}

// ChannelFailureRecorder durably replies when a verified channel request
// cannot reach execution admission.
type ChannelFailureRecorder interface {
	RecordChannelFailure(context.Context, AdmissionRequest) error
}

// ChannelAttachmentPreparer is an adapter-owned media boundary. Raw provider
// media handles stay in the adapter closure and only resulting ArtifactRefs
// cross into Gateway admission.
type ChannelAttachmentPreparer func(
	context.Context,
	channels.ChannelInput,
	string,
) (channels.ChannelInput, func(context.Context) error, error)

// Gateway converts trusted requests into atomic admission commands.
type Gateway struct {
	admitter             Admitter
	Metrics              *platformmetrics.Recorder
	RateLimiter          AdmissionRateLimiter
	AdmissionConcurrency *AdmissionConcurrency
}

// New creates a Gateway backed by the authoritative admission store.
func New(admitter Admitter) *Gateway {
	return &Gateway{admitter: admitter}
}

// Handle validates a request and submits it to the authoritative admission
// backend. It never performs a separate in-memory enqueue.
func (g Gateway) Handle(ctx context.Context, req Request) (result AdmissionResult, err error) {
	return g.handle(ctx, req, nil)
}

// HandleChannel pins the channel config before invoking the adapter-owned
// attachment preparer. Failed admission invokes the returned compensator so
// pre-admission objects do not remain orphaned.
func (g Gateway) HandleChannel(
	ctx context.Context,
	req Request,
	prepare ChannelAttachmentPreparer,
) (result AdmissionResult, err error) {
	if prepare == nil {
		return AdmissionResult{}, errors.New("channel attachment preparer is required")
	}
	return g.handle(ctx, req, prepare)
}

// RecordChannelFailure persists one idempotent failure reply for a verified
// channel request that bypassed normal admission, such as a platform command.
func (g Gateway) RecordChannelFailure(ctx context.Context, request AdmissionRequest) error {
	recorder, ok := g.admitter.(ChannelFailureRecorder)
	if !ok {
		return errors.New("channel failure recorder is required")
	}
	return recorder.RecordChannelFailure(ctx, request)
}

func (g Gateway) handle(
	ctx context.Context,
	req Request,
	prepare ChannelAttachmentPreparer,
) (result AdmissionResult, err error) {
	if g.admitter == nil {
		return AdmissionResult{}, ErrAdmitterRequired
	}
	if req.Tenant == nil {
		return AdmissionResult{}, errors.New("tenant resolver is required")
	}
	identityResolver, ok := req.Tenant.(AdmissionIdentityResolver)
	if !ok {
		return AdmissionResult{}, ErrAdmissionIdentityRequired
	}
	identity, err := identityResolver.ResolveAdmissionIdentity(ctx)
	if err != nil {
		return AdmissionResult{}, err
	}
	admitCtx, span := platformtelemetry.StartSpan(ctx, "gateway.admit",
		attribute.String("tenant_id", identity.Tenant.TenantID),
		attribute.String("app_id", identity.Tenant.AppID),
		attribute.String("channel", identity.Tenant.Channel),
	)
	defer span.End()
	defer func() {
		if g.Metrics == nil {
			return
		}
		labels := platformmetrics.Labels{
			TenantID:      identity.Tenant.TenantID,
			AppID:         identity.Tenant.AppID,
			ConfigVersion: result.ConfigVersion,
			Channel:       identity.Tenant.Channel,
			Result:        "accepted",
		}
		errorType := ""
		if err != nil {
			labels.Result = "error"
			errorType = "admission"
		} else if result.Status == AdmissionStatusRejected {
			labels.Result = "rejected"
			errorType = "governance"
		}
		g.Metrics.RecordRequest(ctx, labels, errorType)
	}()
	if traceID := platformtelemetry.TraceID(admitCtx); traceID != "" {
		identity.Tenant.TraceID = traceID
	}
	identity.Tenant.TraceParent = platformtelemetry.TraceParent(admitCtx)
	identity.Tenant.TraceState = platformtelemetry.TraceState(admitCtx)
	// Trace propagation enriches the trusted runtime context after the
	// resolver has constructed its binding provenance. Keep the provenance in
	// lockstep so the enrichment cannot look like a scope mutation during the
	// channel-input validation below.
	if identity.channelBindingProvenance != nil {
		identity.channelBindingProvenance.runtimeContext = identity.Tenant
	}
	var channelInput *channels.ChannelInput
	message := Message{
		Text:         req.Message.Text,
		ArtifactRefs: slices.Clone(req.Message.ArtifactRefs),
	}
	if req.ChannelInput != nil {
		input := req.ChannelInput.Clone()
		channelInput = &input
		message = Message{
			Text:         input.Text,
			ArtifactRefs: slices.Clone(input.ArtifactRefs),
		}
	}
	admissionRequest := AdmissionRequest{
		RequestID:      req.RequestID,
		IdempotencyKey: req.IdempotencyKey,
		Identity:       identity,
		Message:        message,
		ChannelInput:   channelInput,
		TraceParent:    identity.Tenant.TraceParent,
		TraceState:     identity.Tenant.TraceState,
	}
	if err := admissionRequest.Validate(); err != nil {
		return AdmissionResult{}, err
	}
	if g.AdmissionConcurrency != nil {
		release, err := g.AdmissionConcurrency.Acquire()
		if err != nil {
			return AdmissionResult{}, err
		}
		defer release()
	}
	if g.RateLimiter != nil {
		if err := g.RateLimiter.Allow(admitCtx, identity); err != nil {
			return AdmissionResult{}, err
		}
	}
	if channelInput != nil {
		defer func() {
			if err == nil && result.Status != AdmissionStatusRejected {
				return
			}
			if failureErr := g.RecordChannelFailure(admitCtx, admissionRequest); failureErr != nil {
				err = errors.Join(err, fmt.Errorf("record channel failure: %w", failureErr))
			}
		}()
	}
	var cleanup func(context.Context) error
	if prepare != nil {
		if channelInput == nil || identity.Source != TenantSourceVerifiedChannelBinding {
			return AdmissionResult{}, errors.New("channel attachment requires a verified channel input")
		}
		pinner, ok := g.admitter.(ChannelConfigVersionPinner)
		if !ok {
			return AdmissionResult{}, errors.New("channel config version pinner is required")
		}
		pinnedVersion, err := pinner.PinChannelConfig(admitCtx, admissionRequest)
		if err != nil {
			return AdmissionResult{}, fmt.Errorf("pin channel config version: %w", err)
		}
		if pinnedVersion == "" {
			return AdmissionResult{}, errors.New("pinned channel config version is required")
		}
		identity.Tenant.ConfigVersion = pinnedVersion
		identity.configVersionPinned = true
		if identity.channelBindingProvenance != nil {
			identity.channelBindingProvenance.runtimeContext = identity.Tenant
			identity.channelBindingProvenance.configPinned = true
		}
		admissionRequest.Identity = identity
		prepared, compensator, prepareErr := prepare(admitCtx, *channelInput, pinnedVersion)
		if prepareErr != nil {
			if compensator != nil {
				prepareErr = errors.Join(prepareErr, compensator(context.WithoutCancel(admitCtx)))
			}
			return AdmissionResult{}, prepareErr
		}
		cleanup = compensator
		prepared = prepared.Clone()
		channelInput = &prepared
		admissionRequest.ChannelInput = channelInput
		admissionRequest.Message = Message{
			Text:         prepared.Text,
			ArtifactRefs: slices.Clone(prepared.ArtifactRefs),
		}
		if err := admissionRequest.Validate(); err != nil {
			if cleanup != nil {
				err = errors.Join(err, cleanup(context.WithoutCancel(admitCtx)))
			}
			return AdmissionResult{}, err
		}
	}
	result, err = g.admitter.Admit(admitCtx, admissionRequest)
	if err != nil {
		if cleanup != nil {
			err = errors.Join(err, cleanup(context.WithoutCancel(admitCtx)))
		}
		return AdmissionResult{}, err
	}
	if err := result.Validate(); err != nil {
		validationErr := fmt.Errorf("admitter result: %w", err)
		if cleanup != nil {
			validationErr = errors.Join(validationErr, cleanup(context.WithoutCancel(admitCtx)))
		}
		return AdmissionResult{}, validationErr
	}
	return result, nil
}

func validTenantSource(source TenantSource) bool {
	return source == TenantSourceAuthenticatedClaims || source == TenantSourceVerifiedChannelBinding
}
