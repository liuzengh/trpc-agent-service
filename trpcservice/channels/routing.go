package channels

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// DefaultCandidateTTL is the short lifetime for a discovered candidate.
const DefaultCandidateTTL = 30 * time.Second

// MaxCandidateLifetime bounds the lifetime accepted by the candidate
// capability boundary. A resolver may choose a shorter TTL, but a caller
// cannot turn a candidate into a long-lived credential by supplying timestamps.
const MaxCandidateLifetime = 5 * time.Minute

// VerificationPurpose binds a candidate-scoped resolver handle to one
// protocol operation. A handle for one purpose cannot be reused elsewhere.
type VerificationPurpose string

const (
	// PurposeWebhookVerification is the inbound callback authentication use.
	PurposeWebhookVerification VerificationPurpose = "webhook-verification"
)

func (purpose VerificationPurpose) validate() error {
	if purpose == "" || hasControl(string(purpose)) || len([]rune(purpose)) > 128 {
		return fmt.Errorf("%w: verification purpose is invalid", ErrInvalid)
	}
	return nil
}

// SecretScope is the only scope accepted by a tenant-level Secret Resolver.
// It is intentionally a pair; there is no global Resolve(secretRef) contract.
type SecretScope struct {
	TenantID  string
	SecretRef string
}

// Validate checks the explicit tenant and secret-reference boundary.
func (scope SecretScope) Validate() error {
	if err := validateTenantID(scope.TenantID); err != nil {
		return err
	}
	secretRef, err := normalizeRequiredValue(scope.SecretRef, maxSecretRefLength, "secret reference")
	if err != nil || secretRef != scope.SecretRef {
		return fmt.Errorf("%w: secret scope is not normalized", ErrInvalid)
	}
	return nil
}

// CandidateBindingContext is the restricted result of public route discovery.
// It contains no TenantID, AppID, SecretRef, Secret value, or live client.
// CandidateToken is a short-lived, single-use opaque capability and must not
// be logged or reconstructed from request fields.
type CandidateBindingContext struct {
	Channel              Channel
	PublicRouteKeyDigest string
	BindingVersion       int64
	ConfigDigest         string
	Purpose              VerificationPurpose
	CandidateToken       string
	IssuedAt             time.Time
	ExpiresAt            time.Time
}

// CandidateBindingInput groups the fields used to mint one validated
// candidate context. It keeps repository call sites readable as the opaque
// capability contract evolves.
type CandidateBindingInput struct {
	Channel              Channel
	PublicRouteKeyDigest string
	BindingVersion       int64
	ConfigDigest         string
	Purpose              VerificationPurpose
	CandidateToken       string
	IssuedAt             time.Time
	ExpiresAt            time.Time
}

// NewCandidateBindingContextFromInput constructs a validated opaque candidate
// result from an explicit input group. The Repository normally creates it
// after a route-index hit.
func NewCandidateBindingContextFromInput(input CandidateBindingInput) (CandidateBindingContext, error) {
	candidate := CandidateBindingContext{
		Channel: input.Channel, PublicRouteKeyDigest: input.PublicRouteKeyDigest, BindingVersion: input.BindingVersion,
		ConfigDigest: input.ConfigDigest, Purpose: input.Purpose, CandidateToken: input.CandidateToken,
		IssuedAt: input.IssuedAt, ExpiresAt: input.ExpiresAt,
	}
	if err := candidate.Validate(input.IssuedAt); err != nil {
		return CandidateBindingContext{}, err
	}
	return candidate, nil
}

// Clone returns a value copy of the candidate context. The Repository compares
// all fields when consuming it, so changing this copy cannot change its state.
func (c CandidateBindingContext) Clone() CandidateBindingContext { return c }

// Validate checks candidate shape and optionally its expiry against now.
func (c CandidateBindingContext) Validate(now time.Time) error {
	if err := c.Channel.Validate(); err != nil {
		return err
	}
	if err := ValidatePublicRouteKeyDigest(c.PublicRouteKeyDigest); err != nil {
		return err
	}
	if c.BindingVersion < 1 || !validHexDigest(c.ConfigDigest) {
		return fmt.Errorf("%w: candidate version or config digest is invalid", ErrInvalid)
	}
	if err := c.Purpose.validate(); err != nil {
		return err
	}
	if c.CandidateToken == "" || len([]rune(c.CandidateToken)) > 256 || hasControl(c.CandidateToken) {
		return fmt.Errorf("%w: candidate token is invalid", ErrInvalid)
	}
	if c.IssuedAt.IsZero() || c.ExpiresAt.IsZero() || c.IssuedAt.Location() != time.UTC || c.ExpiresAt.Location() != time.UTC || !c.ExpiresAt.After(c.IssuedAt) || c.ExpiresAt.Sub(c.IssuedAt) > MaxCandidateLifetime {
		return fmt.Errorf("%w: candidate lifetime is invalid", ErrInvalid)
	}
	if !now.IsZero() && (now.Before(c.IssuedAt) || !now.Before(c.ExpiresAt)) {
		return ErrCandidateUnavailable
	}
	return nil
}

// CandidateSecretRequest carries a discovered candidate and the one operation
// for which a resolver may mint a verifier handle.
type CandidateSecretRequest struct {
	Candidate CandidateBindingContext
	Purpose   VerificationPurpose
}

// ScopedVerifierHandle is a one-time resolver capability. Its opaque token is
// private to the concrete resolver; it is not serializable as a credential.
type ScopedVerifierHandle struct {
	token     string
	Purpose   VerificationPurpose
	ExpiresAt time.Time
}

// NewScopedVerifierHandle creates a handle for a trusted candidate resolver.
// Possessing a syntactically valid handle is not sufficient to verify it; the
// resolver must have an active matching capability in its private state.
func NewScopedVerifierHandle(token string, purpose VerificationPurpose, expiresAt time.Time) (ScopedVerifierHandle, error) {
	if token == "" || len([]rune(token)) > 256 || hasControl(token) || purpose.validate() != nil || expiresAt.IsZero() || expiresAt.Location() != time.UTC {
		return ScopedVerifierHandle{}, fmt.Errorf("%w: verifier handle is invalid", ErrInvalid)
	}
	return ScopedVerifierHandle{token: token, Purpose: purpose, ExpiresAt: expiresAt}, nil
}

// Token returns the opaque handle token to the resolver that owns it. Callers
// must not log, persist, or expose it to a request payload.
func (h ScopedVerifierHandle) Token() string { return h.token }

// VerificationRequest is the provider-neutral fake verification input. The
// route hints are explicitly untrusted and are never copied into VerifiedBinding.
type VerificationRequest struct {
	Purpose       VerificationPurpose
	Timestamp     time.Time
	Nonce         string
	Signature     string
	MessageDigest string
	ReceiveID     string
	RouteHints    UntrustedRouteHints
}

// UntrustedRouteHints models tenant/app/binding fields that may be present in
// an external payload. Verifiers must ignore them for routing decisions.
type UntrustedRouteHints struct {
	TenantID  string
	BindingID string
	AppID     string
}

// CandidateResolver resolves an opaque candidate into a one-time verifier
// handle. It must not accept tenant identity from the request.
type CandidateResolver interface {
	ResolveCandidate(context.Context, CandidateSecretRequest) (ScopedVerifierHandle, error)
}

// CandidateVerifier authenticates a provider request and returns a fixed
// Binding identity only after verification succeeds.
type CandidateVerifier interface {
	Verify(context.Context, ScopedVerifierHandle, VerificationRequest) (VerifiedBinding, error)
}

// VerifiedBinding is the fixed result of successful candidate verification.
// It contains routing identity but no SecretRef or Secret value.
type VerifiedBinding struct {
	TenantID          string
	BindingID         string
	BindingVersion    int64
	AppID             string
	Channel           Channel
	ProviderAccountID string
	ConfigDigest      string
	proof             *verifiedBindingProof
}

// verifiedBindingProof is only created inside this package after a resolver
// has completed provider verification. It also retains the verified fields so
// a copied public value cannot be edited and then re-used as trusted input.
type verifiedBindingProof struct {
	fields VerifiedBinding
}

// Clone returns a defensive copy of the verified result.
func (v VerifiedBinding) Clone() VerifiedBinding { return v }

// Validate checks the fixed identity shape carried by a verifier result.
func (v VerifiedBinding) Validate() error {
	if v.proof == nil {
		return fmt.Errorf("%w: verified binding has no verifier proof", ErrVerificationFailed)
	}
	if v.proof.fields.TenantID != v.TenantID || v.proof.fields.BindingID != v.BindingID || v.proof.fields.BindingVersion != v.BindingVersion || v.proof.fields.AppID != v.AppID || v.proof.fields.Channel != v.Channel || v.proof.fields.ProviderAccountID != v.ProviderAccountID || v.proof.fields.ConfigDigest != v.ConfigDigest {
		return fmt.Errorf("%w: verified binding was modified after verification", ErrInvalid)
	}
	if err := validateTenantID(v.TenantID); err != nil {
		return err
	}
	if err := validateBindingID(v.BindingID); err != nil {
		return err
	}
	if v.BindingVersion < 1 {
		return fmt.Errorf("%w: verified binding version is invalid", ErrInvalid)
	}
	if err := validateAppID(v.AppID); err != nil {
		return err
	}
	if err := v.Channel.Validate(); err != nil {
		return err
	}
	providerAccountID, err := normalizeRequiredValue(v.ProviderAccountID, maxProviderAccountLength, "provider account id")
	if err != nil || providerAccountID != v.ProviderAccountID || !validHexDigest(v.ConfigDigest) {
		return fmt.Errorf("%w: verified binding fields are invalid", ErrInvalid)
	}
	return nil
}

// newVerifiedBinding seals the public routing fields from a validated active
// Binding after a resolver in this package has completed verification. It is
// intentionally unexported: callers must obtain the result from a
// CandidateVerifier rather than minting one from request-shaped fields.
func newVerifiedBinding(binding Binding) (VerifiedBinding, error) {
	if err := binding.Validate(); err != nil {
		return VerifiedBinding{}, err
	}
	if !binding.CanAcceptInbound() {
		return VerifiedBinding{}, ErrVerificationFailed
	}
	verified := VerifiedBinding{
		TenantID: binding.TenantID, BindingID: binding.BindingID, BindingVersion: binding.Version,
		AppID: binding.AppID, Channel: binding.Channel, ProviderAccountID: binding.ProviderAccountID,
		ConfigDigest: binding.ConfigDigest,
	}
	verified.proof = &verifiedBindingProof{fields: verified}
	return verified, nil
}

// RoutingTarget is the fixed, non-secret route selected after candidate
// verification and active Tenant/App/Binding checks.
type RoutingTarget struct {
	TenantID          string
	BindingID         string
	BindingVersion    int64
	AppID             string
	Channel           Channel
	ProviderAccountID string
	ConfigDigest      string
	capability        *routingTargetCapability
}

// routingTargetCapability retains the verifier-owned snapshot that created a
// RoutingTarget. Keeping the expected fields private makes the public route
// value tamper-evident: callers cannot mint the capability or change a copied
// target without failing validation.
type routingTargetCapability struct {
	verified VerifiedBinding
}

// NewRoutingTarget validates the trusted Tenant snapshot, current Binding,
// active App, and verifier result as one boundary. It ignores all external
// route hints and does not expose SecretRef.
func NewRoutingTarget(tenantSnapshot tenant.ConfigurationSnapshot, binding *Binding, app *appmodel.App, verified VerifiedBinding) (RoutingTarget, error) {
	tenantValue := tenantSnapshot.Tenant()
	if err := tenantValue.Validate(); err != nil || !tenantValue.CanAcceptExecution() {
		return RoutingTarget{}, fmt.Errorf("%w: tenant snapshot cannot accept inbound routing", ErrVerificationFailed)
	}
	if binding == nil || app == nil {
		return RoutingTarget{}, fmt.Errorf("%w: trusted binding and app snapshots are required", ErrVerificationFailed)
	}
	if err := binding.Validate(); err != nil || !binding.CanAcceptInbound() {
		return RoutingTarget{}, fmt.Errorf("%w: binding snapshot cannot accept inbound routing", ErrVerificationFailed)
	}
	if err := app.Validate(); err != nil || !app.CanAcceptExecution() {
		return RoutingTarget{}, fmt.Errorf("%w: app snapshot cannot accept inbound routing", ErrVerificationFailed)
	}
	if err := verified.Validate(); err != nil {
		return RoutingTarget{}, fmt.Errorf("%w: verified binding is invalid", ErrVerificationFailed)
	}
	if tenantValue.TenantID != binding.TenantID || tenantValue.TenantID != app.TenantID || verified.TenantID != binding.TenantID || verified.AppID != binding.AppID || verified.BindingID != binding.BindingID || verified.BindingVersion != binding.Version || verified.AppID != app.AppID || verified.Channel != binding.Channel || verified.ProviderAccountID != binding.ProviderAccountID || verified.ConfigDigest != binding.ConfigDigest {
		return RoutingTarget{}, fmt.Errorf("%w: trusted tenant, binding, app, and verifier scopes do not match", ErrVerificationFailed)
	}
	routingTarget := RoutingTarget{
		TenantID: tenantValue.TenantID, BindingID: binding.BindingID, BindingVersion: binding.Version,
		AppID: app.AppID, Channel: binding.Channel, ProviderAccountID: binding.ProviderAccountID,
		ConfigDigest: binding.ConfigDigest,
	}
	routingTarget.capability = &routingTargetCapability{verified: verified}
	return routingTarget, nil
}

// ResolveCandidateRoutingTarget consumes one candidate after a protocol
// verifier has authenticated its Binding, then reads the current trusted
// Tenant and App snapshots needed to seal a RoutingTarget. The verifier sees
// a Binding only after route-key candidate selection and must never accept
// routing identity from an external payload.
func ResolveCandidateRoutingTarget(
	ctx context.Context,
	consumer CandidateConsumer,
	tenants tenant.Repository,
	apps appmodel.Repository,
	candidate CandidateBindingContext,
	verify func(context.Context, Binding) error,
) (RoutingTarget, error) {
	if ctx == nil {
		return RoutingTarget{}, ErrVerificationFailed
	}
	if err := ctx.Err(); err != nil {
		return RoutingTarget{}, err
	}
	if consumer == nil || tenants == nil || apps == nil || verify == nil || candidate.Channel == "" {
		return RoutingTarget{}, ErrVerificationFailed
	}
	binding, err := consumer.ConsumeCandidate(ctx, candidate)
	if err != nil {
		if IsContextCancellation(err) {
			return RoutingTarget{}, err
		}
		return RoutingTarget{}, ErrVerificationFailed
	}
	if binding == nil {
		return RoutingTarget{}, ErrVerificationFailed
	}
	if err := verify(ctx, binding.Clone()); err != nil {
		if IsContextCancellation(err) {
			return RoutingTarget{}, err
		}
		return RoutingTarget{}, ErrVerificationFailed
	}
	return routingTargetForBinding(ctx, tenants, apps, binding)
}

// ResolveConfiguredRoutingTarget seals a target for a process-configured
// Binding after reading the current trusted control-plane state. BindingID is
// an operator-owned startup setting, never an inbound routing hint.
func ResolveConfiguredRoutingTarget(ctx context.Context, consumer CandidateConsumer, tenants tenant.Repository, apps appmodel.Repository, tenantID, bindingID string) (RoutingTarget, error) {
	if ctx == nil {
		return RoutingTarget{}, ErrVerificationFailed
	}
	if err := ctx.Err(); err != nil {
		return RoutingTarget{}, err
	}
	if consumer == nil || tenants == nil || apps == nil || tenantID == "" || bindingID == "" {
		return RoutingTarget{}, ErrVerificationFailed
	}
	binding, err := consumer.Get(ctx, tenantID, bindingID)
	if err != nil {
		if IsContextCancellation(err) {
			return RoutingTarget{}, err
		}
		return RoutingTarget{}, ErrVerificationFailed
	}
	return routingTargetForBinding(ctx, tenants, apps, binding)
}

func routingTargetForBinding(ctx context.Context, tenants tenant.Repository, apps appmodel.Repository, binding *Binding) (RoutingTarget, error) {
	if binding == nil {
		return RoutingTarget{}, ErrVerificationFailed
	}
	verified, err := newVerifiedBinding(*binding)
	if err != nil {
		return RoutingTarget{}, ErrVerificationFailed
	}
	tenantValue, err := tenants.Get(ctx, binding.TenantID)
	if err != nil {
		if IsContextCancellation(err) {
			return RoutingTarget{}, err
		}
		return RoutingTarget{}, ErrVerificationFailed
	}
	snapshot, err := tenant.NewConfigurationSnapshot(tenantValue)
	if err != nil {
		return RoutingTarget{}, ErrVerificationFailed
	}
	app, err := apps.Get(ctx, binding.TenantID, binding.AppID)
	if err != nil {
		if IsContextCancellation(err) {
			return RoutingTarget{}, err
		}
		return RoutingTarget{}, ErrVerificationFailed
	}
	target, err := NewRoutingTarget(snapshot, binding, app, verified)
	if err != nil {
		return RoutingTarget{}, ErrVerificationFailed
	}
	return target, nil
}

// Validate checks the non-secret target identity.
func (target RoutingTarget) Validate() error {
	if target.capability == nil {
		return fmt.Errorf("%w: routing target was not created by the trusted boundary", ErrVerificationFailed)
	}
	current := VerifiedBinding{
		TenantID:          target.TenantID,
		BindingID:         target.BindingID,
		BindingVersion:    target.BindingVersion,
		AppID:             target.AppID,
		Channel:           target.Channel,
		ProviderAccountID: target.ProviderAccountID,
		ConfigDigest:      target.ConfigDigest,
	}
	if !sameVerifiedBindingFields(current, target.capability.verified) {
		return fmt.Errorf("%w: routing target was modified after trusted creation", ErrVerificationFailed)
	}
	return target.capability.verified.Validate()
}

func sameVerifiedBindingFields(left, right VerifiedBinding) bool {
	return left.TenantID == right.TenantID && left.BindingID == right.BindingID && left.BindingVersion == right.BindingVersion && left.AppID == right.AppID && left.Channel == right.Channel && left.ProviderAccountID == right.ProviderAccountID && left.ConfigDigest == right.ConfigDigest
}

// ConversationKind controls the external identity tuple used for a session.
type ConversationKind string

const (
	// ConversationDirect identifies a direct peer conversation.
	ConversationDirect ConversationKind = "direct"
	// ConversationGroup identifies a group/chat conversation.
	ConversationGroup ConversationKind = "group"
)

// IdentityInput contains stable provider identifiers for one Runner identity.
// Names and display labels are intentionally not accepted.
type IdentityInput struct {
	ExternalUserID   string
	Kind             ConversationKind
	ExternalPeerID   string
	ExternalChatID   string
	ExternalThreadID string
}

// RunnerIdentity creates a Tenant + Channel + Binding-aware collision-free
// Runner identity. Thread/topic is a separate length-prefixed segment.
func (target RoutingTarget) RunnerIdentity(input IdentityInput) (tenant.RunnerIdentity, error) {
	if err := target.Validate(); err != nil {
		return tenant.RunnerIdentity{}, err
	}
	userID, err := requiredExternalID(input.ExternalUserID, "external user id")
	if err != nil {
		return tenant.RunnerIdentity{}, err
	}
	var conversationID string
	switch input.Kind {
	case ConversationDirect:
		conversationID, err = requiredExternalID(input.ExternalPeerID, "external peer id")
	case ConversationGroup:
		conversationID, err = requiredExternalID(input.ExternalChatID, "external chat id")
	default:
		return tenant.RunnerIdentity{}, fmt.Errorf("%w: unknown conversation kind", ErrInvalid)
	}
	if err != nil {
		return tenant.RunnerIdentity{}, err
	}
	if input.ExternalThreadID != "" && hasControl(input.ExternalThreadID) {
		return tenant.RunnerIdentity{}, fmt.Errorf("%w: external thread id is invalid", ErrInvalid)
	}
	scopedUser := encodeIdentityParts(string(target.Channel), target.BindingID, userID)
	scopedSession := encodeIdentityParts(string(target.Channel), target.BindingID, string(input.Kind), conversationID, input.ExternalThreadID)
	return tenant.NewRunnerIdentity(target.TenantID, scopedUser, scopedSession)
}

func requiredExternalID(value, label string) (string, error) {
	if value == "" || hasControl(value) || len([]rune(value)) > 1024 {
		return "", fmt.Errorf("%w: %s is invalid", ErrInvalid, label)
	}
	return value, nil
}

func encodeIdentityParts(parts ...string) string {
	var builder strings.Builder
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len([]byte(part))))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}

func validHexDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

// IsContextCancellation reports whether an error is a caller cancellation;
// concrete repositories use it to preserve cancellation rather than replacing
// it with a generic candidate error.
func IsContextCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
