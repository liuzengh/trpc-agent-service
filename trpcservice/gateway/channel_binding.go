package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrPublicRouteResolverRequired means no public route resolver was
	// configured for channel ingress.
	ErrPublicRouteResolverRequired = errors.New("public route resolver is required")
	// ErrChannelBindingScopeMismatch means the runtime context does not match
	// the located channel Binding for this request.
	ErrChannelBindingScopeMismatch = errors.New("channel binding scope mismatch")
)

// PublicRouteResolver locates a binding by its opaque public route. It must not
// infer tenant or application identity from caller or provider payloads.
type PublicRouteResolver interface {
	ResolveBindingByPublicRoute(
		ctx context.Context,
		channel channels.Channel,
		publicRouteID string,
	) (channels.BindingSnapshot, error)
}

// LocatedChannelBinding is the result of a route lookup. Its unexported state
// prevents callers from manufacturing a trusted binding from an arbitrary
// Binding value. Callers may inspect Snapshot while performing provider
// verification, then pass this value to NewChannelBindingIdentityResolver.
type LocatedChannelBinding struct {
	snapshot   channels.BindingSnapshot
	provenance *locatedChannelBindingProvenance
}

// locatedChannelBindingProvenance is created only by
// ResolveChannelBindingRoute. The resolver itself is trusted application
// infrastructure; this marker prevents a zero or manually assembled
// LocatedChannelBinding from entering the identity resolver constructor.
type locatedChannelBindingProvenance struct{}

// Snapshot returns the Binding snapshot located by the public route.
func (r LocatedChannelBinding) Snapshot() channels.BindingSnapshot {
	return r.snapshot
}

// ResolveChannelBindingRoute locates and validates one active Binding by its
// opaque public route. The route only locates the Binding; tenant and
// application identity are always taken from the returned Binding snapshot.
func ResolveChannelBindingRoute(
	ctx context.Context,
	resolver PublicRouteResolver,
	channel channels.Channel,
	publicRouteID string,
) (LocatedChannelBinding, error) {
	if resolver == nil {
		return LocatedChannelBinding{}, ErrPublicRouteResolverRequired
	}
	if err := channel.Validate(); err != nil {
		return LocatedChannelBinding{}, err
	}
	if err := channels.ValidatePublicRouteID(publicRouteID); err != nil {
		return LocatedChannelBinding{}, err
	}
	snapshot, err := resolver.ResolveBindingByPublicRoute(ctx, channel, publicRouteID)
	if err != nil {
		return LocatedChannelBinding{}, err
	}
	if snapshot.PublicRouteID != publicRouteID {
		return LocatedChannelBinding{}, channels.ErrBindingNotFound
	}
	if snapshot.Channel != channel {
		return LocatedChannelBinding{}, channels.ErrBindingChannelMismatch
	}
	if snapshot.Status != channels.BindingActive {
		return LocatedChannelBinding{}, channels.ErrBindingInactive
	}
	if err := snapshot.Validate(); err != nil {
		return LocatedChannelBinding{}, fmt.Errorf("channel binding snapshot: %w", err)
	}
	return LocatedChannelBinding{
		snapshot:   snapshot,
		provenance: &locatedChannelBindingProvenance{},
	}, nil
}

// ChannelBindingIdentityResolver adapts one located Binding snapshot to the
// Gateway tenant resolver contract. It contains no provider payload and
// cannot be constructed with a mismatched tenant or application scope.
type ChannelBindingIdentityResolver struct {
	runtimeContext tenant.RuntimeContext
	identity       AdmissionIdentity
	snapshot       channels.BindingSnapshot
}

// NewChannelBindingIdentityResolver creates a Gateway resolver from a route
// located Binding and a mapped runtime context. Provider-specific verification
// must already have succeeded before this function is called; this constructor
// does not verify provider payloads. Production callers must obtain the route
// from ResolveChannelBindingRoute, while tests may use a test route fixture.
func NewChannelBindingIdentityResolver(
	route LocatedChannelBinding,
	runtimeContext tenant.RuntimeContext,
) (*ChannelBindingIdentityResolver, error) {
	return newChannelBindingIdentityResolver(route, runtimeContext, false)
}

// NewChannelBindingInputIdentityResolver creates a Gateway resolver for a
// channel input whose user and session principals will be mapped by the
// admission transaction. The route and runtime scope still come only from the
// located Binding.
func NewChannelBindingInputIdentityResolver(
	route LocatedChannelBinding,
	runtimeContext tenant.RuntimeContext,
) (*ChannelBindingIdentityResolver, error) {
	if runtimeContext.SessionID == "" {
		runtimeContext.SessionID = channels.DefaultSessionID
	}
	return newChannelBindingIdentityResolver(route, runtimeContext, true)
}

// NewChannelBindingInputIdentityResolverFromBinding creates the same trusted
// channel identity for an event delivered by an authenticated provider
// long-connection client. The binding must come from the authoritative
// BindingSource; no public HTTP route is used by this path.
func NewChannelBindingInputIdentityResolverFromBinding(
	binding channels.BindingSnapshot,
	runtimeContext tenant.RuntimeContext,
) (*ChannelBindingIdentityResolver, error) {
	return NewChannelBindingInputIdentityResolver(
		LocatedChannelBinding{
			snapshot:   binding,
			provenance: &locatedChannelBindingProvenance{},
		},
		runtimeContext,
	)
}

func newChannelBindingIdentityResolver(
	route LocatedChannelBinding,
	runtimeContext tenant.RuntimeContext,
	mappingPending bool,
) (*ChannelBindingIdentityResolver, error) {
	if route.provenance == nil {
		return nil, errors.New("channel binding route provenance is required")
	}
	snapshot := route.snapshot
	if err := snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("channel binding snapshot: %w", err)
	}
	if snapshot.Status != channels.BindingActive {
		return nil, channels.ErrBindingInactive
	}
	if runtimeContext.TenantID != snapshot.TenantID ||
		runtimeContext.AppID != snapshot.AppID ||
		runtimeContext.Channel != string(snapshot.Channel) ||
		runtimeContext.BindingID != snapshot.BindingID {
		return nil, ErrChannelBindingScopeMismatch
	}
	// The route snapshot is authoritative for the binding generation. Do not
	// let a caller-supplied or stale runtime value become the execution scope.
	runtimeContext.BindingRevision = snapshot.BindingRevision
	if mappingPending {
		if err := validatePendingRuntimeContext(runtimeContext); err != nil {
			return nil, fmt.Errorf("pending channel binding context: %w", err)
		}
	} else if err := runtimeContext.Validate(); err != nil {
		return nil, fmt.Errorf("channel binding runtime context: %w", err)
	}
	identity := AdmissionIdentity{
		Tenant:                runtimeContext,
		Source:                TenantSourceVerifiedChannelBinding,
		SourceID:              snapshot.BindingID,
		PublicRouteID:         snapshot.PublicRouteID,
		BindingRevision:       snapshot.BindingRevision,
		channelMappingPending: mappingPending,
		channelBindingProvenance: &channelBindingProvenance{
			runtimeContext:   runtimeContext,
			source:           TenantSourceVerifiedChannelBinding,
			sourceID:         snapshot.BindingID,
			credentialDigest: CredentialDigest{},
			publicRouteID:    snapshot.PublicRouteID,
			bindingRevision:  snapshot.BindingRevision,
			mappingPending:   mappingPending,
		},
	}
	validateIdentity := identity.Validate
	if mappingPending {
		validateIdentity = identity.ValidateForChannelInput
	}
	if err := validateIdentity(); err != nil {
		return nil, fmt.Errorf("channel binding identity: %w", err)
	}
	return &ChannelBindingIdentityResolver{
		runtimeContext: runtimeContext,
		identity:       identity,
		snapshot:       snapshot,
	}, nil
}

// BindingSnapshot returns the value copy of the Binding used to build the
// trusted resolver.
func (r *ChannelBindingIdentityResolver) BindingSnapshot() channels.BindingSnapshot {
	if r == nil {
		return channels.BindingSnapshot{}
	}
	return r.snapshot
}

// ResolveTenant returns the channel binding runtime context and its trusted source.
func (r *ChannelBindingIdentityResolver) ResolveTenant(ctx context.Context) (
	tenant.RuntimeContext,
	TenantSource,
	error,
) {
	if err := contextError(ctx); err != nil {
		return tenant.RuntimeContext{}, "", err
	}
	if r == nil {
		return tenant.RuntimeContext{}, "", errors.New("channel binding identity resolver is nil")
	}
	return r.runtimeContext, TenantSourceVerifiedChannelBinding, nil
}

// ResolveAdmissionIdentity returns the channel Binding identity required by
// the atomic admission transaction.
func (r *ChannelBindingIdentityResolver) ResolveAdmissionIdentity(
	ctx context.Context,
) (AdmissionIdentity, error) {
	if err := contextError(ctx); err != nil {
		return AdmissionIdentity{}, err
	}
	if r == nil {
		return AdmissionIdentity{}, errors.New("channel binding identity resolver is nil")
	}
	return r.identity, nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	return ctx.Err()
}

var _ TenantResolver = (*ChannelBindingIdentityResolver)(nil)
var _ AdmissionIdentityResolver = (*ChannelBindingIdentityResolver)(nil)
