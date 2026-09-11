package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Ledger commits the entire acceptance, including Outbox, atomically. Commit
// rechecks deduplication and route generation under the same transaction locks.
type Ledger interface {
	Find(context.Context, domain.EventKey) (domain.Receipt, string, bool, error)
	Commit(context.Context, domain.Acceptance) (domain.Receipt, error)
}
type RouteResolver interface {
	Resolve(context.Context, string, string) (domain.RouteSnapshot, error)
}
type cohortRouteResolver interface {
	ResolveFor(context.Context, string, string, string, string, string) (domain.RouteSnapshot, error)
}
type UsagePolicySource interface {
	UsagePolicy(context.Context, string) (governancev1.Policy, error)
}

// Options contains initial operating limits, not a throughput guarantee.
type Options struct {
	Tracer        trace.Tracer
	MaxConcurrent int
	Timeout       time.Duration
}
type Service struct {
	tracer   trace.Tracer
	ledger   Ledger
	routes   RouteResolver
	slots    chan struct{}
	lookups  chan struct{}
	stopping atomic.Bool
	timeout  time.Duration
	policies UsagePolicySource
}

// WithUsagePolicies binds the Control-owned source before the Service is
// exposed to ingress. A nil source keeps fixture mode permissive.
func (s *Service) WithUsagePolicies(source UsagePolicySource) *Service {
	s.policies = source
	return s
}

func New(ledger Ledger, routes RouteResolver, tracers ...trace.Tracer) *Service {
	var tracer trace.Tracer
	if len(tracers) > 1 {
		panic("one admission tracer required")
	}
	if len(tracers) == 1 {
		tracer = tracers[0]
	}
	service, err := NewWithOptions(ledger, routes, Options{MaxConcurrent: 128, Tracer: tracer})
	if err != nil {
		panic(err)
	}
	return service
}
func NewWithOptions(ledger Ledger, routes RouteResolver, options Options) (*Service, error) {
	if ledger == nil || routes == nil || options.MaxConcurrent <= 0 {
		return nil, errors.New("invalid admission configuration")
	}
	if options.Timeout == 0 {
		options.Timeout = 10 * time.Second
	}
	if options.Timeout < 0 {
		return nil, errors.New("invalid admission timeout")
	}
	return &Service{tracer: options.Tracer, ledger: ledger, routes: routes, slots: make(chan struct{}, options.MaxConcurrent), lookups: make(chan struct{}, options.MaxConcurrent), timeout: options.Timeout}, nil
}

// Stop rejects new acceptance work but permits durable receipt replay. Work
// already inside Commit is in-flight and drained by the workload HTTP shutdown.
// No process mutex is held across a database call.
func (s *Service) Stop() { s.stopping.Store(true) }
func (s *Service) AcceptInbound(ctx context.Context, in domain.Inbound) (receipt domain.Receipt, err error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "gateway.run.admit")
	defer func() {
		if receipt.RunID != "" {
			span.SetAttributes(attribute.String("app.run.id", receipt.RunID))
		}
		telemetrytrace.End(span, err)
	}()
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := in.Validate(); err != nil {
		return domain.Receipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.Receipt{}, err
	}
	// Bound concurrent receipt lookups separately so saturated new-work slots
	// still leave room for durable replay and never queue unbounded DB waiters.
	select {
	case s.lookups <- struct{}{}:
	default:
		return domain.Receipt{}, fmt.Errorf("%w: receipt lookups saturated", domain.ErrUnavailable)
	}
	receipt, digest, found, err := s.ledger.Find(ctx, in.Key)
	<-s.lookups
	if err != nil {
		return domain.Receipt{}, fmt.Errorf("acceptance receipt lookup: %w", err)
	}
	if found {
		if digest != in.SourceDigest {
			return domain.Receipt{}, domain.ErrConflict
		}
		return receipt, nil
	}
	if s.stopping.Load() {
		return s.finalReceipt(ctx, in, fmt.Errorf("%w: admission stopping", domain.ErrUnavailable))
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return s.finalReceipt(ctx, in, fmt.Errorf("%w: admission slots saturated", domain.ErrUnavailable))
	}
	for attempt := 0; attempt < 3; attempt++ {
		if s.stopping.Load() {
			return s.finalReceipt(ctx, in, domain.ErrUnavailable)
		}
		change := domain.Acceptance{Input: in, Receipt: domain.Receipt{Decision: "ignore", Reason: "unsupported_input"}}
		switch in.Kind {
		case "interaction":
			change.Receipt = domain.Receipt{Decision: "interaction", Reason: "interaction_contract_pending"}
		case "text":
			var route domain.RouteSnapshot
			var err error
			if resolver, ok := s.routes.(cohortRouteResolver); ok {
				route, err = resolver.ResolveFor(ctx, in.Key.Provider, in.Key.AccountID, in.ConversationID, in.ThreadID, in.SenderID)
			} else {
				route, err = s.routes.Resolve(ctx, in.Key.Provider, in.Key.AccountID)
			}
			if err != nil {
				return s.finalReceipt(ctx, in, fmt.Errorf("%w: route resolve", domain.ErrUnavailable))
			}
			if err = route.ValidateFor(in.Key); err != nil {
				return s.finalReceipt(ctx, in, fmt.Errorf("%w: route validate: %v", domain.ErrUnavailable, err))
			}
			policy := governancev1.Disabled(route.TenantID)
			if s.policies != nil {
				policy, err = s.policies.UsagePolicy(ctx, route.TenantID)
				if err != nil {
					return s.finalReceipt(ctx, in, fmt.Errorf("%w: usage policy fetch: %v", domain.ErrUnavailable, err))
				}
				if policy.TenantID != route.TenantID {
					return s.finalReceipt(ctx, in, fmt.Errorf("%w: usage policy tenant mismatch", domain.ErrUnavailable))
				}
				if err = policy.Validate(); err != nil {
					return s.finalReceipt(ctx, in, fmt.Errorf("%w: usage policy validate: %v", domain.ErrUnavailable, err))
				}
			}
			if !policy.Allows(route.AccountID, route.BindingID, in.SenderID, in.ConversationID) {
				return s.finalReceipt(ctx, in, domain.ErrUsageDenied)
			}
			span.SetAttributes(attribute.String("app.deployment.revision.id", route.DeploymentRevisionID))
			if route.RolloutID != "" {
				span.SetAttributes(attribute.String("app.rollout.id", route.RolloutID), attribute.String("app.rollout.variant", route.RolloutVariant))
			}
			admissionID, err := newID()
			if err != nil {
				return domain.Receipt{}, err
			}
			runID, err := newID()
			if err != nil {
				return domain.Receipt{}, err
			}
			change.Route = &route
			change.Policy = &policy
			change.Receipt = domain.Receipt{Decision: "admit-run", AdmissionID: admissionID, RunID: runID}
		}
		if s.stopping.Load() {
			return s.finalReceipt(ctx, in, domain.ErrUnavailable)
		}
		if err := ctx.Err(); err != nil {
			return domain.Receipt{}, err
		}
		receipt, err = s.ledger.Commit(ctx, change)
		if errors.Is(err, domain.ErrRouteChanged) {
			continue
		}
		if errors.Is(err, domain.ErrAccountUnavailable) {
			return s.finalReceipt(ctx, in, err)
		}
		return receipt, err
	}
	return s.finalReceipt(ctx, in, domain.ErrUnavailable)
}

// finalReceipt handles the receipt-first race in which another request commits
// after our initial miss, but a mutable preparation dependency then fails before
// our Commit can recheck deduplication. Each caller returns its result immediately:
// one final lookup at most, without a fresh timeout or an unbounded lookup queue.
func (s *Service) finalReceipt(ctx context.Context, in domain.Inbound, original error) (domain.Receipt, error) {
	if err := ctx.Err(); err != nil {
		return domain.Receipt{}, err
	}
	select {
	case s.lookups <- struct{}{}:
		defer func() { <-s.lookups }()
	default:
		return domain.Receipt{}, original
	}
	if err := ctx.Err(); err != nil {
		return domain.Receipt{}, err
	}
	// Ledger.Find is a new read outside any prior transaction, so the SQL
	// adapter obtains a fresh statement snapshot and can observe the winner.
	receipt, digest, found, err := s.ledger.Find(ctx, in.Key)
	if err != nil {
		return domain.Receipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.Receipt{}, err
	}
	if !found {
		return domain.Receipt{}, original
	}
	if digest != in.SourceDigest {
		return domain.Receipt{}, domain.ErrConflict
	}
	return receipt, nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
