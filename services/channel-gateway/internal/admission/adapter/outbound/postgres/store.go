package postgresadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// RouteGuard is an Adapter-only seam; routing owns its SQL and row lock.
// The composition root bridges this to the routing PostgreSQL Adapter.
type RouteGuard interface {
	VerifyGeneration(context.Context, pgx.Tx, string, string, int64) error
}

// ConnectionGuard is an Adapter-only transaction seam. Connection owns its SQL
// and holds the owner row lock until Admission commits or rolls back.
type ConnectionGuard interface {
	VerifyOwner(context.Context, pgx.Tx, string, string, int64, int64) error
}

type AccountUseGuard interface {
	AccountContext(context.Context) (context.Context, context.CancelFunc, error)
	VerifyAccount(context.Context, pgx.Tx, string, string, string, *int64) (string, error)
	RecheckAccount(context.Context, pgx.Tx) error
}

type TelegramGuard interface {
	Required(context.Context) bool
	VerifyPolling(context.Context, pgx.Tx, string, string, string, string, int64, int64) error
}
type Store struct {
	tracer          trace.Tracer
	telegramGuard   TelegramGuard
	accountGuard    AccountUseGuard
	connectionGuard ConnectionGuard
	pool            *pgxpool.Pool
	guard           RouteGuard
	budget          Budget
}

func (s *Store) WithTelegramGuard(g TelegramGuard) *Store { cp := *s; cp.telegramGuard = g; return &cp }

func (s *Store) WithTracing(t trace.Tracer) *Store { cp := *s; cp.tracer = t; return &cp }

func NewStore(pool *pgxpool.Pool, guard RouteGuard) *Store {
	return &Store{pool: pool, guard: guard, budget: DefaultBudget()}
}
func NewStoreWithBudget(pool *pgxpool.Pool, guard RouteGuard, budget Budget) (*Store, error) {
	if pool == nil {
		return nil, errors.New("nil admission database")
	}
	if err := budget.validate(); err != nil {
		return nil, err
	}
	return &Store{pool: pool, guard: guard, budget: budget}, nil
}

// WithConnectionGuard returns a configured copy; it never mutates an in-use Store.
// Existing constructors remain valid for Telegram. New WeCom events fail closed
// when this seam is absent; durable Receipt replay remains independent of it.
func (s *Store) WithConnectionGuard(guard ConnectionGuard) *Store {
	copy := *s
	copy.connectionGuard = guard
	return &copy
}

func (s *Store) WithAccountUseGuard(g AccountUseGuard) *Store {
	cp := *s
	cp.accountGuard = g
	return &cp
}

type rowReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func find(ctx context.Context, q rowReader, k domain.EventKey) (domain.Receipt, string, bool, error) {
	var raw []byte
	var digest string
	var carrier tracecontext.Carrier
	err := q.QueryRow(ctx, `SELECT i.receipt,i.source_digest,COALESCE(o.traceparent,''),COALESCE(o.tracestate,'') FROM gateway_inbox i LEFT JOIN gateway_outbox o ON o.event_id=i.receipt->>'admission_id' WHERE i.provider=$1 AND i.account_id=$2 AND i.event_id=$3`, k.Provider, k.AccountID, k.EventID).Scan(&raw, &digest, &carrier.Traceparent, &carrier.Tracestate)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Receipt{}, "", false, nil
	}
	if err != nil {
		return domain.Receipt{}, "", false, fmt.Errorf("read acceptance: %w", err)
	}
	var receipt domain.Receipt
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&receipt); err != nil {
		return domain.Receipt{}, "", false, fmt.Errorf("decode acceptance: %w", err)
	}
	if err = receipt.Validate(); err != nil {
		return domain.Receipt{}, "", false, fmt.Errorf("%w: corrupt acceptance receipt", domain.ErrUnavailable)
	}
	if sc := trace.SpanContextFromContext(carrier.Restore(context.Background())); sc.IsValid() {
		trace.SpanFromContext(ctx).AddLink(trace.Link{SpanContext: sc})
	}
	return receipt, digest, true, nil
}
func (s *Store) Find(ctx context.Context, k domain.EventKey) (domain.Receipt, string, bool, error) {
	return find(ctx, s.pool, k)
}
func (s *Store) Commit(ctx context.Context, c domain.Acceptance) (receipt domain.Receipt, resultErr error) {
	if err := c.Validate(); err != nil {
		return domain.Receipt{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Receipt{}, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	// Receipt lookup takes no account lock: replay is not new admission.
	if old, digest, found, e := find(ctx, tx, c.Input.Key); e != nil {
		return domain.Receipt{}, e
	} else if found {
		if digest != c.Input.SourceDigest {
			return domain.Receipt{}, domain.ErrConflict
		}
		return old, tx.Commit(ctx)
	}
	if s.accountGuard != nil {
		bounded, stop, e := s.accountGuard.AccountContext(ctx)
		if e != nil {
			return domain.Receipt{}, errors.Join(domain.ErrUnavailable, domain.ErrAccountUnavailable)
		}
		defer stop()
		ctx = bounded
		var tenant string
		var generation *int64
		if c.Route != nil {
			tenant = c.Route.TenantID
			generation = &c.Route.Generation
		}
		if _, e := s.accountGuard.VerifyAccount(ctx, tx, c.Input.Key.Provider, c.Input.Key.AccountID, tenant, generation); e != nil {
			return domain.Receipt{}, errors.Join(domain.ErrUnavailable, domain.ErrAccountUnavailable)
		}
	}
	key, _ := json.Marshal(c.Input.Key)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 37))`, string(key)); err != nil {
		return domain.Receipt{}, err
	}
	old, digest, found, err := find(ctx, tx, c.Input.Key)
	if err != nil {
		return domain.Receipt{}, err
	}
	if found {
		if digest != c.Input.SourceDigest {
			return domain.Receipt{}, domain.ErrConflict
		}
		return old, tx.Commit(ctx)
	}
	if err = s.chargeBudget(ctx, tx, c.Route != nil); err != nil {
		return domain.Receipt{}, err
	}
	if c.Input.TelegramFence != nil || s.telegramGuard != nil && s.telegramGuard.Required(ctx) {
		f := c.Input.TelegramFence
		if f == nil || s.telegramGuard == nil || c.Input.Key.Provider != "telegram" {
			return domain.Receipt{}, fmt.Errorf("%w: fence=%t guard=%t provider=%q", domain.ErrUnavailable, f != nil, s.telegramGuard != nil, c.Input.Key.Provider)
		}
		if err = s.telegramGuard.VerifyPolling(ctx, tx, f.ScopeID, c.Input.Key.AccountID, f.InstanceID, f.InstanceEpoch, f.Epoch, f.Revision); err != nil {
			// Keep the guard's own classification: collapsing every guard failure
			// into ErrUnavailable hides whether the permit or the lease is at fault.
			return domain.Receipt{}, errors.Join(domain.ErrUnavailable, err)
		}
	}
	if c.Input.Key.Provider == "wecom" {
		if s.connectionGuard == nil {
			return domain.Receipt{}, domain.ErrUnavailable
		}
		fence := c.Input.ConnectionFence // Validate has already required a nonnil valid fence.
		if err = s.connectionGuard.VerifyOwner(ctx, tx, c.Input.Key.AccountID, fence.InstanceID, fence.Epoch, fence.Revision); err != nil {
			// Do not leak connection configuration/driver errors, or retry this as
			// a route-generation change. This transaction (including budget) rolls back.
			return domain.Receipt{}, domain.ErrUnavailable
		}
	}
	if c.Route != nil {
		if s.guard == nil {
			return domain.Receipt{}, domain.ErrUnavailable
		}
		if err = s.guard.VerifyGeneration(ctx, tx, c.Input.Key.Provider, c.Input.Key.AccountID, c.Route.Generation); err != nil {
			return domain.Receipt{}, fmt.Errorf("%w: %v", domain.ErrRouteChanged, err)
		}
	}
	if c.Policy != nil && c.Policy.Enabled {
		if err = chargeUsageRate(ctx, tx, c.Route.TenantID, c.Input.SenderID, c.Policy.Requests.TenantPerMinute, c.Policy.Requests.UserPerMinute); err != nil {
			return domain.Receipt{}, err
		}
	}
	raw, err := json.Marshal(c.Receipt)
	if err != nil {
		return domain.Receipt{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO gateway_inbox(provider,account_id,event_id,source_digest,receipt,received_at) VALUES($1,$2,$3,$4,$5,$6)`, c.Input.Key.Provider, c.Input.Key.AccountID, c.Input.Key.EventID, c.Input.SourceDigest, raw, c.Input.ReceivedAt)
	if err != nil {
		return domain.Receipt{}, err
	}
	if c.Route != nil {
		routeJSON, err := json.Marshal(c.Route)
		if err != nil {
			return domain.Receipt{}, err
		}
		inputJSON, err := json.Marshal(c.Input)
		if err != nil {
			return domain.Receipt{}, err
		}
		var originJSON []byte
		if c.Input.ReplyOrigin != nil {
			originJSON, err = json.Marshal(c.Input.ReplyOrigin)
			if err != nil {
				return domain.Receipt{}, domain.ErrInvalidInput
			}
		}
		_, err = tx.Exec(ctx, `INSERT INTO gateway_admissions(admission_id,run_id,tenant_id,provider,account_id,event_id,route,input,reply_origin) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, c.Receipt.AdmissionID, c.Receipt.RunID, c.Route.TenantID, c.Input.Key.Provider, c.Input.Key.AccountID, c.Input.Key.EventID, routeJSON, inputJSON, originJSON)
		if err != nil {
			return domain.Receipt{}, err
		}
		payload, err := runRequestedPayload(c)
		if err != nil {
			return domain.Receipt{}, fmt.Errorf("%w: normalized execution event", domain.ErrInvalidInput)
		}
		creationCtx, creation := telemetrytrace.Start(s.tracer, ctx, "create execution.run-requested.v1", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.String("messaging.system", "nats"), attribute.String("messaging.destination.name", wire.RunRequestedSubject), attribute.String("messaging.operation.name", "create"), attribute.String("messaging.message.id", c.Receipt.AdmissionID), attribute.String("app.run.id", c.Receipt.RunID)))
		defer func() { telemetrytrace.End(creation, resultErr) }()
		carrier := tracecontext.Capture(creationCtx)
		_, err = tx.Exec(ctx, `INSERT INTO gateway_outbox(event_id,subject,payload,traceparent,tracestate) VALUES($1,$2,$3,NULLIF($4,''),NULLIF($5,''))`, c.Receipt.AdmissionID, wire.RunRequestedSubject, payload, carrier.Traceparent, carrier.Tracestate)
		if err != nil {
			return domain.Receipt{}, err
		}
	}
	if c.Input.TelegramFence != nil {
		f := c.Input.TelegramFence
		if err = s.telegramGuard.VerifyPolling(ctx, tx, f.ScopeID, c.Input.Key.AccountID, f.InstanceID, f.InstanceEpoch, f.Epoch, f.Revision); err != nil {
			// Same reason as the pre-charge check: keep the guard's classification
			// so the failure names the permit or the lease instead of collapsing
			// into an unactionable "admission temporarily unavailable".
			return domain.Receipt{}, errors.Join(domain.ErrUnavailable, err)
		}
	}
	if s.accountGuard != nil {
		if err = s.accountGuard.RecheckAccount(ctx, tx); err != nil {
			return domain.Receipt{}, errors.Join(domain.ErrUnavailable, err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.Receipt{}, err
	}
	return c.Receipt, nil
}
