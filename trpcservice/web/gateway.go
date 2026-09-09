// Package web provides the Admin API and the Gateway HTTP entry points.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var tracer = otel.Tracer("trpc-agent-service/gateway")

// dedupRollbackTimeout bounds the detached dedup rollback. It must survive the
// request context but not hang the handler goroutine on an unresponsive Redis.
const dedupRollbackTimeout = 3 * time.Second

// EnqueueHandler implements channels.Handler as the Gateway's inbound core
// (sync ack + async consume): a message is written to stream:inbound and
// acknowledged immediately; the reply is produced asynchronously by a Worker
// and delivered via stream:outbound.
//
// An empty OutboundMessage.Text means "accepted, reply follows
// asynchronously".
//
// Before enqueueing, the handler resolves tenant routing (webhook_path →
// channel_binding → tenant + app) and stamps tenant_id / app_id onto the
// message; unknown or inactive routes are rejected. Two admission defenses
// follow: a per-tenant token bucket rejects a tenant flooding
// the shared queue (noisy neighbor), and an XLEN backpressure check rejects
// everyone once the queue is nearly full. Both rejections return an error so
// the channel answers 5xx and the IM redelivers later — that redelivery is
// the retry path, and it works because the rate check runs before the dedup
// key is set while the backpressure rejection rolls it back.
type EnqueueHandler struct {
	Stream *storage.Stream
	Dedup  *storage.Deduper
	// Routes resolves webhook_path to tenant/app. Fail-closed: a nil Routes
	// (PG down at startup) rejects callbacks with an error so the IM retries
	// — unrouted messages must never bypass isolation, rate limiting, or
	// guardrails.
	Routes *tenant.Resolver
	// Limiter is the per-tenant token bucket; nil disables rate limiting.
	// DefaultQPS/DefaultBurst apply when the tenant's rate_policy is empty.
	Limiter      *storage.Limiter
	DefaultQPS   float64
	DefaultBurst int
	// BackpressureLimit is the queue length that triggers rejection
	// (XLEN >= limit). Zero means storage.BackpressureThreshold; a negative
	// value disables the check.
	BackpressureLimit int64

	InStream string // empty means storage.StreamInbound
}

func (h EnqueueHandler) inStream() string {
	if h.InStream != "" {
		return h.InStream
	}
	return storage.StreamInbound
}

func (h EnqueueHandler) backpressureLimit() int64 {
	if h.BackpressureLimit == 0 {
		return storage.BackpressureThreshold
	}
	return h.BackpressureLimit
}

// ErrOverloaded rejects a callback because admission control fired (tenant
// rate limit or queue backpressure). The channel layer answers 5xx so the
// IM redelivers later.
var ErrOverloaded = errors.New("gateway overloaded")

// Handle implements channels.Handler.
//
// Starts the root span of the message trace and stamps the message with the
// real trace ID + traceparent, so the Worker continues the same trace after
// the async Stream hop.
func (h EnqueueHandler) Handle(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	// Tenant routing first: a message with no active route is rejected before
	// consuming dedup keys or queue space. No resolver at all (PG down at
	// startup) is likewise an error — fail closed, the IM will retry.
	if h.Routes == nil {
		return channels.OutboundMessage{}, fmt.Errorf("tenant routing unavailable")
	}
	route, err := h.Routes.Resolve(ctx, msg.WebhookPath)
	if err != nil {
		return channels.OutboundMessage{}, fmt.Errorf("tenant route: %w", err)
	}
	msg.TenantID = route.Tenant.ID
	msg.AppID = route.App.ID
	msg.BindingID = route.Binding.ID

	// Per-tenant admission (before dedup: a rejected message must not consume
	// the dedup key, or the IM's redelivery would be dropped as a duplicate).
	if h.Limiter != nil {
		qps, burst := h.DefaultQPS, h.DefaultBurst
		if rl := tenant.ParseRateLimit(route.Tenant.RatePolicy); rl.QPS > 0 && rl.Burst > 0 {
			qps, burst = rl.QPS, rl.Burst
		}
		ok, err := h.Limiter.Allow(ctx, "tenant:"+msg.TenantID, qps, burst)
		if err != nil {
			// Redis hiccup: fail open — the backpressure check below still
			// bounds the queue.
			plog.Warnf("rate limit check failed, allowing: %v", err)
		} else if !ok {
			metrics.GatewayRejectedTotal.Add(ctx, 1, rejectAttr(msg, "rate_limited"))
			return channels.OutboundMessage{}, fmt.Errorf("%w: tenant %s rate limited", ErrOverloaded, msg.TenantID)
		}
	}

	// Inbound idempotency gate: first arrival passes, duplicates get
	// ErrDuplicate so the channel layer answers 200 and the IM stops
	// redelivering.
	if h.Dedup != nil {
		first, err := h.Dedup.Check(ctx, msg.Channel, msg.BindingID, msg.MsgID)
		if err != nil {
			return channels.OutboundMessage{}, fmt.Errorf("dedup check: %w", err)
		}
		if !first {
			metrics.DedupDroppedTotal.Add(ctx, 1, chAttr(msg))
			return channels.OutboundMessage{}, channels.ErrDuplicate
		}
	}

	// Rollback for anything that fails from here on: the dedup key must not
	// outlive a failed delivery attempt, or the IM redelivery that is supposed
	// to retry the message would be dropped as a duplicate.
	//
	// The rollback runs on a context detached from the request: the platform
	// drops the callback connection once its own timeout fires (WeCom gives up
	// after ~5s), and a Forget on that cancelled context would fail, leaving
	// the key to swallow every redelivery for its whole TTL. WithoutCancel
	// keeps the trace values so the rollback still lands in this span's trace.
	rollbackDedup := func() {
		if h.Dedup == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dedupRollbackTimeout)
		defer cancel()
		if err := h.Dedup.Forget(ctx, msg.Channel, msg.BindingID, msg.MsgID); err != nil {
			plog.Warnf("dedup rollback %s/%s: %v", msg.Channel, msg.MsgID, err)
		}
	}

	ctx, span := tracer.Start(ctx, "gateway.enqueue")
	defer span.End()
	span.SetAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
		attribute.String("session_key", msg.SessionKey),
		attribute.String("user_id", msg.UserID),
	)

	msg.TraceID = span.SpanContext().TraceID().String()
	carrier := propagation.MapCarrier{}
	metrics.InjectTraceparent(ctx, carrier)
	msg.TraceParent = carrier.Get("traceparent")

	// Backpressure: with the queue nearly full, reject instead of enqueueing —
	// the boundary of the no-loss guarantee is that messages are refused
	// (and retried by the IM) rather than silently truncated by MAXLEN.
	if limit := h.backpressureLimit(); limit > 0 {
		n, err := h.Stream.Len(ctx, h.inStream())
		if err != nil {
			rollbackDedup()
			return channels.OutboundMessage{}, fmt.Errorf("queue length check: %w", err)
		}
		if n >= limit {
			rollbackDedup()
			metrics.GatewayRejectedTotal.Add(ctx, 1, rejectAttr(msg, "backpressure"))
			return channels.OutboundMessage{}, fmt.Errorf("%w: queue %s at %d/%d",
				ErrOverloaded, h.inStream(), n, limit)
		}
	}

	//nolint:gosec // G117: SessionKey is a routing key on the internal stream, not a credential
	payload, err := json.Marshal(msg)
	if err != nil {
		rollbackDedup()
		return channels.OutboundMessage{}, fmt.Errorf("marshal inbound message: %w", err)
	}
	if _, err := h.Stream.Add(ctx, h.inStream(), payload); err != nil {
		// Return the error so the channel layer replies 5xx and the IM
		// retries later — the dedup key is rolled back first, otherwise that
		// retry would be swallowed.
		rollbackDedup()
		return channels.OutboundMessage{}, fmt.Errorf("enqueue inbound: %w", err)
	}
	metrics.InboundTotal.Add(ctx, 1, chAttr(msg))
	return channels.OutboundMessage{}, nil
}

func chAttr(msg channels.InboundMessage) otelmetric.AddOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
	)
}

// rejectAttr tags gateway rejections with their admission-control reason.
func rejectAttr(msg channels.InboundMessage, reason string) otelmetric.AddOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
		attribute.String("reason", reason),
	)
}
