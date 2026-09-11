// Package natsadapter consumes the complete retained Control route event stream.
// It derives source identity/position from trusted JetStream metadata, not JSON.
package natsadapter

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	controlevents "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const RouteSubject = "control.channel-route.v1"

type MessageConsumer interface {
	Next(...jetstream.FetchOpt) (jetstream.Msg, error)
}
type StreamInspector interface {
	Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error)
}
type Applier interface {
	BeginReplay(context.Context, domain.ReplaySource) error
	ObserveSource(context.Context, domain.ReplaySource) error
	ApplyFromStream(context.Context, domain.StreamPosition, domain.RouteEvent) error
	Quarantine(context.Context, domain.StreamPosition, domain.QuarantineReason, string) error
}
type Consumer struct {
	consumer    MessageConsumer
	stream      StreamInspector
	applier     Applier
	sourceMu    sync.Mutex
	source      domain.ReplaySource
	initialized bool
}

func New(consumer MessageConsumer, stream StreamInspector, applier Applier) *Consumer {
	return &Consumer{consumer: consumer, stream: stream, applier: applier}
}

// Initialize captures the authoritative startup replay target before bootstrap
// exposes any Admission handler. Run reuses this initialized source. Repeating
// Initialize only extends the target monotonically and never clears quarantine.
func (c *Consumer) Initialize(ctx context.Context) error {
	if c.stream == nil || c.applier == nil {
		return errors.New("routing initialization dependencies are required")
	}
	c.sourceMu.Lock()
	defer c.sourceMu.Unlock()
	info, err := c.stream.Info(ctx)
	if err != nil {
		return err
	}
	source := sourceFromInfo(info)
	if !singleRouteSubject(info) {
		if err = c.applier.Quarantine(ctx, domain.StreamPosition{StreamName: source.StreamName, StreamID: source.StreamID}, domain.QuarantineMetadata, ""); err != nil {
			return err
		}
		return domain.ErrProjectionBlocked
	}
	if err = c.applier.BeginReplay(ctx, source); err != nil {
		return err
	}
	c.source = source
	c.initialized = true
	return nil
}
func (c *Consumer) initializedSource() (domain.ReplaySource, bool) {
	c.sourceMu.Lock()
	defer c.sourceMu.Unlock()
	return c.source, c.initialized
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.consumer == nil || c.stream == nil || c.applier == nil {
		return errors.New("routing consumer dependencies are required")
	}
	source, initialized := c.initializedSource()
	for !initialized {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := c.Initialize(callCtx)
		cancel()
		if err == nil {
			source, initialized = c.initializedSource()
			break
		}
		if errors.Is(err, domain.ErrProjectionBlocked) {
			return err
		}
		if !retryWait(ctx) {
			return ctx.Err()
		}
	}
	lastObserved := time.Now()
	for ctx.Err() == nil {
		// An idle stream still proves source reachability. Business event timestamps
		// do not measure staleness: a quiet account is not an unhealthy account.
		if time.Since(lastObserved) >= domain.SourceObservationInterval {
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.observe(callCtx, source)
			cancel()
			if err == nil {
				lastObserved = time.Now()
			} else if errors.Is(err, domain.ErrProjectionBlocked) {
				return err
			}
		}
		msg, err := c.consumer.Next(jetstream.FetchMaxWait(time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if !retryWait(ctx) {
				return ctx.Err()
			}
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		refresh := time.Since(lastObserved) >= domain.SourceObservationInterval
		err = c.process(callCtx, msg, source, refresh)
		cancel()
		if errors.Is(err, domain.ErrProjectionBlocked) {
			return err
		}
		if err != nil {
			_ = msg.NakWithDelay(time.Second)
		} else if refresh {
			lastObserved = time.Now()
		}
	}
	return ctx.Err()
}

func (c *Consumer) observe(ctx context.Context, expected domain.ReplaySource) error {
	info, err := c.stream.Info(ctx)
	if err != nil {
		return err
	}
	current := sourceFromInfo(info)
	reason := sourceFault(expected, current, info)
	if reason != "" {
		if err = c.applier.Quarantine(ctx, domain.StreamPosition{StreamName: current.StreamName, StreamID: current.StreamID}, reason, ""); err != nil {
			return err
		}
		return domain.ErrProjectionBlocked
	}
	return c.applier.ObserveSource(ctx, current)
}

func (c *Consumer) process(ctx context.Context, msg jetstream.Msg, expected domain.ReplaySource, refresh bool) error {
	metadata, err := msg.Metadata()
	if err != nil {
		return c.quarantineMessage(ctx, msg, domain.StreamPosition{StreamName: expected.StreamName, StreamID: expected.StreamID}, domain.QuarantineMetadata)
	}
	p := domain.StreamPosition{StreamName: metadata.Stream, StreamID: expected.StreamID, Sequence: metadata.Sequence.Stream}
	info, err := c.stream.Info(ctx)
	if err != nil {
		return err
	}
	current := sourceFromInfo(info)
	if reason := sourceFault(expected, current, info); reason != "" {
		return c.quarantineMessage(ctx, msg, p, reason)
	}
	if metadata.Stream != expected.StreamName || msg.Subject() != RouteSubject || p.Sequence == 0 || p.Sequence > current.LastSequence {
		return c.quarantineMessage(ctx, msg, p, domain.QuarantineMetadata)
	}
	if refresh {
		if err = c.applier.ObserveSource(ctx, current); err != nil {
			return err
		}
	}
	wire, err := controlevents.DecodeRouteProjectionEvent(msg.Data())
	if err != nil {
		return c.quarantineMessage(ctx, msg, p, domain.QuarantineInvalidSchema)
	}
	r := wire.Route
	var traffic *domain.TrafficRollout
	if r.Traffic != nil {
		t := r.Traffic.Target
		traffic = &domain.TrafficRollout{RolloutID: r.Traffic.RolloutID, PercentageBasisPoints: r.Traffic.PercentageBasisPoints, CanarySubjects: slices.Clone(r.Traffic.CanarySubjects), Target: domain.PublishedTarget{TenantID: t.TenantID, DeploymentID: t.DeploymentID, RevisionNumber: t.RevisionNumber, DeploymentRevisionID: t.DeploymentRevisionID, ManifestRef: t.ManifestRef, ManifestDigest: t.ManifestDigest}}
	}
	event := domain.RouteEvent{SchemaVersion: wire.SchemaVersion, EventID: wire.EventID, Enabled: wire.Enabled, Route: domain.RouteSnapshot{Provider: r.Provider, AccountID: r.AccountID, TenantID: r.TenantID, BindingID: r.BindingID, Generation: r.Generation, DeploymentRevisionID: r.DeploymentRevisionID, ManifestRef: r.ManifestRef, ManifestDigest: r.ManifestDigest, Traffic: traffic}}
	if err = c.applier.ApplyFromStream(ctx, p, event); err != nil {
		// Store persists permanent conflicts before returning ProjectionBlocked.
		// Transient PostgreSQL failures leave no ACK or initialization progress.
		if errors.Is(err, domain.ErrProjectionBlocked) {
			return errors.Join(err, msg.DoubleAck(ctx))
		}
		return err
	}
	return msg.DoubleAck(ctx)
}

func (c *Consumer) quarantineMessage(ctx context.Context, msg jetstream.Msg, p domain.StreamPosition, reason domain.QuarantineReason) error {
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(msg.Data()))
	if err := c.applier.Quarantine(ctx, p, reason, digest); err != nil {
		return err
	}
	return errors.Join(domain.ErrProjectionBlocked, msg.DoubleAck(ctx))
}
func sourceFromInfo(info *jetstream.StreamInfo) domain.ReplaySource {
	if info == nil {
		return domain.ReplaySource{}
	}
	return domain.ReplaySource{ObservedAt: time.Now().UTC(), StreamName: info.Config.Name, StreamID: info.Created.UTC().Format(time.RFC3339Nano), FirstSequence: info.State.FirstSeq, LastSequence: info.State.LastSeq, MessageCount: info.State.Msgs}
}
func sourceFault(expected, current domain.ReplaySource, info *jetstream.StreamInfo) domain.QuarantineReason {
	if !singleRouteSubject(info) {
		return domain.QuarantineMetadata
	}
	if expected.StreamName != current.StreamName || expected.StreamID != current.StreamID {
		return domain.QuarantineSourceChanged
	}
	if err := current.Validate(); err != nil {
		return domain.QuarantineHistoryGap
	}
	return ""
}
func singleRouteSubject(info *jetstream.StreamInfo) bool {
	return info != nil && len(info.Config.Subjects) == 1 && info.Config.Subjects[0] == RouteSubject
}
func retryWait(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
