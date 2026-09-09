package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var errPollRetention = errors.New("WeCom MCP checkpoint exceeds source retention")

type WeComWindowReader interface {
	ReadWindow(context.Context, controlplane.ChannelBinding, string, time.Time, time.Time) ([]channels.InboundEnvelope, error)
}
type WeComBatchReader interface {
	ReadWindowBatch(context.Context, controlplane.ChannelBinding, string, time.Time, time.Time) (wecommcp.WindowBatch, error)
}
type PolledIntake interface {
	AcceptPolled(context.Context, controlplane.ChannelBinding, channels.InboundEnvelope) error
}
type WeComPollOptions struct {
	Targets                                         []config.WeComMCPTarget
	Interval, Window, Overlap, SettleDelay, Timeout time.Duration
	Audit                                           audit.Writer
	Metrics                                         *platformmetrics.Recorder
}
type WeComPoller struct {
	repository  controlplane.Repository
	reader      WeComWindowReader
	intake      PolledIntake
	state       wecommcp.Store
	coordinator coordination.Coordinator
	opts        WeComPollOptions
	now         func() time.Time
}

func NewWeComPoller(repository controlplane.Repository, reader WeComWindowReader, intake PolledIntake, state wecommcp.Store, coordinator coordination.Coordinator, opts WeComPollOptions) (*WeComPoller, error) {
	if repository == nil || reader == nil || intake == nil || state == nil || coordinator == nil {
		return nil, errors.New("WeCom MCP receiver dependencies required")
	}
	if opts.Interval < time.Second || opts.Window < time.Second || opts.Overlap < time.Second || opts.Window+opts.Overlap > 2*time.Minute || opts.SettleDelay < time.Second || opts.Timeout < time.Second || opts.Window%time.Second != 0 || opts.Overlap%time.Second != 0 || opts.SettleDelay%time.Second != 0 {
		return nil, errors.New("invalid WeCom MCP receiver timing")
	}
	return &WeComPoller{repository: repository, reader: reader, intake: intake, state: state, coordinator: coordinator, opts: opts, now: time.Now}, nil
}

func (p *WeComPoller) ProcessOnce(ctx context.Context) (int, error) {
	return p.processOnce(ctx, false)
}

func (p *WeComPoller) ProcessBackfillOnce(ctx context.Context) (int, error) {
	return p.processOnce(ctx, true)
}

func (p *WeComPoller) processOnce(ctx context.Context, backfill bool) (int, error) {
	count := 0
	var failures error
	for _, target := range p.opts.Targets {
		binding, err := p.repository.GetChannelBinding(ctx, target.TenantID, target.BindingID)
		if err != nil {
			failures = errors.Join(failures, p.failure(ctx, target, err))
			continue
		}
		if binding.Status != controlplane.StatusActive {
			continue
		}
		cfg, err := wecommcp.ParseBinding(binding)
		if err != nil {
			failures = errors.Join(failures, p.failure(ctx, target, err))
			continue
		}
		tenant, err := p.repository.GetTenant(ctx, binding.TenantID)
		if err != nil {
			failures = errors.Join(failures, p.failure(ctx, target, err))
			continue
		}
		app, err := p.repository.GetAgentApp(ctx, binding.TenantID, binding.AppID)
		if err != nil {
			failures = errors.Join(failures, p.failure(ctx, target, err))
			continue
		}
		if tenant.Status != controlplane.StatusActive || app.Status != controlplane.StatusActive {
			continue
		}
		for _, chat := range cfg.AllowedChatIDs {
			if ctx.Err() != nil {
				return count, context.Cause(ctx)
			}
			n, err := p.pollGroup(ctx, binding, cfg, chat, backfill)
			count += n
			if err != nil {
				failures = errors.Join(failures, err)
			}
		}
	}
	return count, failures
}

func (p *WeComPoller) pollGroup(parent context.Context, b controlplane.ChannelBinding, cfg wecommcp.BindingConfig, chat string, backfill bool) (accepted int, pollErr error) {
	ctx, cancel := context.WithTimeout(parent, p.opts.Timeout)
	defer cancel()
	ctx, span := otel.Tracer("trpc-agent-service/gateway").Start(ctx, "wecom_mcp.poll")
	defer span.End()
	defer func() {
		if pollErr != nil {
			pollErr = p.failure(ctx, config.WeComMCPTarget{TenantID: b.TenantID, BindingID: b.ID}, pollErr)
			span.SetStatus(codes.Error, "channel_poll_failed")
		}
	}()
	span.SetAttributes(attribute.String("tenant.id", b.TenantID), attribute.String("channel.binding.id", b.ID))
	h := sha256.Sum256([]byte(chat))
	chatHash := hex.EncodeToString(h[:])
	key := wecommcp.PollKey{TenantID: b.TenantID, BindingID: b.ID, ChatHash: chatHash}
	lane := "wecom-poll/"
	if backfill {
		lane = "wecom-backfill/"
	}
	lease, err := p.coordinator.Acquire(ctx, coordination.Key{AppName: lane + b.TenantID, UserID: b.ID, SessionID: chatHash})
	if err != nil {
		return 0, err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), time.Second)
		defer cancel()
		_ = lease.Release(releaseCtx)
	}()
	ctx = lease.Context()
	checkpoint, err := p.state.Checkpoint(ctx, key, wecommcp.ConfigFingerprint(b, cfg), cfg.Start())
	if err != nil {
		return 0, err
	}
	policy, err := channels.ParseMessagePolicy(b.Config)
	if err != nil {
		return 0, err
	}
	now := p.now().UTC().Truncate(time.Second)
	to := now.Add(-p.opts.SettleDelay).Truncate(time.Second)
	if !backfill && !to.After(checkpoint.Through) {
		return 0, nil
	}
	if policy.Mode == channels.ReliableMessages {
		if max := checkpoint.Through.Add(p.opts.Window); to.After(max) {
			to = max
		}
	}
	from := checkpoint.Through.Add(-p.opts.Overlap)
	if policy.Mode == channels.RealtimeMessages {
		from = now.Add(-policy.MaxAge())
	}
	if from.Before(cfg.Start()) {
		from = cfg.Start()
	}
	if from.Before(checkpoint.Floor) {
		from = checkpoint.Floor
	}
	var gap wecommcp.Gap
	if backfill {
		var found bool
		gap, found, err = p.state.NextGap(ctx, key)
		if err != nil || !found {
			return 0, err
		}
		if gap.ConfigHash != checkpoint.ConfigHash || gap.Cursor.Before(checkpoint.Floor) {
			return 0, p.state.AdvanceGap(ctx, b, key, gap, gap.Cursor, "checkpoint_changed")
		}
		from = gap.Cursor
		to = minTime(gap.RecentFrom, from.Add(p.opts.Window+p.opts.Overlap))
		if from.Before(now.Add(-7 * 24 * time.Hour)) {
			return 0, p.state.AdvanceGap(ctx, b, key, gap, from, "source_retention_exceeded")
		}
	}
	if from.Before(now.Add(-7 * 24 * time.Hour)) {
		return 0, errPollRetention
	}
	var batch wecommcp.WindowBatch
	if reader, ok := p.reader.(WeComBatchReader); ok {
		batch, err = reader.ReadWindowBatch(ctx, b, chat, from, to)
	} else {
		batch.Messages, err = p.reader.ReadWindow(ctx, b, chat, from, to)
	}
	if err != nil {
		return 0, err
	}
	count := 0
	for _, rejected := range batch.Rejected {
		rejected.TraceID = audit.TraceID(ctx)
		fresh, err := p.state.RecordRejection(ctx, key, rejected)
		if err != nil {
			return 0, err
		}
		if fresh {
			p.opts.Metrics.RecordChannelRejection(ctx, b.TenantID, rejected.Reason)
			// The rejection row itself is the durable audit fact. This secondary
			// event is useful for the unified audit view but contains no raw input.
			if p.opts.Audit != nil {
				_ = p.opts.Audit.Record(ctx, audit.Event{TenantID: b.TenantID, Channel: wecommcp.ChannelType, ChannelBindingID: b.ID, MessageID: rejected.Fingerprint, TraceID: rejected.TraceID, Decision: "channel_message_quarantined", ErrorType: rejected.Reason})
			}
		}
	}
	for _, message := range batch.Messages {
		seen, err := p.state.Seen(ctx, key, message.ExternalMessageID)
		if err != nil {
			return count, err
		}
		if seen {
			continue
		}
		if err := p.intake.AcceptPolled(ctx, b, message); err != nil {
			return count, err
		}
		// A crash between Inbox commit and this write replays the same fingerprint
		// to Inbox's unique key. MarkSeen is never allowed to precede acceptance.
		if err := p.state.MarkSeen(ctx, key, message.ExternalMessageID); err != nil {
			return count, err
		}
		count++
	}
	if err := ctx.Err(); err != nil {
		return count, err
	}
	// Optimistic version check prevents an old reader from regressing a newer
	// checkpoint even if a distributed lease was lost during an I/O pause.
	if backfill {
		err = p.state.AdvanceGap(ctx, b, key, gap, to, "")
	} else if policy.Mode == channels.RealtimeMessages {
		state, ok := p.state.(wecommcp.RealtimeStore)
		if !ok {
			return count, errors.New("realtime checkpoint store unavailable")
		}
		err = state.AdvanceRecent(ctx, b, key, checkpoint, from, to)
	} else {
		err = p.state.Advance(ctx, key, checkpoint, to)
	}
	if err != nil {
		return count, err
	}
	outcome := "ok"
	if backfill {
		outcome = "backfill_ok"
	}
	p.opts.Metrics.RecordChannelPoll(ctx, b.TenantID, outcome, now.Sub(to))
	return count, nil
}

func (p *WeComPoller) failure(ctx context.Context, target config.WeComMCPTarget, cause error) error {
	category := "channel_poll"
	switch {
	case errors.Is(cause, wecommcp.ErrStateConflict):
		category = "channel_state_conflict"
	case errors.Is(cause, wecommcp.ErrSourceContract):
		category = "channel_source_contract"
	case errors.Is(cause, errPollRetention):
		category = "channel_retention"
	case errors.Is(cause, context.DeadlineExceeded):
		category = "channel_poll_timeout"
	case errors.Is(cause, context.Canceled):
		category = "channel_poll_cancelled"
	}
	p.opts.Metrics.RecordChannelPoll(ctx, target.TenantID, "failed", 0)
	if p.opts.Audit != nil {
		auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = p.opts.Audit.Record(auditCtx, audit.Event{TenantID: target.TenantID, Channel: wecommcp.ChannelType, ChannelBindingID: target.BindingID, TraceID: audit.TraceID(ctx), Decision: "channel_poll_failed", ErrorType: category})
	}
	return errors.New("WeCom MCP receiver window failed; checkpoint not advanced")
}
func (p *WeComPoller) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Separate bounded loops and leases: a slow historical read cannot occupy
	// the recent receiver. Both paths share Inbox and channel deduplication.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(p.opts.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			_, _ = p.ProcessBackfillOnce(ctx)
		}
	}()
	defer func() { cancel(); <-done }()
	ticker := time.NewTicker(p.opts.Interval)
	defer ticker.Stop()
	for {
		_, _ = p.ProcessOnce(ctx)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
