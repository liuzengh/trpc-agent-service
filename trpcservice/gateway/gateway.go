// Package gateway resolves inbound messages, submits reliable tasks, and
// converts stored terminal results back into the synchronous Demo contract.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/scheduler"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

var (
	ErrMessageConflict      = channels.ErrIngressConflict
	ErrTaskPending          = errors.New("task is still processing")
	ErrMessagingUnavailable = errors.New("messaging dependency unavailable")
)

type Service struct {
	router     *routing.Router
	store      *messaging.Store
	consumer   string
	scheduler  *scheduler.Service
	authorizer governance.TaskAuthorizer

	running  atomic.Bool
	mu       sync.Mutex
	waiters  map[string][]chan struct{}
	cancel   context.CancelFunc
	adapters map[string]channels.Adapter
	queues   map[string]chan messaging.ReplyDelivery
	loops    sync.WaitGroup
}

const agentFailureText = "抱歉，处理失败，请稍后重试。"

// Accept is the asynchronous ingress contract used by IM adapters. It waits
// only for the Inbox and task stream durable write, never for Agent execution.
func (s *Service) Accept(ctx context.Context, inbound message.InboundMessage) (result channels.AcceptResult, finalErr error) {
	ctx, span := telemetry.Start(ctx, "gateway.submit")
	defer span.End()
	started := time.Now()
	metricTenant := "unknown"
	defer func() {
		status := "succeeded"
		if finalErr != nil {
			status = "error"
		}
		telemetry.RecordRequest(ctx, metricTenant, inbound.Channel, status, time.Since(started).Seconds())
	}()
	if s.router.TraceDigestV2Enabled() {
		if traceID := span.SpanContext().TraceID(); traceID.IsValid() {
			inbound.TraceID = traceID.String()
		}
		inbound.TraceParent = telemetry.InjectTraceParent(ctx)
		s.reuseExistingTaskTrace(ctx, &inbound)
	}
	task, err := s.router.Resolve(ctx, inbound)
	if err != nil {
		switch {
		case errors.Is(err, routing.ErrUnknownBinding):
			return channels.AcceptResult{}, executor.ErrUnknownBinding
		case errors.Is(err, routing.ErrConfigurationUnavailable):
			return channels.AcceptResult{}, executor.ErrConfigurationUnavailable
		default:
			return channels.AcceptResult{}, err
		}
	}
	metricTenant = task.TenantID
	if err := s.authorize(ctx, task); err != nil {
		return channels.AcceptResult{RequestID: task.RequestID, TraceID: task.TraceID}, err
	}
	snapshot, created, err := s.submit(ctx, task)
	if err != nil {
		if errors.Is(err, messaging.ErrConflict) {
			return channels.AcceptResult{RequestID: task.RequestID, TraceID: task.TraceID}, ErrMessageConflict
		}
		return channels.AcceptResult{RequestID: task.RequestID, TraceID: task.TraceID}, ErrMessagingUnavailable
	}
	return channels.AcceptResult{
		TaskID: snapshot.TaskID, RequestID: task.RequestID, TraceID: snapshot.TraceID,
		Duplicate: !created, Terminal: snapshot.Terminal(),
	}, nil
}

func (s *Service) Snapshot(ctx context.Context, channel, bindingID, platformMessageID string) (messaging.Snapshot, error) {
	inboxID, err := s.router.InboxID(ctx, channel, bindingID, platformMessageID)
	if err != nil {
		if errors.Is(err, routing.ErrUnknownBinding) {
			return messaging.Snapshot{}, executor.ErrUnknownBinding
		}
		return messaging.Snapshot{}, executor.ErrConfigurationUnavailable
	}
	return s.store.Snapshot(ctx, inboxID)
}

func New(router *routing.Router, store *messaging.Store, consumer string) (*Service, error) {
	return NewWithAdapters(router, store, consumer)
}

func NewWithAdapters(router *routing.Router, store *messaging.Store, consumer string, adapters ...channels.Adapter) (*Service, error) {
	return NewWithScheduler(router, store, consumer, nil, adapters...)
}

func NewWithScheduler(router *routing.Router, store *messaging.Store, consumer string, assignmentScheduler *scheduler.Service, adapters ...channels.Adapter) (*Service, error) {
	if router == nil || store == nil {
		return nil, errors.New("router and messaging store are required")
	}
	if consumer == "" {
		return nil, errors.New("gateway consumer name is required")
	}
	service := &Service{
		router: router, store: store, consumer: consumer, scheduler: assignmentScheduler, waiters: make(map[string][]chan struct{}),
		adapters: make(map[string]channels.Adapter, len(adapters)), queues: make(map[string]chan messaging.ReplyDelivery, len(adapters)),
	}
	for _, adapter := range adapters {
		if adapter == nil || adapter.Name() == "" {
			return nil, errors.New("channel adapter and name are required")
		}
		if _, exists := service.adapters[adapter.Name()]; exists {
			return nil, fmt.Errorf("duplicate channel adapter %q", adapter.Name())
		}
		service.adapters[adapter.Name()] = adapter
		service.queues[adapter.Name()] = make(chan messaging.ReplyDelivery, 64)
	}
	return service, nil
}

func (s *Service) TraceDigestV2Enabled() bool {
	return s != nil && s.router.TraceDigestV2Enabled()
}

func (s *Service) SetTaskAuthorizer(authorizer governance.TaskAuthorizer) {
	if s != nil {
		s.authorizer = authorizer
	}
}

func (s *Service) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		cancel()
		return errors.New("gateway reply consumer is already running")
	}
	s.cancel = cancel
	s.mu.Unlock()
	defer func() {
		cancel()
		s.running.Store(false)
		s.mu.Lock()
		s.cancel = nil
		s.mu.Unlock()
	}()
	if err := s.store.Ready(runCtx); err != nil {
		return err
	}
	if s.scheduler != nil {
		if err := s.scheduler.Start(runCtx); err != nil {
			return err
		}
		defer s.scheduler.Close()
	}
	adapterErrors := make(chan error, len(s.adapters))
	for name, adapter := range s.adapters {
		queue := s.queues[name]
		s.loops.Add(2)
		go func(current channels.Adapter) {
			defer s.loops.Done()
			if err := current.Start(runCtx, s); err != nil && runCtx.Err() == nil {
				select {
				case adapterErrors <- err:
				default:
				}
			}
		}(adapter)
		go func(current channels.Adapter, deliveries <-chan messaging.ReplyDelivery) {
			defer s.loops.Done()
			s.deliveryLoop(runCtx, current, deliveries)
		}(adapter, queue)
	}
	s.running.Store(true)
	for {
		if runCtx.Err() != nil {
			return nil
		}
		select {
		case err := <-adapterErrors:
			return err
		default:
		}
		claimed, err := s.store.ClaimReplies(runCtx, s.consumer, outboundClaimIdle(s.store.Config()), 16)
		if err == nil {
			for _, reply := range claimed {
				s.deliver(runCtx, reply)
			}
		}
		reply, err := s.store.ReadReply(runCtx, s.consumer, time.Second)
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if runCtx.Err() != nil {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.deliver(runCtx, reply)
	}
}

func (s *Service) deliver(ctx context.Context, delivery messaging.ReplyDelivery) {
	s.mu.Lock()
	waiters := s.waiters[delivery.Result.TaskID]
	delete(s.waiters, delivery.Result.TaskID)
	s.mu.Unlock()
	for _, waiter := range waiters {
		close(waiter)
	}
	if !delivery.Result.Target.Valid() || delivery.Result.Target.Channel == "demo" {
		if delivery.Result.Target.Channel == "demo" {
			telemetry.RecordOutbound(ctx, "demo", "acked")
		}
		_ = s.store.AckReply(ctx, delivery.StreamID)
		return
	}
	queue := s.queues[delivery.Result.Target.ChannelBindingID]
	if queue == nil {
		return
	}
	select {
	case queue <- delivery:
	default:
		// Leave the reply Pending; reclaim will retry without blocking other bindings.
	}
}

func (s *Service) deliveryLoop(ctx context.Context, adapter channels.Adapter, deliveries <-chan messaging.ReplyDelivery) {
	for {
		select {
		case <-ctx.Done():
			return
		case delivery := <-deliveries:
			s.deliverOne(ctx, adapter, delivery)
		}
	}
}

func (s *Service) deliverOne(ctx context.Context, adapter channels.Adapter, delivery messaging.ReplyDelivery) {
	ctx, span := telemetry.Start(telemetry.ExtractTraceParent(ctx, delivery.Result.TraceParent), "outbound.send")
	defer span.End()
	timeout := outboundSendTimeout(s.store.Config())
	for ctx.Err() == nil {
		state, err := s.store.BeginOutbound(ctx, delivery)
		if errors.Is(err, messaging.ErrOutboundDeferred) {
			if !waitUntil(ctx, state.NextAttemptAt) {
				return
			}
			continue
		}
		if errors.Is(err, messaging.ErrTerminal) {
			_ = s.store.AckReply(ctx, delivery.StreamID)
			return
		}
		if err != nil {
			if !waitUntil(ctx, time.Now().Add(100*time.Millisecond)) {
				return
			}
			continue
		}
		reply := delivery.Result.Target.Apply(delivery.Result.Reply)
		if !delivery.Result.Succeeded {
			reply.Text = agentFailureText
			reply.TraceID = ""
			reply.RequestID = ""
			reply.SessionID = ""
		}
		sendCtx, cancel := context.WithTimeout(ctx, timeout)
		sendErr := adapter.Send(sendCtx, reply)
		cancel()
		transitionCtx, transitionCancel := context.WithTimeout(context.Background(), timeout)
		if sendErr == nil {
			err = s.store.CompleteOutbound(transitionCtx, delivery)
			transitionCancel()
			if err == nil {
				telemetry.RecordOutbound(ctx, delivery.Result.Target.Channel, "succeeded")
				return
			}
			continue
		}
		terminal, retryErr := s.store.RetryOutbound(transitionCtx, delivery, state, "send_failed")
		telemetry.RecordOutbound(ctx, delivery.Result.Target.Channel, "send_failed")
		transitionCancel()
		if retryErr == nil && terminal {
			return
		}
	}
}

func waitUntil(ctx context.Context, target time.Time) bool {
	delay := time.Until(target)
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) Handle(ctx context.Context, inbound message.InboundMessage) (message.OutboundMessage, error) {
	if s.router.TraceDigestV2Enabled() {
		s.reuseExistingTaskTrace(ctx, &inbound)
	}
	task, err := s.router.Resolve(ctx, inbound)
	if err != nil {
		switch {
		case errors.Is(err, routing.ErrUnknownBinding):
			return message.OutboundMessage{}, executor.ErrUnknownBinding
		case errors.Is(err, routing.ErrConfigurationUnavailable):
			return message.OutboundMessage{}, executor.ErrConfigurationUnavailable
		default:
			return message.OutboundMessage{}, err
		}
	}
	if err := s.authorize(ctx, task); err != nil {
		return message.OutboundMessage{TraceID: task.TraceID}, err
	}
	snapshot, _, err := s.submit(ctx, task)
	if err != nil {
		if errors.Is(err, messaging.ErrConflict) {
			if existing, snapshotErr := s.store.Snapshot(ctx, task.InboxID()); snapshotErr == nil {
				return message.OutboundMessage{TraceID: existing.TraceID}, ErrMessageConflict
			}
			return message.OutboundMessage{}, ErrMessageConflict
		}
		return message.OutboundMessage{}, ErrMessagingUnavailable
	}
	if snapshot.Terminal() {
		return resultForRequest(snapshot, inbound.RequestID)
	}

	waiter := make(chan struct{})
	s.mu.Lock()
	s.waiters[snapshot.TaskID] = append(s.waiters[snapshot.TaskID], waiter)
	s.mu.Unlock()
	defer s.removeWaiter(snapshot.TaskID, waiter)

	timer := time.NewTimer(s.store.Config().ReplyWaitTimeout)
	poll := time.NewTicker(250 * time.Millisecond)
	defer timer.Stop()
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return message.OutboundMessage{TraceID: snapshot.TraceID}, ErrTaskPending
		case <-timer.C:
			return message.OutboundMessage{TraceID: snapshot.TraceID}, ErrTaskPending
		case <-waiter:
		case <-poll.C:
		}
		current, err := s.store.Snapshot(context.Background(), task.InboxID())
		if err == nil && current.Terminal() {
			return resultForRequest(current, inbound.RequestID)
		}
	}
}

func (s *Service) reuseExistingTaskTrace(ctx context.Context, inbound *message.InboundMessage) {
	if s == nil || s.router == nil || s.store == nil || inbound == nil {
		return
	}
	channel := inbound.Channel
	if channel == "" {
		channel = "demo"
	}
	messageID := inbound.PlatformMessageID
	if messageID == "" {
		messageID = inbound.MessageID
	}
	inboxID, err := s.router.InboxID(ctx, channel, inbound.BindingID, messageID)
	if err != nil {
		return
	}
	snapshot, err := s.store.Snapshot(ctx, inboxID)
	if err != nil {
		return
	}
	if snapshot.TraceID != "" {
		inbound.TraceID = snapshot.TraceID
	}
	if snapshot.TraceParent != "" {
		inbound.TraceParent = snapshot.TraceParent
	}
}

func (s *Service) authorize(ctx context.Context, task message.ExecutionTask) error {
	if s.authorizer == nil {
		return nil
	}
	return s.authorizer.AuthorizeTask(ctx, task)
}

func (s *Service) submit(ctx context.Context, task message.ExecutionTask) (messaging.Snapshot, bool, error) {
	if s.scheduler != nil {
		return s.scheduler.Submit(ctx, task)
	}
	return s.store.Submit(ctx, task)
}

func resultForRequest(snapshot messaging.Snapshot, requestID string) (message.OutboundMessage, error) {
	if snapshot.Result == nil {
		return message.OutboundMessage{TraceID: snapshot.TraceID}, ErrMessagingUnavailable
	}
	if snapshot.Result.Succeeded {
		reply := snapshot.Result.Reply
		reply.Channel = snapshot.Result.Channel
		reply.BindingID = snapshot.Result.BindingID
		reply.RequestID = requestID
		return reply, nil
	}
	switch snapshot.Result.ErrorCode {
	case "model_timeout":
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrAgentTimeout
	case "dependency_unavailable":
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrDependencyUnavailable
	case "configuration_unavailable", "invalid_task":
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrConfigurationUnavailable
	case "worker_lost":
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrWorkerLost
	default:
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrAgentFailed
	}
}

func (s *Service) removeWaiter(taskID string, target chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.waiters[taskID]
	for index, waiter := range current {
		if waiter == target {
			current = append(current[:index], current[index+1:]...)
			break
		}
	}
	if len(current) == 0 {
		delete(s.waiters, taskID)
	} else {
		s.waiters[taskID] = current
	}
}

func (s *Service) Ready(ctx context.Context) error {
	if !s.running.Load() {
		return errors.New("gateway reply consumer is not running")
	}
	if err := s.store.Ready(ctx); err != nil {
		return err
	}
	for _, adapter := range s.adapters {
		if err := adapter.Ready(ctx); err != nil {
			return err
		}
	}
	if s.scheduler != nil {
		if err := s.scheduler.Ready(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var result error
	for _, adapter := range s.adapters {
		result = errors.Join(result, adapter.Close())
	}
	s.loops.Wait()
	return result
}

func outboundSendTimeout(cfg config.MessagingConfig) time.Duration {
	if cfg.OutboundSendTimeout == 0 {
		return config.DefaultOutboundSendTimeout
	}
	return cfg.OutboundSendTimeout
}

func outboundClaimIdle(cfg config.MessagingConfig) time.Duration {
	if cfg.OutboundClaimIdle == 0 {
		return config.DefaultOutboundClaimIdle
	}
	return cfg.OutboundClaimIdle
}
