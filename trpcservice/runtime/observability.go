package runtime

import (
	"context"
	"fmt"
	"time"

	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/event"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
)

// tracedSessionService instruments the actual session backend calls. The
// framework invocation tracing is disabled at the runner boundary because it
// records payloads; these spans keep the required storage boundary visible
// without exposing messages or state values.
type tracedSessionService struct {
	frameworksession.Service
	exec           worker.Execution
	metrics        *platformmetrics.Recorder
	leaseValidator worker.ExecutionLeaseValidator
}

func (s *tracedSessionService) validateLease(ctx context.Context) error {
	if s == nil || s.leaseValidator == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	lease, ok := worker.JobLeaseFromContext(ctx)
	if !ok {
		return fmt.Errorf("session execution lease is missing: %w", queue.ErrLeaseLost)
	}
	if err := s.leaseValidator.ValidateExecutionLease(ctx, s.exec, lease); err != nil {
		return fmt.Errorf("validate session execution lease: %w", err)
	}
	return nil
}

func (s *tracedSessionService) start(ctx context.Context, method string) (context.Context, trace.Span, time.Time) {
	opCtx, span := platformtelemetry.StartSpan(ctx, "session.operation",
		attribute.String("tenant_id", s.exec.Tenant.TenantID),
		attribute.String("app_id", s.exec.Tenant.AppID),
		attribute.String("config_version", s.exec.Tenant.ConfigVersion),
		attribute.String("request_id", s.exec.RequestID),
		attribute.String("backend.provider", s.exec.Config.BackendConfig.Session.Provider),
		attribute.String("session.method", method),
	)
	return opCtx, span, time.Now()
}

func (s *tracedSessionService) finish(
	ctx context.Context,
	span trace.Span,
	started time.Time,
	err error,
) {
	if err != nil {
		platformtelemetry.MarkError(span, "session", err)
	}
	span.End()
	if s.metrics == nil {
		return
	}
	errorType := ""
	if err != nil {
		errorType = "session"
	}
	s.metrics.RecordSession(ctx, platformmetrics.Labels{
		TenantID:      s.exec.Tenant.TenantID,
		AppID:         s.exec.Tenant.AppID,
		ConfigVersion: s.exec.Tenant.ConfigVersion,
		Channel:       s.exec.Tenant.Channel,
		Provider:      s.exec.Config.BackendConfig.Session.Provider,
	}, time.Since(started), errorType)
}

func (s *tracedSessionService) CreateSession(
	ctx context.Context,
	key frameworksession.Key,
	state frameworksession.StateMap,
	opts ...frameworksession.Option,
) (result *frameworksession.Session, err error) {
	if err = s.validateLease(ctx); err != nil {
		return nil, err
	}
	opCtx, span, started := s.start(ctx, "create")
	result, err = s.Service.CreateSession(opCtx, key, state, opts...)
	s.finish(opCtx, span, started, err)
	return result, err
}

func (s *tracedSessionService) GetSession(
	ctx context.Context,
	key frameworksession.Key,
	opts ...frameworksession.Option,
) (result *frameworksession.Session, err error) {
	opCtx, span, started := s.start(ctx, "get")
	result, err = s.Service.GetSession(opCtx, key, opts...)
	s.finish(opCtx, span, started, err)
	return result, err
}

func (s *tracedSessionService) ListSessions(
	ctx context.Context,
	key frameworksession.UserKey,
	opts ...frameworksession.Option,
) (result []*frameworksession.Session, err error) {
	opCtx, span, started := s.start(ctx, "list")
	result, err = s.Service.ListSessions(opCtx, key, opts...)
	s.finish(opCtx, span, started, err)
	return result, err
}

func (s *tracedSessionService) DeleteSession(
	ctx context.Context,
	key frameworksession.Key,
	opts ...frameworksession.Option,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "delete")
	err = s.Service.DeleteSession(opCtx, key, opts...)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) UpdateAppState(
	ctx context.Context,
	appName string,
	state frameworksession.StateMap,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "update_app_state")
	err = s.Service.UpdateAppState(opCtx, appName, state)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) DeleteAppState(
	ctx context.Context,
	appName string,
	key string,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "delete_app_state")
	err = s.Service.DeleteAppState(opCtx, appName, key)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) ListAppStates(
	ctx context.Context,
	appName string,
) (result frameworksession.StateMap, err error) {
	opCtx, span, started := s.start(ctx, "list_app_state")
	result, err = s.Service.ListAppStates(opCtx, appName)
	s.finish(opCtx, span, started, err)
	return result, err
}

func (s *tracedSessionService) UpdateUserState(
	ctx context.Context,
	key frameworksession.UserKey,
	state frameworksession.StateMap,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "update_user_state")
	err = s.Service.UpdateUserState(opCtx, key, state)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) ListUserStates(
	ctx context.Context,
	key frameworksession.UserKey,
) (result frameworksession.StateMap, err error) {
	opCtx, span, started := s.start(ctx, "list_user_state")
	result, err = s.Service.ListUserStates(opCtx, key)
	s.finish(opCtx, span, started, err)
	return result, err
}

func (s *tracedSessionService) DeleteUserState(
	ctx context.Context,
	key frameworksession.UserKey,
	stateKey string,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "delete_user_state")
	err = s.Service.DeleteUserState(opCtx, key, stateKey)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) UpdateSessionState(
	ctx context.Context,
	key frameworksession.Key,
	state frameworksession.StateMap,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "update_session_state")
	err = s.Service.UpdateSessionState(opCtx, key, state)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) AppendEvent(
	ctx context.Context,
	sess *frameworksession.Session,
	evt *event.Event,
	opts ...frameworksession.Option,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "append_event")
	err = s.Service.AppendEvent(opCtx, sess, evt, opts...)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) CreateSessionSummary(
	ctx context.Context,
	sess *frameworksession.Session,
	filterKey string,
	force bool,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "create_summary")
	err = s.Service.CreateSessionSummary(opCtx, sess, filterKey, force)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) EnqueueSummaryJob(
	ctx context.Context,
	sess *frameworksession.Session,
	filterKey string,
	force bool,
) (err error) {
	if err = s.validateLease(ctx); err != nil {
		return err
	}
	opCtx, span, started := s.start(ctx, "enqueue_summary")
	err = s.Service.EnqueueSummaryJob(opCtx, sess, filterKey, force)
	s.finish(opCtx, span, started, err)
	return err
}

func (s *tracedSessionService) GetSessionSummaryText(
	ctx context.Context,
	sess *frameworksession.Session,
	opts ...frameworksession.SummaryOption,
) (result string, ok bool) {
	opCtx, span, started := s.start(ctx, "get_summary")
	result, ok = s.Service.GetSessionSummaryText(opCtx, sess, opts...)
	s.finish(opCtx, span, started, nil)
	return result, ok
}

func (s *tracedSessionService) Close() (err error) {
	return s.Service.Close()
}

type tracedSessionIngestor struct {
	frameworksession.Ingestor
	exec    worker.Execution
	metrics *platformmetrics.Recorder
}

func (i *tracedSessionIngestor) IngestSession(
	ctx context.Context,
	sess *frameworksession.Session,
	opts ...frameworksession.IngestOption,
) (err error) {
	opCtx, span := platformtelemetry.StartSpan(ctx, "memory.operation",
		attribute.String("tenant_id", i.exec.Tenant.TenantID),
		attribute.String("app_id", i.exec.Tenant.AppID),
		attribute.String("config_version", i.exec.Tenant.ConfigVersion),
		attribute.String("request_id", i.exec.RequestID),
		attribute.String("backend.provider", i.exec.Config.BackendConfig.Memory.Provider),
	)
	started := time.Now()
	defer func() {
		errorType := ""
		if err != nil {
			errorType = "memory"
			platformtelemetry.MarkError(span, "memory", err)
		}
		span.End()
		if i.metrics != nil {
			i.metrics.RecordMemoryOperation(opCtx, platformmetrics.Labels{
				TenantID:      i.exec.Tenant.TenantID,
				AppID:         i.exec.Tenant.AppID,
				ConfigVersion: i.exec.Tenant.ConfigVersion,
				Channel:       i.exec.Tenant.Channel,
				Provider:      i.exec.Config.BackendConfig.Memory.Provider,
			}, "ingest", time.Since(started), errorType)
		}
	}()
	return i.Ingestor.IngestSession(opCtx, sess, opts...)
}

// tracedKnowledge instruments the actual knowledge backend search while
// keeping query text, history, and retrieved content out of telemetry.
type tracedKnowledge struct {
	frameworkknowledge.Knowledge
	exec    worker.Execution
	metrics *platformmetrics.Recorder
}

func (k *tracedKnowledge) Search(
	ctx context.Context,
	req *frameworkknowledge.SearchRequest,
) (result *frameworkknowledge.SearchResult, err error) {
	opCtx, span := platformtelemetry.StartSpan(ctx, "knowledge.search",
		attribute.String("tenant_id", k.exec.Tenant.TenantID),
		attribute.String("app_id", k.exec.Tenant.AppID),
		attribute.String("config_version", k.exec.Tenant.ConfigVersion),
		attribute.String("request_id", k.exec.RequestID),
		attribute.String("backend.provider", k.exec.Config.BackendConfig.Knowledge.Provider),
	)
	started := time.Now()
	defer func() {
		errorType := ""
		if err != nil {
			errorType = "knowledge"
			platformtelemetry.MarkError(span, "knowledge", err)
		}
		span.End()
		if k.metrics != nil {
			k.metrics.RecordKnowledge(opCtx, platformmetrics.Labels{
				TenantID:      k.exec.Tenant.TenantID,
				AppID:         k.exec.Tenant.AppID,
				ConfigVersion: k.exec.Tenant.ConfigVersion,
				Channel:       k.exec.Tenant.Channel,
				Provider:      k.exec.Config.BackendConfig.Knowledge.Provider,
			}, time.Since(started), errorType)
		}
	}()
	return k.Knowledge.Search(opCtx, req)
}
