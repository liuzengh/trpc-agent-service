package summary

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// CheckpointSessionService overlays platform-coordinated summaries onto the
// tenant Session returned to Runner. It deliberately does not dual-write the
// tenant backend: summary_checkpoints remains the fenced source of truth and
// every replica observes it before constructing model history.
type CheckpointSessionService struct {
	session.Service
	checkpoints CheckpointReader
	tenantID    string
	agentAppID  string
	appName     string
}

func NewCheckpointSessionService(
	delegate session.Service,
	checkpoints CheckpointReader,
	tenantID string,
	agentAppID string,
	appName string,
) (*CheckpointSessionService, error) {
	if nilInterface(delegate) || nilInterface(checkpoints) {
		return nil, fmt.Errorf("%w: Session or checkpoint dependency is not configured", ErrSummaryReadUnavailable)
	}
	probe := Key{
		TenantID: tenantID, AgentAppID: agentAppID,
		SessionOwnerID: "probe", SessionID: "probe",
	}
	if err := probe.Validate(); err != nil || !validScopedText(appName, 255, false) {
		return nil, fmt.Errorf("%w: invalid fixed summary scope", ErrSummaryScope)
	}
	return &CheckpointSessionService{
		Service: delegate, checkpoints: checkpoints,
		tenantID: tenantID, agentAppID: agentAppID, appName: appName,
	}, nil
}

func (s *CheckpointSessionService) GetSession(
	ctx context.Context,
	key session.Key,
	opts ...session.Option,
) (*session.Session, error) {
	if err := s.validateSessionKey(key); err != nil {
		return nil, err
	}
	value, err := s.Service.GetSession(ctx, key, opts...)
	if err != nil || value == nil {
		return value, err
	}
	return s.hydrate(ctx, value)
}

func (s *CheckpointSessionService) ListSessions(
	ctx context.Context,
	key session.UserKey,
	opts ...session.Option,
) ([]*session.Session, error) {
	if s == nil || s.Service == nil || key.AppName != s.appName || key.UserID == "" {
		return nil, ErrSummaryScope
	}
	values, err := s.Service.ListSessions(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	result := make([]*session.Session, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		hydrated, hydrateErr := s.hydrate(ctx, value)
		if hydrateErr != nil {
			return nil, hydrateErr
		}
		result = append(result, hydrated)
	}
	return result, nil
}

func (s *CheckpointSessionService) GetSessionSummaryText(
	ctx context.Context,
	value *session.Session,
	opts ...session.SummaryOption,
) (string, bool) {
	if value == nil {
		return "", false
	}
	hydrated, err := s.hydrate(ctx, value)
	if err != nil || hydrated == nil {
		return "", false
	}
	options := &session.SummaryOptions{FilterKey: session.SummaryFilterKeyAllContents}
	for _, option := range opts {
		if option != nil {
			option(options)
		}
	}
	hydrated.SummariesMu.RLock()
	defer hydrated.SummariesMu.RUnlock()
	if current := hydrated.Summaries[options.FilterKey]; current != nil && current.Summary != "" {
		return current.Summary, true
	}
	if options.FilterKey != session.SummaryFilterKeyAllContents {
		if current := hydrated.Summaries[session.SummaryFilterKeyAllContents]; current != nil && current.Summary != "" {
			return current.Summary, true
		}
	}
	return "", false
}

func (s *CheckpointSessionService) hydrate(ctx context.Context, value *session.Session) (*session.Session, error) {
	if value == nil {
		return nil, nil
	}
	if err := s.validateSessionKey(session.Key{AppName: value.AppName, UserID: value.UserID, SessionID: value.ID}); err != nil {
		return nil, err
	}
	incarnation, incarnationErr := storage.SessionIncarnationID(value)
	if incarnationErr != nil {
		return nil, ErrSummaryReadUnavailable
	}
	if incarnation == "" {
		return value, nil
	}
	key := Key{
		TenantID: s.tenantID, AgentAppID: s.agentAppID,
		SessionOwnerID: value.UserID, SessionID: value.ID,
		FilterKey:            session.SummaryFilterKeyAllContents,
		SessionIncarnationID: incarnation,
	}
	checkpoint, found, err := s.checkpoints.Get(nonNilContext(ctx), key)
	if err != nil {
		// The dependency error can contain a DSN or backend address. Keep the
		// user/model-visible failure stable and safe; backend telemetry owns the
		// detailed diagnostic.
		return nil, ErrSummaryReadUnavailable
	}
	if !found {
		return value, nil
	}
	if checkpoint.Key != key || checkpoint.EventSequence <= 0 || checkpoint.UpdatedAt.IsZero() || checkpoint.Validate() != nil {
		return nil, ErrSummaryReadUnavailable
	}
	clone := value.Clone()
	if clone == nil {
		return nil, ErrSummaryReadUnavailable
	}
	clone.SummariesMu.Lock()
	if clone.Summaries == nil {
		clone.Summaries = make(map[string]*session.Summary)
	}
	clone.Summaries[key.FilterKey] = &session.Summary{
		Summary:   checkpoint.Content,
		UpdatedAt: checkpoint.UpdatedAt.UTC(),
		Boundary: session.NewSummaryBoundaryWithEventID(
			key.FilterKey, checkpoint.CutoffAt.UTC(), checkpoint.LastEventID,
		),
	}
	clone.SummariesMu.Unlock()
	return clone, nil
}

func (s *CheckpointSessionService) validateSessionKey(key session.Key) error {
	if s == nil || s.Service == nil || key.AppName != s.appName || key.UserID == "" || key.SessionID == "" {
		return ErrSummaryScope
	}
	return nil
}

var _ session.Service = (*CheckpointSessionService)(nil)
