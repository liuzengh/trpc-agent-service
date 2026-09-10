package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// ErrSummaryImportRequired reports that a Session contains or may contain
// summaries that cannot be transferred through the provider's base Session
// API without a backend-specific capability.
var ErrSummaryImportRequired = errors.New("session summary import is required")

// SummaryImporter writes existing summaries to a target backend without
// regenerating their text or boundaries.
type SummaryImporter interface {
	ReplaceSessionSummaries(context.Context, session.Key, map[string]*session.Summary) error
}

// summarySource is an optional provider capability that returns all persisted
// summaries, including when the provider's normal GetSession response omits
// them for a session with no events. It remains private so providers are not
// forced into a platform-wide Session abstraction.
type summarySource interface {
	GetSessionSummaries(context.Context, session.Key) (map[string]*session.Summary, error)
}

// RedisPostgresCopier copies the Redis-to-PostgreSQL Session migration path.
// Source and Target must be the official Redis and PostgreSQL providers for
// the fixed migration configuration versions.
type RedisPostgresCopier struct {
	Source    session.Service
	Target    session.Service
	Summaries SummaryImporter
}

// CopySession moves Event, State, Track, and Summary data for key.
func (c RedisPostgresCopier) CopySession(ctx context.Context, key session.Key) error {
	_, err := copySession(ctx, c.Source, c.Target, key, c.Summaries)
	return err
}

// VerifySession checks the complete migrated Session for key.
func (c RedisPostgresCopier) VerifySession(ctx context.Context, key session.Key) error {
	return verifySession(ctx, sessionVerification{
		source:          c.Source,
		target:          c.Target,
		key:             key,
		verifySummaries: true,
		sourceSummaries: summarySourceFor(c.Source),
		targetSummaries: targetSummarySource(c.Target, c.Summaries),
	})
}

func copySession(
	ctx context.Context,
	source, target session.Service,
	key session.Key,
	summaries SummaryImporter,
) (bool, error) {
	if source == nil || target == nil {
		return false, errors.New("source and target session services are required")
	}
	if err := key.CheckSessionKey(); err != nil {
		return false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	sourceSession, err := getCompleteSession(ctx, source, key, summarySourceFor(source))
	if err != nil {
		return false, fmt.Errorf("get source session: %w", err)
	}
	if sourceSession == nil {
		return false, nil
	}
	if sourceSession.ID != key.SessionID || sourceSession.AppName != key.AppName || sourceSession.UserID != key.UserID {
		return false, errors.New("source session does not match key")
	}
	sourceSession = sourceSession.Clone()
	if len(sourceSession.Summaries) > 0 && summaries == nil {
		return false, ErrSummaryImportRequired
	}

	targetSession, err := target.GetSession(ctx, key, session.WithEventNum(math.MaxInt))
	if err != nil {
		return false, fmt.Errorf("get target session: %w", err)
	}
	if targetSession != nil {
		if err := target.DeleteSession(ctx, key); err != nil {
			return false, fmt.Errorf("replace target session: %w", err)
		}
	}
	targetSession, err = target.CreateSession(ctx, key, cloneState(sourceSession.State))
	if err != nil {
		return false, fmt.Errorf("create target session: %w", err)
	}
	if targetSession == nil {
		return false, errors.New("created target session is required")
	}
	for index := range sourceSession.Events {
		event := sourceSession.Events[index]
		if err := target.AppendEvent(ctx, targetSession, &event); err != nil {
			return false, fmt.Errorf("append target event %d: %w", index, err)
		}
	}
	if err := copyTracks(ctx, target, targetSession, sourceSession); err != nil {
		return false, err
	}
	if err := target.UpdateSessionState(ctx, key, cloneState(sourceSession.State)); err != nil {
		return false, fmt.Errorf("update target session state: %w", err)
	}
	if len(sourceSession.Summaries) > 0 {
		if err := summaries.ReplaceSessionSummaries(ctx, key, cloneSummaries(sourceSession.Summaries)); err != nil {
			return false, fmt.Errorf("import target summaries: %w", err)
		}
	}
	return true, nil
}

type sessionVerification struct {
	source, target                   session.Service
	key                              session.Key
	verifySummaries                  bool
	sourceSummaries, targetSummaries summarySource
}

func verifySession(ctx context.Context, verification sessionVerification) error {
	if verification.source == nil || verification.target == nil {
		return errors.New("source and target session services are required")
	}
	if err := verification.key.CheckSessionKey(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sourceSession, err := getCompleteSession(ctx, verification.source, verification.key, verification.sourceSummaries)
	if err != nil {
		return fmt.Errorf("get source session: %w", err)
	}
	targetSession, err := getCompleteSession(ctx, verification.target, verification.key, verification.targetSummaries)
	if err != nil {
		return fmt.Errorf("get target session: %w", err)
	}
	if sourceSession == nil {
		if targetSession != nil {
			return errors.New("target session exists without source session")
		}
		return nil
	}
	if targetSession == nil {
		return errors.New("target session is missing")
	}
	if sourceSession.ID != targetSession.ID ||
		sourceSession.AppName != targetSession.AppName ||
		sourceSession.UserID != targetSession.UserID {
		return errors.New("target session identity does not match source")
	}
	if !equalState(sourceSession.State, targetSession.State) {
		return errors.New("target session state does not match source")
	}
	if !equalEvents(sourceSession.Events, targetSession.Events) {
		return errors.New("target session events do not match source")
	}
	if !equalTracks(sourceSession.Tracks, targetSession.Tracks) {
		return errors.New("target session tracks do not match source")
	}
	if len(sourceSession.Summaries) > 0 && !verification.verifySummaries {
		return ErrSummaryImportRequired
	}
	if verification.verifySummaries && !equalSummaries(sourceSession.Summaries, targetSession.Summaries) {
		return errors.New("target session summaries do not match source")
	}
	return nil
}

func getCompleteSession(
	ctx context.Context,
	service session.Service,
	key session.Key,
	summaries summarySource,
) (*session.Session, error) {
	value, err := service.GetSession(ctx, key, session.WithEventNum(math.MaxInt))
	if err != nil || value == nil || len(value.Summaries) > 0 {
		return value, err
	}
	if summaries == nil {
		if len(value.Events) > 0 {
			return value, nil
		}
		return nil, ErrSummaryImportRequired
	}
	if len(value.Events) > 0 {
		loaded, err := summaries.GetSessionSummaries(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read session summaries: %w", err)
		}
		value.Summaries = loaded
		return value, nil
	}
	loaded, err := summaries.GetSessionSummaries(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read session summaries: %w", err)
	}
	value.Summaries = loaded
	return value, nil
}

func summarySourceFor(value session.Service) summarySource {
	if source, ok := value.(summarySource); ok {
		return source
	}
	return nil
}

func targetSummarySource(target session.Service, importer SummaryImporter) summarySource {
	if source := summarySourceFor(target); source != nil {
		return source
	}
	if source, ok := importer.(summarySource); ok {
		return source
	}
	return nil
}

func equalEvents(left, right []event.Event) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftProjection, ok := projectEvent(left[index])
		if !ok {
			return false
		}
		rightProjection, ok := projectEvent(right[index])
		if !ok || leftProjection != rightProjection {
			return false
		}
	}
	return true
}

type eventProjection struct {
	ID        string
	Version   int
	Timestamp int64
	Content   [sha256.Size]byte
}

func projectEvent(value event.Event) (eventProjection, bool) {
	content := value
	content.Timestamp = time.Time{}
	if content.Response != nil {
		response := *content.Response
		// Response.Timestamp is a provider/runtime receive time. Event.Timestamp
		// is the persisted event time and is compared as an instant above.
		response.Timestamp = time.Time{}
		content.Response = &response
	}
	digest, ok := contentDigest(content)
	if !ok {
		return eventProjection{}, false
	}
	return eventProjection{
		ID:        value.ID,
		Version:   value.Version,
		Timestamp: timeInstant(value.Timestamp),
		Content:   digest,
	}, true
}

func equalTracks(left, right map[session.Track]*session.TrackEvents) bool {
	if len(left) != len(right) {
		return false
	}
	for track, leftHistory := range left {
		rightHistory, ok := right[track]
		if !ok || !equalTrackHistory(leftHistory, rightHistory) {
			return false
		}
	}
	return true
}

func equalTrackHistory(left, right *session.TrackEvents) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Track != right.Track || len(left.Events) != len(right.Events) {
		return false
	}
	for index := range left.Events {
		leftEvent := left.Events[index]
		rightEvent := right.Events[index]
		if leftEvent.Track != rightEvent.Track ||
			timeInstant(leftEvent.Timestamp) != timeInstant(rightEvent.Timestamp) ||
			jsonContentDigest(leftEvent.Payload) != jsonContentDigest(rightEvent.Payload) {
			return false
		}
	}
	return true
}

func copyTracks(ctx context.Context, target session.Service, targetSession, sourceSession *session.Session) error {
	if len(sourceSession.Tracks) == 0 {
		return nil
	}
	trackTarget, ok := target.(session.TrackService)
	if !ok {
		return errors.New("target session service does not support track import")
	}
	for _, history := range sourceSession.Tracks {
		if history == nil {
			continue
		}
		for index := range history.Events {
			trackEvent := history.Events[index]
			if err := trackTarget.AppendTrackEvent(ctx, targetSession, &trackEvent); err != nil {
				return fmt.Errorf("append target track event %q/%d: %w", history.Track, index, err)
			}
		}
	}
	return nil
}

func cloneSummaries(source map[string]*session.Summary) map[string]*session.Summary {
	if source == nil {
		return nil
	}
	target := make(map[string]*session.Summary, len(source))
	for key, value := range source {
		target[key] = value.Clone()
	}
	return target
}

func equalSummaries(left, right map[string]*session.Summary) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftSummary := range left {
		rightSummary, ok := right[key]
		if !ok || !equalSummary(leftSummary, rightSummary) {
			return false
		}
	}
	return true
}

func equalSummary(left, right *session.Summary) bool {
	leftProjection := projectSummary(left)
	rightProjection := projectSummary(right)
	if leftProjection == nil || rightProjection == nil {
		return leftProjection == nil && rightProjection == nil
	}
	if leftProjection.Summary != rightProjection.Summary ||
		!equalStrings(leftProjection.Topics, rightProjection.Topics) ||
		leftProjection.UpdatedAt != rightProjection.UpdatedAt {
		return false
	}
	if leftProjection.Boundary == nil || rightProjection.Boundary == nil {
		return leftProjection.Boundary == rightProjection.Boundary
	}
	return *leftProjection.Boundary == *rightProjection.Boundary
}

type summaryProjection struct {
	Summary   string
	Topics    []string
	UpdatedAt int64
	Boundary  *summaryBoundaryProjection
}

type summaryBoundaryProjection struct {
	Version     int
	FilterKey   string
	CutoffAt    int64
	LastEventID string
}

func projectSummary(value *session.Summary) *summaryProjection {
	if value == nil {
		return nil
	}
	topics := append([]string(nil), value.Topics...)
	sort.Strings(topics)
	projection := &summaryProjection{
		Summary:   value.Summary,
		Topics:    topics,
		UpdatedAt: timeInstant(value.UpdatedAt),
	}
	if value.Boundary != nil {
		projection.Boundary = &summaryBoundaryProjection{
			Version:     value.Boundary.Version,
			FilterKey:   value.Boundary.FilterKey,
			CutoffAt:    timeInstant(value.Boundary.CutoffAt),
			LastEventID: value.Boundary.LastEventID,
		}
	}
	return projection
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func contentDigest(value any) ([sha256.Size]byte, bool) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}, false
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return [sha256.Size]byte{}, false
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return [sha256.Size]byte{}, false
	}
	return sha256.Sum256(canonical), true
}

func jsonContentDigest(value json.RawMessage) [sha256.Size]byte {
	if len(value) == 0 {
		return sha256.Sum256(nil)
	}
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 {
		return sha256.Sum256(value)
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err == nil {
		var trailing any
		if err := decoder.Decode(&trailing); err == io.EOF {
			if canonical, err := json.Marshal(decoded); err == nil {
				trimmed = canonical
			}
		}
	}
	return sha256.Sum256(trimmed)
}

func timeInstant(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func cloneState(source session.StateMap) session.StateMap {
	if source == nil {
		return nil
	}
	target := make(session.StateMap, len(source))
	for key, value := range source {
		target[key] = bytes.Clone(value)
	}
	return target
}

func equalState(left, right session.StateMap) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		rightValue, ok := right[key]
		if !ok || !bytes.Equal(leftValue, rightValue) {
			return false
		}
	}
	return true
}
