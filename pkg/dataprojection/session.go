package dataprojection

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const (
	sessionKeyPrefix       = "session/v1/"
	maxSessionRecordEvents = 100000
)

type sessionIdentity struct {
	AppName   string `json:"app_name"`
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
}

type sessionTrackPayload struct {
	Track  session.Track        `json:"track"`
	Events []session.TrackEvent `json:"events"`
}

// sessionPayload is intentionally provider-neutral. App/user shared state and
// platform Summary checkpoints have their own authorities; this record moves
// the session-owned state, transcript and protocol tracks consumed by Runner.
type sessionPayload struct {
	State  session.StateMap      `json:"state"`
	Events []event.Event         `json:"events"`
	Tracks []sessionTrackPayload `json:"tracks,omitempty"`
}

type sessionTrackReader interface {
	GetTrackEvents(context.Context, session.Key, session.Track, ...session.Option) (*session.TrackEvents, error)
}

// SessionServiceResolver returns the operator-owned target service for one
// tenant and physical app name. Implementations must reject a tenant/app pair
// not present in the migration's allowlist before returning credentials.
type SessionServiceResolver func(context.Context, string, string) (session.Service, error)

// SessionProjector materializes canonical Session records through the public
// tRPC session.Service API. It never reaches into Redis or PostgreSQL private
// schemas, so the migrated transcript is readable by a normal Runner.
type SessionProjector struct {
	resolve   SessionServiceResolver
	maxEvents int
}

func NewSessionProjector(resolve SessionServiceResolver, maxEvents int) (*SessionProjector, error) {
	if resolve == nil || maxEvents <= 0 || maxEvents > maxSessionRecordEvents {
		return nil, fmt.Errorf("%w: session resolver or event bound", ErrInvalidProjection)
	}
	return &SessionProjector{resolve: resolve, maxEvents: maxEvents}, nil
}

func (p *SessionProjector) Domain() datamigration.Domain { return datamigration.DomainSession }

func (p *SessionProjector) Apply(ctx context.Context, tenantID string, record datamigration.Record) error {
	if p == nil || p.resolve == nil || p.maxEvents <= 0 {
		return fmt.Errorf("%w: session projector", ErrInvalidProjection)
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: tenant", ErrInvalidProjection)
	}
	if err := record.Validate(); err != nil {
		return err
	}
	identity, err := decodeSessionIdentity(record.Key)
	if err != nil {
		return err
	}
	key := session.Key{AppName: identity.AppName, UserID: identity.UserID, SessionID: identity.SessionID}
	if err := key.CheckSessionKey(); err != nil {
		return fmt.Errorf("%w: session identity", ErrInvalidProjection)
	}
	target, err := p.resolve(nonNilProjectionContext(ctx), tenantID, identity.AppName)
	if err != nil {
		return fmt.Errorf("resolve session projection service: %w", err)
	}
	if isNilSessionService(target) {
		return fmt.Errorf("%w: nil session target", ErrInvalidProjection)
	}
	if record.Deleted {
		if err := target.DeleteSession(nonNilProjectionContext(ctx), key); err != nil {
			return fmt.Errorf("delete projected session: %w", err)
		}
		return nil
	}
	var payload sessionPayload
	if err := decodeStrictJSON(record.Payload, &payload); err != nil {
		return fmt.Errorf("%w: session payload", ErrInvalidProjection)
	}
	if err := validateSessionPayload(payload, p.maxEvents); err != nil {
		return err
	}
	if err := applySessionPayload(nonNilProjectionContext(ctx), target, key, payload, p.maxEvents); err != nil {
		return err
	}
	projected, err := loadSessionPayload(nonNilProjectionContext(ctx), target, key, p.maxEvents)
	if err != nil {
		return fmt.Errorf("validate projected session: %w", err)
	}
	if !sessionPayloadEqual(payload, projected) {
		return fmt.Errorf("%w: projected session does not match source", ErrInvalidProjection)
	}
	return nil
}

// NewSessionRecord snapshots one real tRPC session backend into the canonical
// migration wire format. A full-page result is rejected because the public
// service API cannot distinguish an exact boundary from truncation; operators
// must increase the explicit limit instead of silently losing old events.
func NewSessionRecord(ctx context.Context, source session.Service, key session.Key, version int64, maxEvents int) (datamigration.Record, error) {
	if isNilSessionService(source) || version <= 0 || maxEvents <= 0 || maxEvents > maxSessionRecordEvents {
		return datamigration.Record{}, fmt.Errorf("%w: session source, version or event bound", ErrInvalidProjection)
	}
	if err := key.CheckSessionKey(); err != nil {
		return datamigration.Record{}, fmt.Errorf("%w: session identity", ErrInvalidProjection)
	}
	payload, err := loadSessionPayload(nonNilProjectionContext(ctx), source, key, maxEvents)
	if err != nil {
		return datamigration.Record{}, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return datamigration.Record{}, fmt.Errorf("%w: encode session payload", ErrInvalidProjection)
	}
	recordKey, err := encodeSessionIdentity(sessionIdentity{AppName: key.AppName, UserID: key.UserID, SessionID: key.SessionID})
	if err != nil {
		return datamigration.Record{}, err
	}
	record := datamigration.Record{Key: recordKey, Version: version, Payload: encoded, Hash: projectionHash(encoded)}
	if err := record.Validate(); err != nil {
		return datamigration.Record{}, err
	}
	return record, nil
}

func NewSessionTombstone(key session.Key, version int64) (datamigration.Record, error) {
	if err := key.CheckSessionKey(); err != nil || version <= 0 {
		return datamigration.Record{}, fmt.Errorf("%w: session tombstone", ErrInvalidProjection)
	}
	recordKey, err := encodeSessionIdentity(sessionIdentity{AppName: key.AppName, UserID: key.UserID, SessionID: key.SessionID})
	if err != nil {
		return datamigration.Record{}, err
	}
	record := datamigration.Record{Key: recordKey, Version: version, Payload: []byte{}, Hash: projectionHash(nil), Deleted: true}
	if err := record.Validate(); err != nil {
		return datamigration.Record{}, err
	}
	return record, nil
}

func loadSessionPayload(ctx context.Context, service session.Service, key session.Key, maxEvents int) (sessionPayload, error) {
	value, err := service.GetSession(ctx, key, session.WithEventNum(maxEvents))
	if err != nil {
		return sessionPayload{}, fmt.Errorf("read session source: %w", err)
	}
	if value == nil {
		return sessionPayload{}, fmt.Errorf("%w: session source was not found", ErrInvalidProjection)
	}
	if len(value.Events) >= maxEvents {
		return sessionPayload{}, fmt.Errorf("%w: session event bound reached", ErrInvalidProjection)
	}
	value.SummariesMu.RLock()
	hasNativeSummaries := len(value.Summaries) > 0
	value.SummariesMu.RUnlock()
	if hasNativeSummaries {
		return sessionPayload{}, fmt.Errorf("%w: native backend summaries require a dedicated importer", ErrInvalidProjection)
	}
	payload := sessionPayload{
		State:  sessionOnlyState(value.State),
		Events: append([]event.Event(nil), value.Events...),
	}
	trackNames := sessionTrackNames(value)
	if len(trackNames) == 0 {
		return payload, nil
	}
	reader, ok := service.(sessionTrackReader)
	if !ok {
		return sessionPayload{}, fmt.Errorf("%w: source track reader", ErrInvalidProjection)
	}
	payload.Tracks = make([]sessionTrackPayload, 0, len(trackNames))
	for _, track := range trackNames {
		tracked, err := reader.GetTrackEvents(ctx, key, track, session.WithEventNum(maxEvents))
		if err != nil {
			return sessionPayload{}, fmt.Errorf("read session track: %w", err)
		}
		if tracked == nil || len(tracked.Events) >= maxEvents {
			return sessionPayload{}, fmt.Errorf("%w: session track event bound reached", ErrInvalidProjection)
		}
		payload.Tracks = append(payload.Tracks, sessionTrackPayload{Track: track, Events: append([]session.TrackEvent(nil), tracked.Events...)})
	}
	return payload, nil
}

func applySessionPayload(ctx context.Context, target session.Service, key session.Key, payload sessionPayload, maxEvents int) error {
	current, err := target.GetSession(ctx, key, session.WithEventNum(maxEvents))
	if err != nil {
		return fmt.Errorf("read session target: %w", err)
	}
	if current == nil {
		current, err = target.CreateSession(ctx, key, cloneState(payload.State))
		if err != nil {
			// A concurrent idempotent projector may have won CreateSession.
			current, err = target.GetSession(ctx, key, session.WithEventNum(maxEvents))
		}
		if err != nil || current == nil {
			return fmt.Errorf("create session target: %w", err)
		}
	}
	if len(current.Events) >= maxEvents {
		return fmt.Errorf("%w: target session event bound reached", ErrInvalidProjection)
	}
	if len(current.Events) > len(payload.Events) {
		return fmt.Errorf("%w: target transcript is ahead of source", ErrInvalidProjection)
	}
	for index := range current.Events {
		if !canonicalEqual(current.Events[index], payload.Events[index]) {
			return fmt.Errorf("%w: target transcript diverged at event %d", ErrInvalidProjection, index)
		}
	}
	for index := len(current.Events); index < len(payload.Events); index++ {
		item := payload.Events[index]
		if err := target.AppendEvent(ctx, current, &item); err != nil {
			return fmt.Errorf("append projected session event: %w", err)
		}
	}
	if err := target.UpdateSessionState(ctx, key, cloneState(payload.State)); err != nil {
		return fmt.Errorf("synchronize projected session state: %w", err)
	}
	if len(payload.Tracks) > 0 {
		writer, ok := target.(session.TrackService)
		if !ok {
			return fmt.Errorf("%w: target track writer", ErrInvalidProjection)
		}
		reader, ok := target.(sessionTrackReader)
		if !ok {
			return fmt.Errorf("%w: target track reader", ErrInvalidProjection)
		}
		for _, sourceTrack := range payload.Tracks {
			targetTrack, err := reader.GetTrackEvents(ctx, key, sourceTrack.Track, session.WithEventNum(maxEvents))
			if err != nil {
				return fmt.Errorf("read projected session track: %w", err)
			}
			var existing []session.TrackEvent
			if targetTrack != nil {
				existing = targetTrack.Events
			}
			if len(existing) > len(sourceTrack.Events) {
				return fmt.Errorf("%w: target track is ahead of source", ErrInvalidProjection)
			}
			for index := range existing {
				if !canonicalEqual(existing[index], sourceTrack.Events[index]) {
					return fmt.Errorf("%w: target track diverged", ErrInvalidProjection)
				}
			}
			for index := len(existing); index < len(sourceTrack.Events); index++ {
				item := sourceTrack.Events[index]
				if err := writer.AppendTrackEvent(ctx, current, &item); err != nil {
					return fmt.Errorf("append projected session track: %w", err)
				}
			}
		}
	}
	return nil
}

func validateSessionPayload(payload sessionPayload, maxEvents int) error {
	if len(payload.Events) >= maxEvents {
		return fmt.Errorf("%w: session event bound reached", ErrInvalidProjection)
	}
	for key := range payload.State {
		if strings.HasPrefix(key, session.StateAppPrefix) || strings.HasPrefix(key, session.StateUserPrefix) {
			return fmt.Errorf("%w: shared state in session record", ErrInvalidProjection)
		}
	}
	var previous session.Track
	for index, tracked := range payload.Tracks {
		if tracked.Track == "" || len(tracked.Events) >= maxEvents || (index > 0 && tracked.Track <= previous) {
			return fmt.Errorf("%w: session track order or bound", ErrInvalidProjection)
		}
		for _, item := range tracked.Events {
			if item.Track != tracked.Track {
				return fmt.Errorf("%w: session track identity", ErrInvalidProjection)
			}
		}
		previous = tracked.Track
	}
	return nil
}

func sessionOnlyState(value session.StateMap) session.StateMap {
	result := make(session.StateMap)
	for key, item := range value {
		if strings.HasPrefix(key, session.StateAppPrefix) || strings.HasPrefix(key, session.StateUserPrefix) {
			continue
		}
		if item == nil {
			result[key] = nil
			continue
		}
		result[key] = append([]byte(nil), item...)
	}
	return result
}

func cloneState(value session.StateMap) session.StateMap { return sessionOnlyState(value) }

func sessionTrackNames(value *session.Session) []session.Track {
	set := make(map[session.Track]struct{})
	value.TracksMu.RLock()
	for name := range value.Tracks {
		set[name] = struct{}{}
	}
	value.TracksMu.RUnlock()
	if names, err := session.TracksFromState(value.State); err == nil {
		for _, name := range names {
			set[name] = struct{}{}
		}
	}
	result := make([]session.Track, 0, len(set))
	for name := range set {
		result = append(result, name)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func encodeSessionIdentity(identity sessionIdentity) (string, error) {
	key := session.Key{AppName: identity.AppName, UserID: identity.UserID, SessionID: identity.SessionID}
	if err := key.CheckSessionKey(); err != nil {
		return "", fmt.Errorf("%w: session identity", ErrInvalidProjection)
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("%w: encode session identity", ErrInvalidProjection)
	}
	return sessionKeyPrefix + base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeSessionIdentity(key string) (sessionIdentity, error) {
	if !strings.HasPrefix(key, sessionKeyPrefix) {
		return sessionIdentity{}, fmt.Errorf("%w: session record key", ErrInvalidProjection)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(key, sessionKeyPrefix))
	if err != nil {
		return sessionIdentity{}, fmt.Errorf("%w: session record key", ErrInvalidProjection)
	}
	var identity sessionIdentity
	if err := decodeStrictJSON(raw, &identity); err != nil {
		return sessionIdentity{}, fmt.Errorf("%w: session record key", ErrInvalidProjection)
	}
	canonical, err := encodeSessionIdentity(identity)
	if err != nil || canonical != key {
		return sessionIdentity{}, fmt.Errorf("%w: non-canonical session record key", ErrInvalidProjection)
	}
	return identity, nil
}

func sessionPayloadEqual(left, right sessionPayload) bool {
	return canonicalEqual(left, right)
}

func canonicalEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func isNilSessionService(value session.Service) bool {
	if value == nil {
		return true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}

var _ Projector = (*SessionProjector)(nil)
