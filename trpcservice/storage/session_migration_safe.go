package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func copyStoredEvent(e *event.Event) *event.Event {
	if e == nil {
		return nil
	}
	out := e.Clone()
	out.ID = e.ID
	out.Version = e.Version
	out.FilterKey = e.FilterKey
	if e.StateDelta == nil {
		out.StateDelta = nil
	} else {
		out.StateDelta = cloneStateMap(e.StateDelta)
	}
	return out
}

func sameEvents(a, b *session.Session) bool {
	if len(a.Events) != len(b.Events) {
		return false
	}
	for i := range a.Events {
		if dataDigest(a.Events[i]) != dataDigest(b.Events[i]) {
			return false
		}
	}
	return true
}
func sameSummaries(a, b *session.Session) bool {
	return dataDigest(a.Clone().Summaries) == dataDigest(b.Clone().Summaries)
}
func logicalState(s session.StateMap) session.StateMap {
	out := cloneStateMap(s)
	delete(out, portableSummariesKey)
	return out
}
func sessionOnlyState(s session.StateMap) session.StateMap {
	out := cloneStateMap(s)
	for k := range out {
		if strings.HasPrefix(k, "app:") || strings.HasPrefix(k, "user:") {
			delete(out, k)
		}
	}
	return out
}

// Missing is proven by a successful metadata listing, never by matching an
// opaque backend error string or treating database unavailability as absence.
func sessionExists(ctx context.Context, service session.Service, key session.Key) (bool, error) {
	items, err := service.ListSessions(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID}, session.WithListSessionOnlyMeta())
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if item != nil && item.ID == key.SessionID {
			return true, nil
		}
	}
	return false, nil
}
func copySharedState(ctx context.Context, key session.Key, source, target session.Service) error {
	a, err := source.ListAppStates(ctx, key.AppName)
	if err != nil {
		return err
	}
	b, err := target.ListAppStates(ctx, key.AppName)
	if err != nil {
		return err
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			if err = target.DeleteAppState(ctx, key.AppName, k); err != nil {
				return err
			}
		}
	}
	if len(a) > 0 {
		if err = target.UpdateAppState(ctx, key.AppName, a); err != nil {
			return err
		}
	}
	u := session.UserKey{AppName: key.AppName, UserID: key.UserID}
	a, err = source.ListUserStates(ctx, u)
	if err != nil {
		return err
	}
	b, err = target.ListUserStates(ctx, u)
	if err != nil {
		return err
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			if err = target.DeleteUserState(ctx, u, k); err != nil {
				return err
			}
		}
	}
	if len(a) > 0 {
		return target.UpdateUserState(ctx, u, a)
	}
	return nil
}

func (r *SessionRouter) BackfillSession(ctx context.Context, tenantID, migrationID string, item SessionMigrationItem) (SessionMigrationVerification, error) {
	m, err := r.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return SessionMigrationVerification{}, err
	}
	app := "t/" + tenantID + "/a/" + m.AppID
	return resourceValue(ctx, r.repository, app, "session", resourceSubject(item.UserID, item.SessionID), false, func(ctx context.Context) (SessionMigrationVerification, error) {
		m, source, target, err := r.migrationServices(ctx, tenantID, migrationID)
		if err != nil {
			return SessionMigrationVerification{}, err
		}
		if m.State != controlplane.MigrationBackfill && m.State != controlplane.MigrationVerify && m.State != controlplane.MigrationCutover {
			return SessionMigrationVerification{}, errors.New("Session migration is not in a copy/verify state")
		}
		key := session.Key{AppName: app, UserID: item.UserID, SessionID: item.SessionID}
		exists, err := sessionExists(ctx, source, key)
		if err != nil {
			return SessionMigrationVerification{}, err
		}
		if !exists {
			if err = target.DeleteSession(ctx, key); err != nil {
				return SessionMigrationVerification{}, err
			}
			return verifySessionServices(ctx, migrationID, key, source, target)
		}
		snapshot, err := source.GetSession(ctx, key)
		if err != nil {
			return SessionMigrationVerification{}, err
		}
		if snapshot == nil {
			return SessionMigrationVerification{}, errors.New("source snapshot unavailable")
		}
		snapshot = snapshot.Clone()
		p, ok := target.(*portableSession)
		if !ok {
			return SessionMigrationVerification{}, errors.New("target cannot stage Session imports")
		}
		state, save, err := resourceState(ctx, app, "session")
		if err != nil {
			return SessionMigrationVerification{}, err
		}
		digest := dataDigest(struct {
			State     session.StateMap
			Events    any
			Summaries any
		}{snapshot.SnapshotState(), snapshot.Events, snapshot.Summaries})
		stageID := "staging/" + migrationID + "/" + resourceSubject(key.UserID, key.SessionID) + "/" + digest
		physicalID := state.Aliases[stageID]
		if physicalID == "" {
			physicalID = stagingSessionPrefix + uuid.NewString()
			state.Aliases[stageID] = physicalID
			if err = save(); err != nil {
				return SessionMigrationVerification{}, err
			}
		}
		stageKey := key
		stageKey.SessionID = physicalID
		present, err := sessionExists(ctx, p.Service, stageKey)
		if err != nil {
			return SessionMigrationVerification{}, err
		}
		var stage *session.Session
		if present {
			stage, err = p.Service.GetSession(ctx, stageKey)
		} else {
			stage, err = p.Service.CreateSession(ctx, stageKey, sessionOnlyState(snapshot.SnapshotState()))
		}
		if err != nil {
			return SessionMigrationVerification{}, err
		}
		if stage == nil || len(stage.Events) > len(snapshot.Events) {
			return SessionMigrationVerification{}, errors.New("invalid staged Session prefix")
		}
		for i := range stage.Events {
			if dataDigest(stage.Events[i]) != dataDigest(snapshot.Events[i]) {
				return SessionMigrationVerification{}, errors.New("staged Session prefix conflicts with source")
			}
		}
		copied := len(stage.Events)
		for i := copied; i < len(snapshot.Events); i++ {
			if err = p.Service.AppendEvent(ctx, stage, copyStoredEvent(&snapshot.Events[i])); err != nil {
				return SessionMigrationVerification{}, fmt.Errorf("copy Session event %d: %w", i, err)
			}
		}
		// Event state deltas may temporarily replay old values. Install the
		// authoritative final snapshot only after all events are copied.
		if err = p.Service.UpdateSessionState(ctx, stageKey, sessionOnlyState(snapshot.SnapshotState())); err != nil {
			return SessionMigrationVerification{}, err
		}
		if err = p.importSummaries(ctx, stageKey, snapshot); err != nil {
			return SessionMigrationVerification{}, err
		}
		if err = copySharedState(ctx, key, source, p.Service); err != nil {
			return SessionMigrationVerification{}, err
		}
		check, err := p.Service.GetSession(ctx, stageKey)
		if err != nil {
			return SessionMigrationVerification{}, err
		}
		if err = hydrateSummaries(check); err != nil {
			return SessionMigrationVerification{}, err
		}
		if check == nil {
			return SessionMigrationVerification{}, errors.New("staged Session missing")
		}
		if !sameEvents(snapshot, check) || !stateMapsEqual(logicalState(snapshot.State), logicalState(check.State)) || !sameSummaries(snapshot, check) {
			return SessionMigrationVerification{}, fmt.Errorf("staged Session validation failed: events=%t state=%t summaries=%t", sameEvents(snapshot, check), stateMapsEqual(logicalState(snapshot.State), logicalState(check.State)), sameSummaries(snapshot, check))
		}
		state.Aliases[p.aliasKey(key)] = physicalID
		if err = save(); err != nil {
			return SessionMigrationVerification{}, err
		}
		return verifySessionServices(ctx, migrationID, key, source, target)
	})
}

func (r *SessionRouter) VerifySession(ctx context.Context, tenantID, migrationID string, item SessionMigrationItem) (SessionMigrationVerification, error) {
	m, err := r.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return SessionMigrationVerification{}, err
	}
	app := "t/" + tenantID + "/a/" + m.AppID
	return resourceValue(ctx, r.repository, app, "session", resourceSubject(item.UserID, item.SessionID), false, func(ctx context.Context) (SessionMigrationVerification, error) {
		m, source, target, err := r.migrationServices(ctx, tenantID, migrationID)
		if err != nil {
			return SessionMigrationVerification{}, err
		}
		key := session.Key{AppName: app, UserID: item.UserID, SessionID: item.SessionID}
		result, err := verifySessionServices(ctx, migrationID, key, source, target)
		if err != nil {
			return result, err
		}
		if result.Passed {
			s, save, err := resourceState(ctx, app, "session")
			if err != nil {
				return result, err
			}
			controlplane.RecordResourceProof(s, m, resourceSubject(key.UserID, key.SessionID), dataDigest(result))
			err = save()
			return result, err
		}
		return result, nil
	})
}
