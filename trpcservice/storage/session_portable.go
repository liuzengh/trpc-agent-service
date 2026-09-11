package storage

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const portableSummariesKey = "_platform:session_summaries_v1"
const stagingSessionPrefix = "psg_"

// A published alias points at a complete immutable import generation. Staging
// generations never replace or delete a live Session. Ordinary writes then
// continue on the current physical generation under the same resource lock.
type portableSession struct {
	session.Service
	repo      controlplane.Repository
	bindingID string
}

func (p *portableSession) aliasKey(key session.Key) string {
	return p.bindingID + "/" + resourceSubject(key.UserID, key.SessionID)
}
func (p *portableSession) nativeKey(ctx context.Context, key session.Key) (session.Key, error) {
	var result = key
	readAlias := func(ctx context.Context) (struct{}, error) {
		s, _, err := resourceState(ctx, key.AppName, "session")
		if err != nil {
			return struct{}{}, err
		}
		if id := s.Aliases[p.aliasKey(key)]; id != "" {
			result.SessionID = id
		}
		return struct{}{}, nil
	}
	var err error
	if resourceHeld(ctx, key.AppName, "session") {
		_, err = readAlias(ctx)
	} else {
		_, err = resourceAccessValue(ctx, p.repo, key.AppName, "session", resourceSubject(key.UserID, key.SessionID), false, readAlias)
	}
	return result, err
}
func hydrateSummaries(sess *session.Session) error {
	if sess == nil {
		return nil
	}
	raw, ok := sess.GetState(portableSummariesKey)
	if !ok {
		return nil
	}
	var summaries map[string]*session.Summary
	if err := json.Unmarshal(raw, &summaries); err != nil {
		return errors.New("invalid persisted Session summaries")
	}
	sess.SummariesMu.Lock()
	sess.Summaries = summaries
	sess.SummariesMu.Unlock()
	return nil
}
func (p *portableSession) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	native, err := p.nativeKey(ctx, key)
	if err != nil {
		return nil, err
	}
	sess, err := p.Service.GetSession(ctx, native, opts...)
	if err != nil || sess == nil {
		return sess, err
	}
	sess = sess.Clone()
	sess.ID = key.SessionID
	return sess, hydrateSummaries(sess)
}
func (p *portableSession) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	native, err := p.nativeKey(ctx, key)
	if err != nil {
		return nil, err
	}
	sess, err := p.Service.CreateSession(ctx, native, state, opts...)
	if err != nil || sess == nil {
		return sess, err
	}
	sess = sess.Clone()
	sess.ID = key.SessionID
	return sess, hydrateSummaries(sess)
}
func (p *portableSession) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) error {
	native, err := p.nativeKey(ctx, key)
	if err != nil {
		return err
	}
	return p.Service.DeleteSession(ctx, native, opts...)
}
func (p *portableSession) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	native, err := p.nativeKey(ctx, key)
	if err != nil {
		return err
	}
	return p.Service.UpdateSessionState(ctx, native, state)
}
func (p *portableSession) ListSessions(ctx context.Context, key session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	cfg := session.Options{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := session.ValidateListSessionsOptions(&cfg); err != nil {
		return nil, err
	}
	var nativeOpts []session.Option
	if cfg.ListSessionOnlyMeta {
		nativeOpts = append(nativeOpts, session.WithListSessionOnlyMeta())
	}
	items, err := p.Service.ListSessions(ctx, key, nativeOpts...)
	if err != nil {
		return nil, err
	}
	var aliases map[string]string
	readAliases := func(ctx context.Context) (struct{}, error) {
		s, _, err := resourceState(ctx, key.AppName, "session")
		if err != nil {
			return struct{}{}, err
		}
		aliases = map[string]string{}
		for k, v := range s.Aliases {
			aliases[k] = v
		}
		return struct{}{}, nil
	}
	if resourceHeld(ctx, key.AppName, "session") {
		_, err = readAliases(ctx)
	} else {
		_, err = resourceValue(ctx, p.repo, key.AppName, "session", "", false, readAliases)
	}
	if err != nil {
		return nil, err
	}
	reverse := map[string]string{}
	for k, v := range aliases {
		if !strings.HasPrefix(k, p.bindingID+"/") {
			continue
		}
		var pair []string
		if json.Unmarshal([]byte(strings.TrimPrefix(k, p.bindingID+"/")), &pair) == nil && len(pair) == 2 && pair[0] == key.UserID {
			reverse[v] = pair[1]
		}
	}
	var out []*session.Session
	for _, item := range items {
		if item == nil {
			continue
		}
		id := item.ID
		if mapped := reverse[id]; mapped != "" {
			id = mapped
		} else {
			if strings.HasPrefix(id, stagingSessionPrefix) || aliases[p.aliasKey(session.Key{AppName: key.AppName, UserID: key.UserID, SessionID: id})] != "" {
				continue
			}
		}
		clone := item.Clone()
		clone.ID = id
		if err := hydrateSummaries(clone); err != nil {
			return nil, err
		}
		clone.ApplyEventFiltering(opts...)
		out = append(out, clone)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if page := cfg.ListSessionPage; page != nil {
		if page.Offset >= len(out) {
			return []*session.Session{}, nil
		}
		out = out[page.Offset:]
		if page.Limit > 0 && len(out) > page.Limit {
			out = out[:page.Limit]
		}
	}
	return out, nil
}
func (p *portableSession) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, opts ...session.Option) error {
	if sess == nil {
		return session.ErrNilSession
	}
	native, err := p.nativeKey(ctx, session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID})
	if err != nil {
		return err
	}
	physical, err := p.Service.GetSession(ctx, native)
	if err != nil {
		return err
	}
	if physical == nil {
		return errors.New("session missing")
	}
	if err := p.Service.AppendEvent(ctx, physical, evt, opts...); err != nil {
		return err
	}
	sess.UpdateUserSession(evt, opts...)
	return nil
}
func (p *portableSession) CreateSessionSummary(ctx context.Context, sess *session.Session, filter string, force bool) error {
	if sess == nil {
		return session.ErrNilSession
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	native, err := p.nativeKey(ctx, key)
	if err != nil {
		return err
	}
	fresh, err := p.Service.GetSession(ctx, native)
	if err != nil {
		return err
	}
	if fresh == nil {
		return errors.New("session missing")
	}
	if err = hydrateSummaries(fresh); err != nil {
		return err
	}
	if err = p.Service.CreateSessionSummary(ctx, fresh, filter, force); err != nil {
		return err
	}
	if err = p.importSummaries(ctx, native, fresh); err != nil {
		return err
	}
	copySummaries(sess, fresh)
	return nil
}
func copySummaries(dst, src *session.Session) {
	clone := src.Clone()
	dst.SummariesMu.Lock()
	dst.Summaries = clone.Summaries
	dst.SummariesMu.Unlock()
	for _, key := range []string{portableSummariesKey, session.SummaryLastIncludedTimestampStateKey, session.SummaryLastIncludedEventIDStateKey} {
		if v, ok := src.GetState(key); ok {
			dst.SetState(key, v)
		}
	}
}
func (p *portableSession) importSummaries(ctx context.Context, native session.Key, src *session.Session) error {
	clone := src.Clone()
	raw, err := json.Marshal(clone.Summaries)
	if err != nil {
		return err
	}
	state := session.StateMap{portableSummariesKey: raw}
	for _, key := range []string{session.SummaryLastIncludedTimestampStateKey, session.SummaryLastIncludedEventIDStateKey} {
		if v, ok := src.GetState(key); ok {
			state[key] = v
		}
	}
	if err = p.Service.UpdateSessionState(ctx, native, state); err != nil {
		return err
	}
	src.SetState(portableSummariesKey, raw)
	return nil
}
func (p *portableSession) CopySummaries(ctx context.Context, key session.Key, src *session.Session) error {
	native, err := p.nativeKey(ctx, key)
	if err != nil {
		return err
	}
	return p.importSummaries(ctx, native, src)
}
func (p *portableSession) GetSessionSummaryText(_ context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	if sess == nil {
		return "", false
	}
	cfg := session.SummaryOptions{}
	for _, opt := range opts {
		opt(&cfg)
	}
	sess.SummariesMu.RLock()
	defer sess.SummariesMu.RUnlock()
	s := sess.Summaries[cfg.FilterKey]
	if s == nil {
		s = sess.Summaries[""]
	}
	if s == nil || s.Summary == "" {
		return "", false
	}
	return s.Summary, true
}
func (p *portableSession) EnqueueSummaryJob(context.Context, *session.Session, string, bool) error {
	return errors.New("summaries must use the durable platform job queue")
}
