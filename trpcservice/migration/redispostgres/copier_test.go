package redispostgres

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestVerifiedRedisSourceFailsClosed(t *testing.T) {
	t.Parallel()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	for name, stub := range map[string]*redisSourceStub{
		"list failure": {
			listErr: errors.New("redis unavailable"),
		},
		"session still exists": {
			values: []*session.Session{{ID: key.SessionID}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			source := verifiedRedisSource{Service: stub}
			if value, err := source.GetSession(context.Background(), key); err == nil || value != nil {
				t.Fatalf("GetSession() = %#v, %v; want fail closed", value, err)
			}
		})
	}
}

func TestVerifiedRedisSourceAcceptsConfirmedAbsence(t *testing.T) {
	t.Parallel()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	source := verifiedRedisSource{Service: &redisSourceStub{}}
	value, err := source.GetSession(context.Background(), key)
	if err != nil || value != nil {
		t.Fatalf("GetSession() = %#v, %v; want confirmed absence", value, err)
	}
}

func TestVerifiedRedisSourceAcceptsEmptySessionWithoutSummary(t *testing.T) {
	t.Parallel()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "empty"}
	source := verifiedRedisSource{Service: &redisSourceStub{
		value: &session.Session{ID: key.SessionID, AppName: key.AppName, UserID: key.UserID},
	}}
	summaries, err := source.GetSessionSummaries(context.Background(), key)
	if err != nil {
		t.Fatalf("GetSessionSummaries() error = %v", err)
	}
	if len(summaries) != 0 {
		t.Fatalf("GetSessionSummaries() = %#v, want empty inventory", summaries)
	}
}

type redisSourceStub struct {
	session.Service
	value   *session.Session
	getErr  error
	values  []*session.Session
	listErr error
}

func (s *redisSourceStub) GetSession(
	context.Context,
	session.Key,
	...session.Option,
) (*session.Session, error) {
	return s.value, s.getErr
}

func (s *redisSourceStub) ListSessions(
	context.Context,
	session.UserKey,
	...session.Option,
) ([]*session.Session, error) {
	return s.values, s.listErr
}
