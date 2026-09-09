package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type incarnationCreateService struct {
	session.Service
	create func(context.Context, session.Key, session.StateMap) (*session.Session, error)
}

func (s incarnationCreateService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, _ ...session.Option) (*session.Session, error) {
	return s.create(ctx, key, state)
}

func TestSessionIncarnationCreateRejectsUntrustworthyBackendResults(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	for _, test := range []struct {
		name    string
		unbound bool
		result  func(session.Key, session.StateMap) *session.Session
		want    error
	}{
		{name: "nil result", result: func(session.Key, session.StateMap) *session.Session { return nil }, want: fence.ErrScopeMismatch},
		{name: "wrong scope", result: func(key session.Key, state session.StateMap) *session.Session {
			return session.NewSession(key.AppName, "other-user", key.SessionID, session.WithSessionState(state))
		}, want: fence.ErrScopeMismatch},
		{name: "missing UUID", result: func(key session.Key, _ session.StateMap) *session.Session {
			return session.NewSession(key.AppName, key.UserID, key.SessionID)
		}, want: ErrSessionIncarnation},
		{name: "wrong UUID", result: func(key session.Key, _ session.StateMap) *session.Session {
			return session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(session.StateMap{SessionIncarnationStateKey: []byte("00000000-0000-4000-8000-000000000001")}))
		}, want: ErrSessionIncarnation},
		{name: "malformed UUID", result: func(key session.Key, _ session.StateMap) *session.Session {
			return session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(session.StateMap{SessionIncarnationStateKey: []byte("malformed")}))
		}, want: ErrSessionIncarnation},
		{name: "unbound malformed UUID", unbound: true, result: func(key session.Key, _ session.StateMap) *session.Session {
			return session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(session.StateMap{SessionIncarnationStateKey: []byte("malformed")}))
		}, want: ErrSessionIncarnation},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, err := NewStrictFencedSessionService(incarnationCreateService{create: func(_ context.Context, key session.Key, state session.StateMap) (*session.Session, error) {
				return test.result(key, state), nil
			}}, &countingFenceAuthorizer{}, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			ctx := strictFenceContext(nil)
			if !test.unbound {
				ctx, _ = incarnationLeaseContext(t, key)
			}
			value, err := service.CreateSession(ctx, key, nil)
			if value != nil || !errors.Is(err, test.want) || SessionIncarnationFromContext(ctx) != "" {
				t.Fatalf("untrustworthy CreateSession result accepted: value=%v error=%v want=%v", value, err, test.want)
			}
		})
	}
}

func TestSessionIncarnationCreateRejectsLeaseLostDuringWrite(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	ctx, lease := incarnationLeaseContext(t, key)
	service, err := NewStrictFencedSessionService(incarnationCreateService{create: func(_ context.Context, key session.Key, state session.StateMap) (*session.Session, error) {
		if err := lease.Release(context.Background()); err != nil {
			return nil, err
		}
		return session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(state)), nil
	}}, &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	value, err := service.CreateSession(ctx, key, nil)
	if value != nil || !errors.Is(err, ErrStaleWriter) || SessionIncarnationFromContext(ctx) != "" {
		t.Fatalf("creation accepted after lease loss: value=%v error=%v", value, err)
	}
}

func TestSessionIncarnationConcurrentCreatesWriteOnlyOnce(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	ctx, _ := incarnationLeaseContext(t, key)
	var writes atomic.Int32
	entered, unblock := make(chan struct{}, 2), make(chan struct{})
	service, err := NewStrictFencedSessionService(incarnationCreateService{create: func(_ context.Context, key session.Key, state session.StateMap) (*session.Session, error) {
		writes.Add(1)
		entered <- struct{}{}
		<-unblock
		return session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(state)), nil
	}}, &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	for range 2 {
		go func() { _, err := service.CreateSession(ctx, key, nil); done <- err }()
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(unblock)
		t.Fatal("CreateSession did not start")
	}
	close(unblock)
	succeeded, rejected := 0, 0
	for range 2 {
		select {
		case err := <-done:
			if err == nil {
				succeeded++
			} else if errors.Is(err, ErrSessionIncarnation) {
				rejected++
			} else {
				t.Fatalf("unexpected concurrent creation error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent CreateSession remained blocked")
		}
	}
	if writes.Load() != 1 || succeeded != 1 || rejected != 1 || SessionIncarnationFromContext(ctx) == "" {
		t.Fatalf("concurrent creation changed incarnation: writes=%d success=%d rejected=%d", writes.Load(), succeeded, rejected)
	}
}

func TestSessionIncarnationFailedCreateDoesNotBindRetry(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	ctx, _ := incarnationLeaseContext(t, key)
	backendErr := errors.New("creation failed before commit")
	var writes int
	service, err := NewStrictFencedSessionService(incarnationCreateService{create: func(_ context.Context, key session.Key, state session.StateMap) (*session.Session, error) {
		writes++
		if writes == 1 {
			return nil, backendErr
		}
		return session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(state)), nil
	}}, &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateSession(ctx, key, nil); !errors.Is(err, backendErr) || SessionIncarnationFromContext(ctx) != "" {
		t.Fatalf("failed creation bound an incarnation: %v", err)
	}
	value, err := service.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatalf("creation could not retry after a backend failure: %v", err)
	}
	id, err := SessionIncarnationID(value)
	if err != nil || id == "" || id != SessionIncarnationFromContext(ctx) || writes != 2 {
		t.Fatalf("retried creation has an invalid incarnation: id=%q writes=%d error=%v", id, writes, err)
	}
}
