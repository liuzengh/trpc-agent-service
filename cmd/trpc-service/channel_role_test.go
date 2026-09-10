package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

type channelReadinessStub bool

func (s channelReadinessStub) Ready() bool { return bool(s) }

func TestReadinessGateRejectsCallbackWhenRoleIsNotReady(t *testing.T) {
	called := false
	handler := readinessGate{Checker: channelReadinessStub(false), Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	})}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/callbacks/feishu", nil))
	if response.Code != http.StatusServiceUnavailable || called {
		t.Fatalf("code=%d called=%t", response.Code, called)
	}

	handler.Checker = channelReadinessStub(true)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/callbacks/feishu", nil))
	if response.Code != http.StatusOK || !called {
		t.Fatalf("ready code=%d called=%t", response.Code, called)
	}
}

type candidateReaperStore struct{ calls chan struct{} }

func (s *candidateReaperStore) PutBindingRoute(context.Context, ingress.BindingRoute) error {
	return nil
}
func (s *candidateReaperStore) ResolveBindingRoute(context.Context, string, string) (ingress.BindingRoute, error) {
	return ingress.BindingRoute{}, nil
}
func (s *candidateReaperStore) IssueCandidate(context.Context, ingress.CandidateRecord) error {
	return nil
}
func (s *candidateReaperStore) AcquireCandidate(context.Context, string, int64, time.Time) (ingress.CandidateRecord, ingress.BindingRoute, error) {
	return ingress.CandidateRecord{}, ingress.BindingRoute{}, nil
}
func (s *candidateReaperStore) MarkCandidateVerified(context.Context, string, int64, string, string, time.Time) (ingress.CandidateRecord, error) {
	return ingress.CandidateRecord{}, nil
}
func (s *candidateReaperStore) PromoteCandidate(context.Context, string, int64, string, string, time.Time, time.Time) (ingress.CandidateRecord, ingress.BindingRoute, error) {
	return ingress.CandidateRecord{}, ingress.BindingRoute{}, nil
}
func (s *candidateReaperStore) BurnCandidate(context.Context, string, int64) error { return nil }
func (s *candidateReaperStore) BurnExpiredCandidates(context.Context, time.Time, int) (int, error) {
	select {
	case s.calls <- struct{}{}:
	default:
	}
	return 1, nil
}

func TestRunChannelCandidateReaperStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &candidateReaperStore{calls: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() { done <- runChannelCandidateReaper(ctx, store, time.Millisecond, 1, testLogger()) }()
	select {
	case <-store.calls:
	case <-time.After(time.Second):
		t.Fatal("candidate reaper did not run")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reaper error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("candidate reaper did not stop")
	}
}

func testLogger() *roleLogger {
	logger, err := servicelog.New(servicelog.Config{Writer: io.Discard, Level: servicelog.LevelInfo, Role: "test"})
	if err != nil {
		panic(err)
	}
	return logger
}
