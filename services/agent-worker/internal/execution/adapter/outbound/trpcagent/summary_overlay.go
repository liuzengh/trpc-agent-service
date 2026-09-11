package trpcagent

import (
	"context"
	"encoding/json"
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
)

var ErrSummaryInProgress = errors.New("session summary is in progress")
var ErrSummaryClosed = errors.New("summary overlay closed")

type overlaySummary struct {
	summarizer summary.SessionSummarizer
	gate       chan struct{}
	closed     bool // guarded by overlay.mu
}

// newSummaryOverlay explicitly enables summary state in the existing candidate
// snapshot, not a new database. The caller owns the summarizer/model lifecycle.
func newSummaryOverlay(tenantID, sessionID string, accepted []byte, capacity int, summarizer summary.SessionSummarizer) (*overlay, error) {
	if nilCapabilityService(summarizer) {
		return nil, errors.New("summary requires summarizer")
	}
	s, err := newOverlayState(tenantID, sessionID, accepted, capacity, true)
	if err != nil {
		return nil, err
	}
	s.summary = &overlaySummary{summarizer: summarizer, gate: make(chan struct{}, 1)}
	return s, nil
}

// CreateSessionSummary uses SDK-owned delta, trigger and boundary behavior. Only
// the owned Session snapshot is input: caller-supplied events/state/summaries
// never replace it. The per-overlay gate serializes model calls; overlay.mu is
// released during those calls so appends/reads remain possible. Newer appended
// events stay uncovered by the older summary boundary and are never discarded.
func (s *overlay) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	if s.summary == nil {
		return nil
	} // Legacy Runner offers jobs even when disabled.
	select {
	case s.summary.gate <- struct{}{}:
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.fail(ctx.Err())
	}
	defer func() { <-s.summary.gate }()
	s.mu.Lock()
	if err := s.summaryCheck(ctx, keyFromSession(sess)); err != nil {
		s.mu.Unlock()
		return err
	}
	body, err := s.encoded()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	var detached snapshot
	if err = json.Unmarshal(body, &detached); err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.fail(err)
	}
	// v1.11.2 cannot disable idle summary workers through public options. Never
	// enqueue into this service: direct Create is synchronous and Close joins its
	// idle worker before return (SDK Stop calls wg.Wait). The worker count is a
	// lifecycle choice, not a business limit. No background summary task is queued.
	service := inmemory.NewSessionService(inmemory.WithSummarizer(s.summary.summarizer), inmemory.WithAsyncSummaryNum(1))
	defer service.Close()
	if _, err = service.CreateSession(ctx, s.key, nil); err == nil {
		err = service.CreateSessionSummary(ctx, detached.Session, filterKey, force)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		return s.fail(err)
	}
	if err = s.summaryCheck(ctx, s.key); err != nil {
		return err
	}
	// Marshal before importing: even custom summarizers retaining or modifying
	// their input cannot give the stored Session an alias to their output.
	raw, err := json.Marshal(detached.Session.Summaries)
	if err != nil {
		return s.fail(err)
	}
	var summaries map[string]*session.Summary
	if err = json.Unmarshal(raw, &summaries); err != nil {
		return s.fail(err)
	}
	old := s.stored.Summaries
	s.stored.Summaries = summaries
	if _, err = s.encoded(); err != nil {
		s.stored.Summaries = old
		return err
	}
	// SDK content processors also read Invocation.Session.Summaries directly.
	// Refresh only this output field, detached from owned state; never import
	// caller events/state or share summary/boundary pointers back to the caller.
	if sess != s.stored {
		var callerSummaries map[string]*session.Summary
		if err = json.Unmarshal(raw, &callerSummaries); err != nil {
			return s.fail(err)
		}
		sess.SummariesMu.Lock()
		sess.Summaries = callerSummaries
		sess.SummariesMu.Unlock()
	}
	return nil
}

// EnqueueSummaryJob intentionally runs synchronously and does not detach ctx.
// All filter keys are SDK same-Session views, never authorization identities.
func (s *overlay) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	return s.CreateSessionSummary(ctx, sess, filterKey, force)
}
func (s *overlay) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	if s.summary == nil {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.summaryCheck(ctx, keyFromSession(sess)); err != nil {
		return "", false
	}
	body, err := s.encoded()
	if err != nil {
		return "", false
	}
	var detached snapshot
	if err = json.Unmarshal(body, &detached); err != nil {
		s.fail(err)
		return "", false
	}
	// No summarizer means this read-only helper creates no summary workers.
	service := inmemory.NewSessionService()
	defer service.Close()
	return service.GetSessionSummaryText(ctx, detached.Session, opts...)
}
func (s *overlay) summaryCheck(ctx context.Context, key session.Key) error {
	if err := s.check(ctx, key); err != nil {
		return err
	}
	if s.summary.closed {
		return s.fail(ErrSummaryClosed)
	}
	return nil
}
func (s *overlay) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.summary != nil {
		s.summary.closed = true
	}
	return nil
}
