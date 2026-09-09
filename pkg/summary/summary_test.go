package summary

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(value time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(value)
	c.mu.Unlock()
}

func summaryKey() Key {
	return Key{TenantID: "tenant-a", AgentAppID: "support", SessionOwnerID: "owner-1", SessionID: "session-1"}
}

func summaryRequest(key Key, sequence int64) EnqueueRequest {
	return EnqueueRequest{Key: key, AgentVersionID: "version-1", TargetEventSequence: sequence}
}

func candidateFor(key Key, sequence int64, content string) Candidate {
	return Candidate{
		Key: key, EventSequence: sequence, Content: content, ContentSHA256: HashContent(content),
		CutoffAt: time.Unix(sequence, 0).UTC(), LastEventID: fmt.Sprintf("event-%d", sequence),
	}
}

func TestKeyValidationMatchesPersistedBoundsAndControlPolicy(t *testing.T) {
	tooLong := summaryKey()
	tooLong.AgentAppID = strings.Repeat("a", MaxAgentAppIDBytes+1)
	if err := tooLong.Validate(); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("oversized agent app id accepted: %v", err)
	}
	unsafe := summaryKey()
	unsafe.SessionID = "session\u202e1"
	if err := unsafe.Validate(); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("format-control session id accepted: %v", err)
	}
}

func TestJobValidationMirrorsCompletionSequenceInvariant(t *testing.T) {
	job := Job{ID: 1, Key: summaryKey(), AgentVersionID: "version-1", TargetEventSequence: 4, MaxAttempts: 8,
		Status: StatusPending, CompletedEventSequence: 5}
	if err := job.Validate(); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("non-completed job exceeded target: %v", err)
	}
	job.Status = StatusCompleted
	job.CompletedEventSequence = 3
	if err := job.Validate(); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("incomplete completed job accepted: %v", err)
	}
}

func TestMemoryStoreCoalescesAndIsolatesJobs(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	first, err := store.Enqueue(context.Background(), summaryRequest(summaryKey(), 3))
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.Job.ID != 1 {
		t.Fatalf("first enqueue=%#v", first)
	}
	second, err := store.Enqueue(context.Background(), summaryRequest(summaryKey(), 8))
	if err != nil {
		t.Fatal(err)
	}
	if second.Created || !second.Coalesced || second.Job.TargetEventSequence != 8 {
		t.Fatalf("coalesced enqueue=%#v", second)
	}
	other := summaryKey()
	other.FilterKey = "user-messages"
	third, err := store.Enqueue(context.Background(), summaryRequest(other, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !third.Created || third.Job.ID != 2 {
		t.Fatalf("isolated enqueue=%#v", third)
	}
}

func TestMemoryStorePinsVersionAndRejectsSameTargetVersionConflict(t *testing.T) {
	clock := &testClock{now: time.Unix(150, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	first, err := store.Enqueue(context.Background(), summaryRequest(summaryKey(), 3))
	if err != nil {
		t.Fatal(err)
	}
	if first.Job.AgentVersionID != "version-1" {
		t.Fatalf("pinned version=%q", first.Job.AgentVersionID)
	}
	conflict := summaryRequest(summaryKey(), 3)
	conflict.AgentVersionID = "version-2"
	if _, err := store.Enqueue(context.Background(), conflict); !errors.Is(err, ErrSummaryVersionConflict) {
		t.Fatalf("same-target version conflict=%v", err)
	}
	newer := summaryRequest(summaryKey(), 4)
	newer.AgentVersionID = "version-2"
	result, err := store.Enqueue(context.Background(), newer)
	if err != nil {
		t.Fatal(err)
	}
	if result.Job.TargetEventSequence != 4 || result.Job.AgentVersionID != "version-2" {
		t.Fatalf("newer pinned job=%#v", result.Job)
	}
}

func TestMemoryStoreAcceptsUnresolvedTargetAndBindsItUnderLease(t *testing.T) {
	clock := &testClock{now: time.Unix(175, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	result, err := store.Enqueue(context.Background(), summaryRequest(summaryKey(), 0))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := store.ResolveTarget(context.Background(), claimed, 7)
	if err != nil {
		t.Fatal(err)
	}
	if result.Job.TargetEventSequence != 0 || bound.TargetEventSequence != 7 || bound.AgentVersionID != "version-1" {
		t.Fatalf("unresolved=%#v bound=%#v", result.Job, bound)
	}
	replayed, err := store.ResolveTarget(context.Background(), claimed, 8)
	if err != nil || replayed.TargetEventSequence != 7 {
		t.Fatalf("replayed target bind=%#v err=%v", replayed, err)
	}
}

func TestMemoryStoreFenceExpiryAndRetryLimit(t *testing.T) {
	clock := &testClock{now: time.Unix(200, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	request := summaryRequest(summaryKey(), 1)
	request.MaxAttempts = 2
	result, err := store.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), claimed, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Renew(context.Background(), claimed, time.Second); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("renew after complete=%v", err)
	}

	request = summaryRequest(summaryKey(), 2)
	request.Force = true
	result, err = store.Enqueue(context.Background(), request)
	if err != nil || result.Job.Status != StatusPending {
		t.Fatalf("force enqueue job=%#v err=%v", result.Job, err)
	}
	claimed, err = store.Claim(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if _, err := store.Fail(context.Background(), claimed, errors.New("temporary"), clock.Now().Add(time.Second)); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("expired fail=%v", err)
	}
	claimed, err = store.Claim(context.Background(), "worker-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := store.Fail(context.Background(), claimed, errors.New("permanent"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != StatusFailed || failed.Attempts != 2 {
		t.Fatalf("failed=%#v", failed)
	}
	if _, err := store.Claim(context.Background(), "worker-c", time.Second); !errors.Is(err, ErrNoWork) {
		t.Fatalf("claim after attempt limit=%v", err)
	}
	_ = result
}

func TestMemorySinkCAS(t *testing.T) {
	clock := &testClock{now: time.Unix(300, 0).UTC()}
	sink := NewMemorySink(clock.Now)
	key := summaryKey()
	first, err := sink.Publish(context.Background(), candidateFor(key, 2, "two"))
	if err != nil || !first.Applied {
		t.Fatalf("first publish=%#v err=%v", first, err)
	}
	retry, err := sink.Publish(context.Background(), candidateFor(key, 2, "two"))
	if err != nil || retry.Applied {
		t.Fatalf("idempotent publish=%#v err=%v", retry, err)
	}
	if _, err := sink.Publish(context.Background(), candidateFor(key, 2, "different")); !errors.Is(err, ErrSummaryConflict) {
		t.Fatalf("same sequence conflict=%v", err)
	}
	if _, err := sink.Publish(context.Background(), candidateFor(key, 1, "one")); !errors.Is(err, ErrSummaryStale) {
		t.Fatalf("stale publish=%v", err)
	}
	newer, err := sink.Publish(context.Background(), candidateFor(key, 3, "three"))
	if err != nil || newer.Checkpoint.EventSequence != 3 {
		t.Fatalf("newer publish=%#v err=%v", newer, err)
	}
}

func TestMemorySinkTreatsHashCaseAsCanonical(t *testing.T) {
	clock := &testClock{now: time.Unix(350, 0).UTC()}
	sink := NewMemorySink(clock.Now)
	key := summaryKey()
	candidate := candidateFor(key, 1, "same")
	if _, err := sink.Publish(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	candidate.ContentSHA256 = strings.ToUpper(candidate.ContentSHA256)
	result, err := sink.Publish(context.Background(), candidate)
	if err != nil || result.Applied {
		t.Fatalf("case-insensitive retry=%#v err=%v", result, err)
	}
}

type scriptedGenerator struct {
	candidate Candidate
	err       error
}

func TestProcessorRejectsTypedNilDependencies(t *testing.T) {
	var store *MemoryStore
	var sink *MemorySink
	var generator *typedNilGenerator
	for _, test := range []struct {
		name   string
		mutate func(*Processor)
	}{
		{name: "store", mutate: func(p *Processor) { p.Store = store }},
		{name: "sink", mutate: func(p *Processor) { p.Sink = sink }},
		{name: "generator", mutate: func(p *Processor) { p.Generator = generator }},
	} {
		t.Run(test.name, func(t *testing.T) {
			processor := NewProcessor(NewMemoryStore(nil), NewMemorySink(nil), scriptedGenerator{}, "worker-a", time.Second)
			test.mutate(processor)
			if _, err := processor.RunOnce(context.Background()); !errors.Is(err, ErrStoreUnavailable) {
				t.Fatalf("typed nil dependency error=%v, want ErrStoreUnavailable", err)
			}
		})
	}
}

type typedNilGenerator struct{}

func (*typedNilGenerator) Generate(context.Context, Job) (Candidate, error) {
	return Candidate{}, nil
}

func TestProcessorRejectsTypedNilTargetResolver(t *testing.T) {
	store := NewMemoryStore(nil)
	sink := NewMemorySink(nil)
	key := summaryKey()
	if _, err := store.Enqueue(context.Background(), summaryRequest(key, 0)); err != nil {
		t.Fatalf("enqueue unresolved summary: %v", err)
	}
	var resolver *typedNilTargetResolver
	processor := NewProcessor(store, sink, scriptedGenerator{}, "worker-a", time.Second)
	processor.TargetResolver = resolver
	if _, err := processor.RunOnce(context.Background()); !errors.Is(err, ErrTargetResolverUnavailable) {
		t.Fatalf("typed nil target resolver error=%v, want ErrTargetResolverUnavailable", err)
	}
}

type typedNilTargetResolver struct{}

func (*typedNilTargetResolver) ResolveTarget(context.Context, Job) (int64, error) {
	return 0, nil
}

func (g scriptedGenerator) Generate(context.Context, Job) (Candidate, error) {
	return g.candidate, g.err
}

type generatorFunc func(context.Context, Job) (Candidate, error)

func (f generatorFunc) Generate(ctx context.Context, job Job) (Candidate, error) {
	return f(ctx, job)
}

type fixedTargetResolver struct {
	sequence int64
	err      error
}

func (r fixedTargetResolver) ResolveTarget(context.Context, Job) (int64, error) {
	return r.sequence, r.err
}

// cancellationRenewStore makes Renew overlap the processor's normal
// post-generation cancellation. The in-flight call must not be reported as a
// lost lease when the generator has already returned successfully.
type cancellationRenewStore struct {
	*MemoryStore
	renewStarted chan struct{}
	renewOnce    sync.Once
}

func (s *cancellationRenewStore) Renew(ctx context.Context, _ Job, _ time.Duration) (Job, error) {
	s.renewOnce.Do(func() { close(s.renewStarted) })
	<-ctx.Done()
	return Job{}, ctx.Err()
}

func TestProcessorPublishesAndCompletes(t *testing.T) {
	clock := &testClock{now: time.Unix(400, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	sink := NewMemorySinkWithLeaseValidator(clock.Now, store.ValidateLease)
	key := summaryKey()
	if _, err := store.Enqueue(context.Background(), summaryRequest(key, 4)); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor(store, sink, scriptedGenerator{candidate: candidateFor(key, 4, "summary")}, "worker-a", time.Second)
	job, err := processor.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusCompleted || job.CompletedEventSequence != 4 {
		t.Fatalf("job=%#v", job)
	}
	checkpoint, ok, err := sink.Get(context.Background(), key)
	if err != nil || !ok || checkpoint.Content != "summary" {
		t.Fatalf("checkpoint=%#v ok=%v err=%v", checkpoint, ok, err)
	}
}

func TestProcessorCompletesNotDueJobWithoutPublishingCheckpoint(t *testing.T) {
	clock := &testClock{now: time.Unix(410, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	sink := NewMemorySinkWithLeaseValidator(clock.Now, store.ValidateLease)
	key := summaryKey()
	if _, err := store.Enqueue(context.Background(), summaryRequest(key, 4)); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor(store, sink, scriptedGenerator{err: ErrSummaryNotDue}, "worker-a", time.Second)
	job, err := processor.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusCompleted || job.CompletedEventSequence != 4 {
		t.Fatalf("job=%#v", job)
	}
	if _, ok, err := sink.Get(context.Background(), key); err != nil || ok {
		t.Fatalf("unexpected checkpoint ok=%v err=%v", ok, err)
	}
}

func TestProcessorResolvesUnresolvedTargetBeforeGeneration(t *testing.T) {
	clock := &testClock{now: time.Unix(425, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	sink := NewMemorySinkWithLeaseValidator(clock.Now, store.ValidateLease)
	key := summaryKey()
	if _, err := store.Enqueue(context.Background(), summaryRequest(key, 0)); err != nil {
		t.Fatal(err)
	}
	generator := generatorFunc(func(_ context.Context, job Job) (Candidate, error) {
		if job.TargetEventSequence != 6 || job.AgentVersionID != "version-1" {
			t.Fatalf("generator job=%#v", job)
		}
		return candidateFor(key, 6, "resolved summary"), nil
	})
	processor := NewProcessor(store, sink, generator, "worker-a", time.Second)
	processor.TargetResolver = fixedTargetResolver{sequence: 6}
	job, err := processor.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusCompleted || job.TargetEventSequence != 6 || job.CompletedEventSequence != 6 {
		t.Fatalf("completed job=%#v", job)
	}
}

func TestProcessorFailsUnresolvedTargetWithoutResolver(t *testing.T) {
	clock := &testClock{now: time.Unix(430, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	sink := NewMemorySinkWithLeaseValidator(clock.Now, store.ValidateLease)
	if _, err := store.Enqueue(context.Background(), summaryRequest(summaryKey(), 0)); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor(store, sink, scriptedGenerator{}, "worker-a", time.Second)
	processor.RetryBackoff = func(int) time.Duration { return 0 }
	job, err := processor.RunOnce(context.Background())
	if !errors.Is(err, ErrTargetResolverUnavailable) {
		t.Fatalf("error=%v", err)
	}
	if job.Status != StatusFailed {
		t.Fatalf("failed job=%#v", job)
	}
}

func TestProcessorIgnoresRenewCancellationAfterGeneration(t *testing.T) {
	clock := &testClock{now: time.Unix(450, 0).UTC()}
	base := NewMemoryStore(clock.Now)
	store := &cancellationRenewStore{
		MemoryStore:  base,
		renewStarted: make(chan struct{}),
	}
	sink := NewMemorySinkWithLeaseValidator(clock.Now, base.ValidateLease)
	key := summaryKey()
	if _, err := base.Enqueue(context.Background(), summaryRequest(key, 1)); err != nil {
		t.Fatal(err)
	}
	generator := generatorFunc(func(ctx context.Context, job Job) (Candidate, error) {
		select {
		case <-store.renewStarted:
			return candidateFor(key, 1, "summary"), nil
		case <-time.After(time.Second):
			return Candidate{}, errors.New("heartbeat did not start")
		}
	})
	processor := NewProcessor(store, sink, generator, "worker-a", 30*time.Millisecond)
	job, err := processor.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("normal cancellation was reported as heartbeat failure: %v", err)
	}
	if job.Status != StatusCompleted || job.CompletedEventSequence != 1 {
		t.Fatalf("job=%#v", job)
	}
}

func TestProcessorLeavesPendingWhenTargetAdvances(t *testing.T) {
	clock := &testClock{now: time.Unix(500, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	sink := NewMemorySinkWithLeaseValidator(clock.Now, store.ValidateLease)
	key := summaryKey()
	if _, err := store.Enqueue(context.Background(), summaryRequest(key, 5)); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor(store, sink, scriptedGenerator{candidate: candidateFor(key, 3, "partial")}, "worker-a", time.Second)
	job, err := processor.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusPending || job.CompletedEventSequence != 3 {
		t.Fatalf("job=%#v", job)
	}
}

func TestProcessorRejectsMisScopedCandidateAndRecordsFailure(t *testing.T) {
	clock := &testClock{now: time.Unix(600, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	sink := NewMemorySinkWithLeaseValidator(clock.Now, store.ValidateLease)
	key := summaryKey()
	wrong := key
	wrong.SessionID = "other"
	if _, err := store.Enqueue(context.Background(), summaryRequest(key, 1)); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor(store, sink, scriptedGenerator{candidate: candidateFor(wrong, 1, "bad")}, "worker-a", time.Second)
	processor.RetryBackoff = func(int) time.Duration { return 0 }
	job, err := processor.RunOnce(context.Background())
	if !errors.Is(err, ErrGeneratorMisScoped) {
		t.Fatalf("error=%v", err)
	}
	if job.Status != StatusFailed || job.LastError == "" {
		t.Fatalf("job=%#v", job)
	}
}

func TestProcessorPersistsFailureAfterGenerationDeadline(t *testing.T) {
	store := NewMemoryStore(nil)
	sink := NewMemorySinkWithLeaseValidator(nil, store.ValidateLease)
	key := summaryKey()
	enqueued, err := store.Enqueue(context.Background(), summaryRequest(key, 1))
	if err != nil {
		t.Fatal(err)
	}
	generator := generatorFunc(func(ctx context.Context, _ Job) (Candidate, error) {
		<-ctx.Done()
		return Candidate{}, ctx.Err()
	})
	processor := NewProcessor(store, sink, generator, "worker-deadline", time.Second)
	processor.RetryBackoff = func(int) time.Duration { return 0 }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	job, err := processor.RunOnce(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	if job.Status != StatusFailed {
		t.Fatalf("returned job status=%s", job.Status)
	}
	persisted, getErr := store.Get(context.Background(), enqueued.Job.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if persisted.Status != StatusFailed || persisted.LeaseOwner != "" || !persisted.LeaseUntil.IsZero() {
		t.Fatalf("deadline left claimed job behind: %#v", persisted)
	}
}

func TestFencedMemorySinkRejectsReplacedWorker(t *testing.T) {
	clock := &testClock{now: time.Unix(650, 0).UTC()}
	store := NewMemoryStore(clock.Now)
	sink := NewMemorySinkWithLeaseValidator(clock.Now, store.ValidateLease)
	key := summaryKey()
	if _, err := store.Enqueue(context.Background(), summaryRequest(key, 1)); err != nil {
		t.Fatal(err)
	}
	old, err := store.Claim(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if _, err := store.Claim(context.Background(), "worker-b", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.PublishFenced(context.Background(), candidateFor(key, 1, "old"), old); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("old worker publication=%v", err)
	}
}

func TestCandidateValidationRejectsTamperedHash(t *testing.T) {
	candidate := candidateFor(summaryKey(), 1, "safe")
	candidate.ContentSHA256 = fmt.Sprintf("%064x", 0)
	if err := candidate.Validate(); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("error=%v", err)
	}
}

func TestErrorTextKeepsUTF8WhenBounded(t *testing.T) {
	long := strings.Repeat("界", MaxJobErrorBytes)
	text := errorText(errors.New(long))
	if len(text) > MaxJobErrorBytes || !strings.Contains(text, "界") {
		t.Fatalf("bounded error length=%d", len(text))
	}
	if !utf8.ValidString(text) {
		t.Fatal("bounded error is invalid UTF-8")
	}
}

func TestErrorTextRedactsCredentialFields(t *testing.T) {
	text := errorText(errors.New("request failed token=secret-value api_key:other-value dsn=postgres://user:password@db.internal/agent connection postgres://user:password@db.internal/agent Authorization: Bearer bearer-value"))
	for _, secret := range []string{"secret-value", "other-value", "password@db.internal", "bearer-value"} {
		if strings.Contains(text, secret) {
			t.Fatalf("credential leaked %q in error=%q", secret, text)
		}
	}
	if !strings.Contains(text, "token=<redacted>") || !strings.Contains(text, "api_key=<redacted>") || !strings.Contains(text, "dsn=<redacted>") ||
		!strings.Contains(text, "postgres://user:<redacted>@") || !strings.Contains(text, "Authorization=<redacted>") {
		t.Fatalf("credential leaked in error=%q", text)
	}
}
