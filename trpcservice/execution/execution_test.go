package execution

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type fakeLeaseStore struct {
	mu          sync.Mutex
	events      []string
	active      map[string]storage.Lease
	nextFence   uint64
	acquireErr  error
	renewErr    error
	validateErr error
	releaseErr  error
	renewed     chan struct{}
}

func newFakeLeaseStore() *fakeLeaseStore {
	return &fakeLeaseStore{active: make(map[string]storage.Lease), nextFence: 10, renewed: make(chan struct{}, 8)}
}

func (s *fakeLeaseStore) Acquire(ctx context.Context, tc tenant.TenantContext, sessionID, ownerID string, ttl time.Duration) (storage.Lease, error) {
	if err := ctx.Err(); err != nil {
		return storage.Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "Acquire:"+sessionID+":"+ownerID)
	if s.acquireErr != nil {
		return storage.Lease{}, s.acquireErr
	}
	key := leaseKey(tc.TenantID, sessionID)
	if existing, ok := s.active[key]; ok && existing.ExpiresAt.After(time.Now()) {
		return storage.Lease{}, storage.ErrConflict
	}
	s.nextFence++
	lease := storage.Lease{
		TenantID: tc.TenantID, SessionID: sessionID, ResourceID: sessionID, OwnerID: ownerID,
		FenceToken: s.nextFence, ExpiresAt: time.Now().Add(ttl), Backend: storage.BackendPostgres, Epoch: 1,
	}
	s.active[key] = lease
	return lease, nil
}

func (s *fakeLeaseStore) Renew(ctx context.Context, tc tenant.TenantContext, lease storage.Lease, ttl time.Duration) (storage.Lease, error) {
	if err := ctx.Err(); err != nil {
		return storage.Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "Renew:"+lease.SessionID)
	if s.renewErr != nil {
		return storage.Lease{}, s.renewErr
	}
	current, ok := s.active[leaseKey(tc.TenantID, lease.SessionID)]
	if !ok || current.TenantID != tc.TenantID || current.OwnerID != lease.OwnerID || current.FenceToken != lease.FenceToken || current.Epoch != lease.Epoch {
		return storage.Lease{}, storage.ErrLeaseLost
	}
	current.ExpiresAt = time.Now().Add(ttl)
	s.active[lease.SessionID] = current
	select {
	case s.renewed <- struct{}{}:
	default:
	}
	return current, nil
}

func (s *fakeLeaseStore) Release(ctx context.Context, tc tenant.TenantContext, lease storage.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "Release:"+lease.SessionID)
	if s.releaseErr != nil {
		return s.releaseErr
	}
	current, ok := s.active[leaseKey(tc.TenantID, lease.SessionID)]
	if !ok {
		return storage.ErrLeaseLost
	}
	if current.TenantID != tc.TenantID || current.OwnerID != lease.OwnerID || current.FenceToken != lease.FenceToken || current.Epoch != lease.Epoch {
		return storage.ErrFenceRejected
	}
	delete(s.active, leaseKey(tc.TenantID, lease.SessionID))
	return nil
}

func (s *fakeLeaseStore) Validate(ctx context.Context, tc tenant.TenantContext, lease storage.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "Validate:"+lease.SessionID)
	if s.validateErr != nil {
		return s.validateErr
	}
	current, ok := s.active[leaseKey(tc.TenantID, lease.SessionID)]
	if !ok || current.TenantID != tc.TenantID || current.OwnerID != lease.OwnerID || current.FenceToken != lease.FenceToken || current.Epoch != lease.Epoch || !current.ExpiresAt.After(time.Now()) {
		return storage.ErrFenceRejected
	}
	return nil
}

func leaseKey(tenantID, sessionID string) string {
	return tenantID + "\x00" + sessionID
}

func (s *fakeLeaseStore) Takeover(tc tenant.TenantContext, ownerID string) storage.Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := leaseKey(tc.TenantID, tc.SessionID)
	current, ok := s.active[key]
	if !ok {
		panic("takeover requires an active lease")
	}
	current.OwnerID = ownerID
	current.FenceToken++
	current.Epoch++
	current.ExpiresAt = time.Now().Add(time.Minute)
	s.active[key] = current
	return current
}

func (s *fakeLeaseStore) Events() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

func (s *fakeLeaseStore) Count(prefix string) int {
	count := 0
	for _, event := range s.Events() {
		if len(event) >= len(prefix) && event[:len(prefix)] == prefix {
			count++
		}
	}
	return count
}

type fakeRuntime struct {
	mu       sync.Mutex
	entered  chan struct{}
	allow    chan struct{}
	result   agent.AgentResult
	runErr   error
	canceled bool
	input    agent.AgentInput
	ctx      context.Context
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{entered: make(chan struct{}, 1), allow: make(chan struct{})}
}

func (r *fakeRuntime) Run(ctx context.Context, input agent.AgentInput) (agent.AgentResult, error) {
	r.mu.Lock()
	r.input = input
	r.ctx = ctx
	r.mu.Unlock()
	select {
	case r.entered <- struct{}{}:
	default:
	}
	if r.runErr != nil {
		return r.result, r.runErr
	}
	select {
	case <-r.allow:
		return r.result, nil
	case <-ctx.Done():
		r.mu.Lock()
		r.canceled = true
		r.mu.Unlock()
		return r.result, ctx.Err()
	}
}

func (r *fakeRuntime) Cancelled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.canceled
}

func (r *fakeRuntime) Input() agent.AgentInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.input
}

type fakeAgentFactory struct {
	mu       sync.Mutex
	builds   []agent.AgentSpec
	contexts []context.Context
	runtimes []*fakeRuntime
	buildErr error
	runErr   error
}

func (f *fakeAgentFactory) Build(ctx context.Context, tc tenant.TenantContext, spec agent.AgentSpec) (agent.AgentRuntime, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.builds = append(f.builds, spec)
	f.contexts = append(f.contexts, ctx)
	if f.buildErr != nil {
		return nil, f.buildErr
	}
	runtime := newFakeRuntime()
	runtime.runErr = f.runErr
	f.runtimes = append(f.runtimes, runtime)
	return runtime, nil
}

func (f *fakeAgentFactory) Runtime(index int) *fakeRuntime {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index >= len(f.runtimes) {
		return nil
	}
	return f.runtimes[index]
}

func (f *fakeAgentFactory) BuildCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.builds)
}

type fakeExecutionSink struct {
	mu        sync.Mutex
	leases    *fakeLeaseStore
	commits   []ExecutionCommit
	commitErr error
}

// Commit models the durable commit guard: the lease store lock covers the
// current-token check and the fake fact append as one test-only operation.
func (s *fakeExecutionSink) Commit(ctx context.Context, commit ExecutionCommit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.leases == nil {
		return storage.ErrInvalidArgument
	}
	s.leases.mu.Lock()
	defer s.leases.mu.Unlock()
	if err := validateFencedCommit(s.leases, commit); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits = append(s.commits, commit)
	return s.commitErr
}

func validateFencedCommit(leases *fakeLeaseStore, commit ExecutionCommit) error {
	lease := commit.Lease
	if commit.JobID != commit.Job.JobID || commit.ExecutionID != commit.Job.ExecutionID ||
		commit.TenantID != commit.TenantContext.TenantID || commit.SessionID != commit.TenantContext.SessionID ||
		commit.OwnerID != lease.OwnerID || commit.Epoch != lease.Epoch || commit.FenceToken != lease.FenceToken ||
		lease.TenantID != commit.TenantID || lease.SessionID != commit.SessionID ||
		lease.ResourceID != commit.SessionID || lease.OwnerID != commit.OwnerID ||
		lease.Epoch != commit.Epoch || lease.FenceToken != commit.FenceToken ||
		commit.Input.TenantContext.TenantID != commit.TenantID || commit.Input.TenantContext.SessionID != commit.SessionID {
		return storage.ErrFenceRejected
	}
	current, ok := leases.active[leaseKey(commit.TenantID, commit.SessionID)]
	if !ok || !current.ExpiresAt.After(time.Now()) || current.OwnerID != commit.OwnerID || current.Epoch != commit.Epoch || current.FenceToken != commit.FenceToken {
		return storage.ErrFenceRejected
	}
	return nil
}

func newTestExecutor() (*Executor, *fakeLeaseStore, *fakeAgentFactory, *fakeExecutionSink) {
	leases := newFakeLeaseStore()
	factory := &fakeAgentFactory{}
	sink := &fakeExecutionSink{leases: leases}
	executor, err := NewExecutor(leases, factory, sink)
	if err != nil {
		panic(err)
	}
	return executor, leases, factory, sink
}

func (s *fakeExecutionSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commits)
}

func (s *fakeExecutionSink) Last() ExecutionCommit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits[len(s.commits)-1]
}

func validExecutionRequest(sessionID, jobID, ownerID string) ExecutionRequest {
	now := time.Now().UTC()
	tc := tenant.TenantContext{
		TenantID: "tenant-execution", AgentAppID: "agent-execution", BindingID: "binding-execution", Channel: "web",
		ExternalUser: "external-user", ExternalChat: "external-chat", InternalUser: "internal-user", SessionID: sessionID,
		RequestID: "request-" + jobID, MessageID: "message-" + jobID, TraceID: "trace-" + jobID, ConfigVersion: 3,
		Permissions: []string{"agent.run"}, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"},
	}
	job := queue.AgentJob{
		SchemaVersion: queue.SchemaVersion, JobID: jobID, ExecutionID: "execution-" + jobID,
		Tenant:    queue.TenantContextDTOFromContext(tc),
		Agent:     queue.AgentRefDTO{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion},
		Message:   queue.MessageDTO{ID: tc.MessageID, Role: "user", Content: "hello", CreatedAt: now},
		Trace:     queue.TraceContextDTO{TraceID: tc.TraceID, RequestID: tc.RequestID, MessageID: tc.MessageID, ExecutionID: "execution-" + jobID},
		CreatedAt: now, Deadline: now.Add(2 * time.Second), Attempt: 1,
	}
	return ExecutionRequest{
		Job: job, Agent: agent.AgentSpec{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion, Name: "execution-agent", ModelProvider: "fake"},
		History: []agent.Message{{ID: "history-" + jobID, Role: "user", Content: "previous", CreatedAt: now.Add(-time.Minute)}},
		OwnerID: ownerID, LeaseTTL: 100 * time.Millisecond, RenewInterval: 5 * time.Millisecond, ReleaseTimeout: time.Second,
	}
}

func waitFor(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertEventOrder(t *testing.T, events []string, names ...string) {
	t.Helper()
	positions := make(map[string]int)
	for i, event := range events {
		for _, name := range names {
			if len(event) >= len(name) && event[:len(name)] == name {
				positions[name] = i
			}
		}
	}
	last := -1
	for _, name := range names {
		position, ok := positions[name]
		if !ok || position <= last {
			t.Fatalf("events %v do not contain ordered %s after %d", events, name, last)
		}
		last = position
	}
}

func TestExecuteMinimalSingleCallRestoresContextAndCommits(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	request := validExecutionRequest("session-minimal", "job-minimal", "owner-minimal")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan struct {
		result ExecutionResult
		err    error
	}, 1)
	go func() {
		result, err := executor.Execute(ctx, request)
		resultCh <- struct {
			result ExecutionResult
			err    error
		}{result, err}
	}()
	waitFor(t, factory.runtimesReady(), "runtime entry")
	runtime := factory.Runtime(0)
	close(runtime.allow)
	out := <-resultCh
	if out.err != nil || out.result.State != StateSucceeded {
		t.Fatalf("Execute result = %#v, err = %v", out.result, out.err)
	}
	if out.result.Lease.FenceToken == 0 || out.result.Lease.Epoch == 0 || out.result.Lease.OwnerID != request.OwnerID {
		t.Fatalf("invalid acquired lease in result: %#v", out.result.Lease)
	}
	if sink.Count() != 1 || out.result.CommitCount != 1 || leases.Count("Release:") != 1 {
		t.Fatalf("commit/release counts: commit=%d result=%d release=%d", sink.Count(), out.result.CommitCount, leases.Count("Release:"))
	}
	committed := sink.Last()
	if committed.JobID != request.Job.JobID || committed.ExecutionID != request.Job.ExecutionID ||
		committed.TenantID != request.Job.Tenant.TenantID || committed.SessionID != request.Job.Tenant.SessionID ||
		committed.OwnerID != out.result.Lease.OwnerID || committed.Epoch != out.result.Lease.Epoch || committed.FenceToken != out.result.Lease.FenceToken {
		t.Fatalf("commit did not carry the acquired fencing identity: %#v", committed)
	}
	input := runtime.Input()
	if input.TenantContext.TenantID != request.Job.Tenant.TenantID || input.TenantContext.SessionID != request.Job.Tenant.SessionID || input.Input.ID != request.Job.Message.ID || len(input.History) != 1 {
		t.Fatalf("runtime input was not restored correctly: %#v", input)
	}
	info, ok := ContextInfoFrom(runtime.ctx)
	if !ok || info.ExecutionID != request.Job.ExecutionID || info.TraceID != request.Job.Trace.TraceID {
		t.Fatalf("execution context metadata = %#v, ok=%v", info, ok)
	}
	if restored, ok := TenantContextFrom(runtime.ctx); !ok || restored.TenantID != request.Job.Tenant.TenantID {
		t.Fatalf("tenant context missing from runtime context: %#v, ok=%v", restored, ok)
	}
}

func TestExecuteTakeoverBetweenValidateAndCommitIsRejected(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	request := validExecutionRequest("session-takeover", "job-takeover", "owner-old")
	tc, err := request.Job.Tenant.Restore()
	if err != nil {
		t.Fatal(err)
	}
	validated := make(chan struct{})
	continueCommit := make(chan struct{})
	executor.beforeCommit = func() {
		close(validated)
		<-continueCommit
	}
	resultCh := make(chan struct {
		result ExecutionResult
		err    error
	}, 1)
	go func() {
		result, executeErr := executor.Execute(context.Background(), request)
		resultCh <- struct {
			result ExecutionResult
			err    error
		}{result, executeErr}
	}()
	waitFor(t, factory.runtimesReady(), "runtime entry")
	close(factory.Runtime(0).allow)
	waitFor(t, validated, "validate hook")
	newLease := leases.Takeover(tc, "owner-new")
	if newLease.OwnerID == request.OwnerID || newLease.FenceToken == 0 || newLease.Epoch == 0 {
		t.Fatalf("takeover did not advance fencing identity: %#v", newLease)
	}
	close(continueCommit)
	out := <-resultCh
	if !errors.Is(out.err, storage.ErrFenceRejected) || out.result.State != StateLeaseLost {
		t.Fatalf("takeover result=%#v err=%v", out.result, out.err)
	}
	if sink.Count() != 0 || out.result.CommitCount != 0 || leases.Count("Release:") != 1 {
		t.Fatalf("stale execution effects: commits=%d result=%d releases=%d", sink.Count(), out.result.CommitCount, leases.Count("Release:"))
	}
}

func TestFencedCommitRejectsInconsistentIdentity(t *testing.T) {
	_, leases, _, sink := newTestExecutor()
	request := validExecutionRequest("session-identity", "job-identity", "owner-identity")
	tc, err := request.Job.Tenant.Restore()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := leases.Acquire(context.Background(), tc, tc.SessionID, request.OwnerID, request.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	base := ExecutionCommit{JobID: request.Job.JobID, ExecutionID: request.Job.ExecutionID, TenantID: tc.TenantID, SessionID: tc.SessionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch, FenceToken: lease.FenceToken, Job: request.Job, TenantContext: tc, Input: agent.AgentInput{TenantContext: tc}, Lease: lease}
	tests := []struct {
		name   string
		mutate func(*ExecutionCommit)
	}{
		{name: "tenant", mutate: func(commit *ExecutionCommit) { commit.TenantID = "tenant-other" }},
		{name: "session", mutate: func(commit *ExecutionCommit) { commit.SessionID = "session-other" }},
		{name: "owner", mutate: func(commit *ExecutionCommit) { commit.OwnerID = "owner-other" }},
		{name: "epoch", mutate: func(commit *ExecutionCommit) { commit.Epoch++ }},
		{name: "fence", mutate: func(commit *ExecutionCommit) { commit.FenceToken++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commit := base
			test.mutate(&commit)
			if err := sink.Commit(context.Background(), commit); !errors.Is(err, storage.ErrFenceRejected) {
				t.Fatalf("error=%v, want ErrFenceRejected", err)
			}
		})
	}
	if sink.Count() != 0 {
		t.Fatalf("inconsistent commits were recorded: %d", sink.Count())
	}
}

func TestFencedCommitDoesNotReuseSessionLeaseAcrossTenants(t *testing.T) {
	leases := newFakeLeaseStore()
	tcOne := tenant.TenantContext{TenantID: "tenant-one", SessionID: "shared-session"}
	tcTwo := tenant.TenantContext{TenantID: "tenant-two", SessionID: "shared-session"}
	one, err := leases.Acquire(context.Background(), tcOne, tcOne.SessionID, "owner-one", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	two, err := leases.Acquire(context.Background(), tcTwo, tcTwo.SessionID, "owner-two", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if one.FenceToken == two.FenceToken || one.TenantID == two.TenantID {
		t.Fatalf("tenant leases were not independent: one=%#v two=%#v", one, two)
	}
}

func (f *fakeAgentFactory) runtimesReady() <-chan struct{} {
	ready := make(chan struct{}, 1)
	go func() {
		deadline := time.After(time.Second)
		for {
			f.mu.Lock()
			count := len(f.runtimes)
			f.mu.Unlock()
			if count > 0 {
				ready <- struct{}{}
				return
			}
			select {
			case <-deadline:
				return
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()
	return ready
}

func TestExecuteSuccessOrderIncludesRenewValidateCommitRelease(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	request := validExecutionRequest("session-order", "job-order", "owner-order")
	resultCh := make(chan error, 1)
	go func() {
		_, err := executor.Execute(context.Background(), request)
		resultCh <- err
	}()
	waitFor(t, factory.runtimesReady(), "runtime entry")
	waitFor(t, leases.renewed, "lease renewal")
	close(factory.Runtime(0).allow)
	if err := <-resultCh; err != nil {
		t.Fatal(err)
	}
	assertEventOrder(t, leases.Events(), "Acquire:", "Renew:", "Validate:", "Release:")
	if sink.Count() != 1 {
		t.Fatalf("commit count = %d", sink.Count())
	}
	if got := sink.Last().Lease.FenceToken; got == 0 {
		t.Fatalf("commit used empty fence token: %#v", sink.Last().Lease)
	}
}

func TestExecuteRejectsAgentVersionMismatchBeforeAcquire(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	request := validExecutionRequest("session-version", "job-version", "owner-version")
	request.Agent.Version++
	result, err := executor.Execute(context.Background(), request)
	if !errors.Is(err, ErrInvalidRequest) || result.State != StatePermanentFailure {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if leases.Count("Acquire:") != 0 || factory.BuildCount() != 0 || sink.Count() != 0 {
		t.Fatalf("invalid request crossed boundary: events=%v builds=%d commits=%d", leases.Events(), factory.BuildCount(), sink.Count())
	}
}

func TestExecuteAcquireFailure(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	leases.acquireErr = storage.ErrBackendUnavailable
	result, err := executor.Execute(context.Background(), validExecutionRequest("session-acquire-fail", "job-acquire-fail", "owner-acquire-fail"))
	if !errors.Is(err, storage.ErrBackendUnavailable) || result.State != StateRetryableFailure {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if factory.BuildCount() != 0 || sink.Count() != 0 || leases.Count("Release:") != 0 {
		t.Fatalf("acquire failure crossed boundary: builds=%d commits=%d releases=%d", factory.BuildCount(), sink.Count(), leases.Count("Release:"))
	}
}

func TestExecuteRenewFailureCancelsRuntimeAndBlocksCommit(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	leases.renewErr = storage.ErrLeaseLost
	resultCh := make(chan struct {
		result ExecutionResult
		err    error
	}, 1)
	go func() {
		result, err := executor.Execute(context.Background(), validExecutionRequest("session-renew-fail", "job-renew-fail", "owner-renew-fail"))
		resultCh <- struct {
			result ExecutionResult
			err    error
		}{result, err}
	}()
	waitFor(t, factory.runtimesReady(), "runtime entry")
	out := <-resultCh
	if !errors.Is(out.err, storage.ErrLeaseLost) || out.result.State != StateLeaseLost {
		t.Fatalf("result=%#v err=%v", out.result, out.err)
	}
	if !factory.Runtime(0).Cancelled() || sink.Count() != 0 || leases.Count("Release:") != 1 {
		t.Fatalf("renew failure cleanup: canceled=%v commits=%d releases=%d", factory.Runtime(0).Cancelled(), sink.Count(), leases.Count("Release:"))
	}
}

func TestExecuteValidateFenceRejectBlocksCommit(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	leases.validateErr = storage.ErrFenceRejected
	resultCh := make(chan error, 1)
	go func() {
		_, err := executor.Execute(context.Background(), validExecutionRequest("session-fence", "job-fence", "owner-fence"))
		resultCh <- err
	}()
	waitFor(t, factory.runtimesReady(), "runtime entry")
	close(factory.Runtime(0).allow)
	err := <-resultCh
	if !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("error = %v", err)
	}
	if sink.Count() != 0 || leases.Count("Validate:") != 1 || leases.Count("Release:") != 1 {
		t.Fatalf("fence rejection effects: commits=%d validates=%d releases=%d", sink.Count(), leases.Count("Validate:"), leases.Count("Release:"))
	}
}

func TestExecuteRuntimeErrorsNeverCommit(t *testing.T) {
	tests := []struct {
		name   string
		runErr error
		state  ExecutionState
	}{
		{name: "producer incomplete", runErr: agent.ErrProducerIncomplete, state: StateRetryableFailure},
		{name: "drain timeout", runErr: agent.ErrDrainTimeout, state: StateRetryableFailure},
		{name: "framework failure", runErr: agent.ErrFrameworkFailure, state: StateRetryableFailure},
		{name: "canceled", runErr: context.Canceled, state: StateCanceled},
		{name: "deadline", runErr: context.DeadlineExceeded, state: StateRetryableFailure},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor, leases, factory, sink := newTestExecutor()
			factory.runErr = test.runErr
			resultCh := make(chan struct {
				result ExecutionResult
				err    error
			}, 1)
			go func() {
				executorResult, err := executor.Execute(context.Background(), validExecutionRequest(fmt.Sprintf("session-runtime-%d", index), fmt.Sprintf("job-runtime-%d", index), fmt.Sprintf("owner-runtime-%d", index)))
				resultCh <- struct {
					result ExecutionResult
					err    error
				}{executorResult, err}
			}()
			out := <-resultCh
			if !errors.Is(out.err, test.runErr) || out.result.State != test.state {
				t.Fatalf("result=%#v err=%v, want state=%s and error %v", out.result, out.err, test.state, test.runErr)
			}
			if sink.Count() != 0 || leases.Count("Release:") != 1 {
				t.Fatalf("runtime failure effects: commits=%d releases=%d", sink.Count(), leases.Count("Release:"))
			}
		})
	}
}

func TestExecuteFencedCommitErrorsRemainFailures(t *testing.T) {
	tests := []struct {
		name  string
		id    string
		err   error
		state ExecutionState
	}{
		{name: "lease lost", id: "lease-lost", err: storage.ErrLeaseLost, state: StateLeaseLost},
		{name: "fence rejected", id: "fence-rejected", err: storage.ErrFenceRejected, state: StateLeaseLost},
		{name: "ordinary commit", id: "ordinary-commit", err: storage.ErrConflict, state: StateRetryableFailure},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor, leases, factory, sink := newTestExecutor()
			sink.commitErr = test.err
			resultCh := make(chan struct {
				result ExecutionResult
				err    error
			}, 1)
			go func() {
				result, err := executor.Execute(context.Background(), validExecutionRequest("session-fenced-error-"+test.id, "job-fenced-error-"+test.id, "owner-fenced-error-"+test.id))
				resultCh <- struct {
					result ExecutionResult
					err    error
				}{result, err}
			}()
			waitFor(t, factory.runtimesReady(), "runtime entry")
			close(factory.Runtime(0).allow)
			out := <-resultCh
			if !errors.Is(out.err, test.err) || out.result.State != test.state || out.result.CommitCount != 0 {
				t.Fatalf("result=%#v err=%v", out.result, out.err)
			}
			if sink.Count() != 1 || leases.Count("Release:") != 1 {
				t.Fatalf("failed commit cleanup: commits=%d releases=%d", sink.Count(), leases.Count("Release:"))
			}
		})
	}
}

func TestExecuteRootCancellationCancelsRuntimeAndDoesNotCommit(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := executor.Execute(ctx, validExecutionRequest("session-cancel", "job-cancel", "owner-cancel"))
		resultCh <- err
	}()
	waitFor(t, factory.runtimesReady(), "runtime entry")
	cancel()
	err := <-resultCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if !factory.Runtime(0).Cancelled() || sink.Count() != 0 || leases.Count("Release:") != 1 {
		t.Fatalf("cancellation effects: canceled=%v commits=%d releases=%d", factory.Runtime(0).Cancelled(), sink.Count(), leases.Count("Release:"))
	}
}

func TestExecuteCommitAndReleaseFailuresRemainFailures(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		executor, leases, factory, sink := newTestExecutor()
		sink.commitErr = storage.ErrConflict
		resultCh := make(chan struct {
			result ExecutionResult
			err    error
		}, 1)
		go func() {
			result, err := executor.Execute(context.Background(), validExecutionRequest("session-commit-fail", "job-commit-fail", "owner-commit-fail"))
			resultCh <- struct {
				result ExecutionResult
				err    error
			}{result, err}
		}()
		waitFor(t, factory.runtimesReady(), "runtime entry")
		close(factory.Runtime(0).allow)
		out := <-resultCh
		if !errors.Is(out.err, storage.ErrConflict) || out.result.State != StateRetryableFailure || sink.Count() != 1 || leases.Count("Release:") != 1 {
			t.Fatalf("result=%#v err=%v commits=%d releases=%d", out.result, out.err, sink.Count(), leases.Count("Release:"))
		}
	})

	t.Run("release", func(t *testing.T) {
		executor, leases, factory, sink := newTestExecutor()
		leases.releaseErr = storage.ErrBackendUnavailable
		resultCh := make(chan struct {
			result ExecutionResult
			err    error
		}, 1)
		go func() {
			result, err := executor.Execute(context.Background(), validExecutionRequest("session-release-fail", "job-release-fail", "owner-release-fail"))
			resultCh <- struct {
				result ExecutionResult
				err    error
			}{result, err}
		}()
		waitFor(t, factory.runtimesReady(), "runtime entry")
		close(factory.Runtime(0).allow)
		out := <-resultCh
		if !errors.Is(out.err, storage.ErrBackendUnavailable) || out.result.State != StateRetryableFailure || sink.Count() != 1 || leases.Count("Release:") != 1 {
			t.Fatalf("result=%#v err=%v commits=%d releases=%d", out.result, out.err, sink.Count(), leases.Count("Release:"))
		}
	})
}

func TestExecuteDifferentSessionsRunInParallel(t *testing.T) {
	leases := newFakeLeaseStore()
	factory := &fakeAgentFactory{}
	sink := &fakeExecutionSink{leases: leases}
	executor, err := NewExecutor(leases, factory, sink)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for i, session := range []string{"session-parallel-a", "session-parallel-b"} {
		request := validExecutionRequest(session, fmt.Sprintf("job-parallel-%d", i), fmt.Sprintf("owner-parallel-%d", i))
		go func() {
			_, executeErr := executor.Execute(context.Background(), request)
			results <- executeErr
		}()
	}
	deadline := time.After(time.Second)
	for {
		factory.mu.Lock()
		count := len(factory.runtimes)
		factory.mu.Unlock()
		if count == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("both sessions did not enter Runtime")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	factory.mu.Lock()
	runtimes := append([]*fakeRuntime(nil), factory.runtimes...)
	factory.mu.Unlock()
	close(runtimes[0].allow)
	close(runtimes[1].allow)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if sink.Count() != 2 {
		t.Fatalf("parallel commit count = %d", sink.Count())
	}
}

func TestExecuteSameSessionUsesLeaseConflictWithoutRuntime(t *testing.T) {
	executor, leases, factory, sink := newTestExecutor()
	firstRequest := validExecutionRequest("session-same", "job-same-first", "owner-first")
	secondRequest := validExecutionRequest("session-same", "job-same-second", "owner-second")
	firstResult := make(chan error, 1)
	go func() {
		_, err := executor.Execute(context.Background(), firstRequest)
		firstResult <- err
	}()
	waitFor(t, factory.runtimesReady(), "first runtime entry")
	second, secondErr := executor.Execute(context.Background(), secondRequest)
	if !errors.Is(secondErr, storage.ErrConflict) || second.State != StateRetryableFailure {
		t.Fatalf("second result=%#v err=%v", second, secondErr)
	}
	if factory.BuildCount() != 1 {
		t.Fatalf("second execution entered Runtime, build count=%d", factory.BuildCount())
	}
	close(factory.Runtime(0).allow)
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	if sink.Count() != 1 || leases.Count("Release:") != 1 {
		t.Fatalf("same-session effects: commits=%d releases=%d", sink.Count(), leases.Count("Release:"))
	}
}

func TestExecutionContextDeadlineIsBoundByParent(t *testing.T) {
	executor, _, factory, _ := newTestExecutor()
	request := validExecutionRequest("session-deadline", "job-deadline", "owner-deadline")
	parent, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, err := executor.Execute(parent, request)
		resultCh <- err
	}()
	waitFor(t, factory.runtimesReady(), "runtime entry")
	if deadline, ok := factory.contexts[0].Deadline(); !ok || time.Until(deadline) > 50*time.Millisecond {
		t.Fatalf("runtime context deadline was not bounded by parent: %v, ok=%v", deadline, ok)
	}
	if err := <-resultCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
}

func TestExecutionErrorSupportsErrorsIsAndAs(t *testing.T) {
	executor, leases, _, _ := newTestExecutor()
	leases.acquireErr = storage.ErrFenceRejected
	_, err := executor.Execute(context.Background(), validExecutionRequest("session-errors", "job-errors", "owner-errors"))
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Class != FailureLeaseLost || !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("error chain = %v, execution error = %#v", err, executionErr)
	}
}

func TestExecutionHelpersDoNotAliasMutableInputs(t *testing.T) {
	executor, _, factory, _ := newTestExecutor()
	request := validExecutionRequest("session-copy", "job-copy", "owner-copy")
	request.Agent.Tools = []agent.ToolSpec{{Name: "tool", InputSchema: map[string]any{"key": "value"}}}
	request.History[0].ToolCalls = []agent.ToolCall{{ID: "call", Name: "tool", Arguments: "{}"}}
	resultCh := make(chan error, 1)
	go func() {
		_, err := executor.Execute(context.Background(), request)
		resultCh <- err
	}()
	waitFor(t, factory.runtimesReady(), "runtime entry")
	close(factory.Runtime(0).allow)
	if err := <-resultCh; err != nil {
		t.Fatal(err)
	}
	input := factory.Runtime(0).Input()
	if !reflect.DeepEqual(input.History[0].ToolCalls, request.History[0].ToolCalls) {
		t.Fatalf("history was changed during conversion: %#v", input.History)
	}
}

func TestFakeLeaseDiagnosticsAreStable(t *testing.T) {
	leases := newFakeLeaseStore()
	request := validExecutionRequest("session-diagnostics", "job-diagnostics", "owner-diagnostics")
	tc, err := request.Job.Tenant.Restore()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := leases.Acquire(context.Background(), tc, tc.SessionID, request.OwnerID, request.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if err := leases.Validate(context.Background(), tc, lease); err != nil {
		t.Fatal(err)
	}
	if err := leases.Release(context.Background(), tc, lease); err != nil {
		t.Fatal(err)
	}
	events := leases.Events()
	sorted := append([]string(nil), events...)
	sort.Strings(sorted)
	if len(events) != 3 || events[0][:7] != "Acquire" || events[1][:8] != "Validate" || events[2][:7] != "Release" {
		t.Fatalf("unexpected lease event sequence: %v (sorted=%v)", events, sorted)
	}
}
