package audit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func testEvent(tenantID, eventID string) Event {
	input, output := int64(4), int64(6)
	return Event{SchemaVersion: SchemaVersion, EventID: eventID, EventType: EventExecutionCompleted, TenantID: tenantID, AgentAppID: "app-1", Channel: "api", Cost: &Usage{InputTokens: &input, OutputTokens: &output, Currency: "USD", Provider: "provider", Model: "model", ExecutionResult: ResultSuccess}, OccurredAt: time.Now().UTC()}
}

func TestInMemoryTenantScopeAndDuplicateDigest(t *testing.T) {
	backend := &Backend{}
	one, err := NewInMemoryWithBackend("tenant-a", backend)
	if err != nil {
		t.Fatal(err)
	}
	two, err := NewInMemoryWithBackend("tenant-b", backend)
	if err != nil {
		t.Fatal(err)
	}
	event := testEvent("tenant-a", "event-1")
	first, err := one.Append(context.Background(), event)
	if err != nil || first.Duplicate {
		t.Fatalf("first append = %#v, %v", first, err)
	}
	duplicate, err := one.Append(context.Background(), event)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate append = %#v, %v", duplicate, err)
	}
	event.Cost.InputTokens = ptr(99)
	got, err := one.Get(context.Background(), "event-1")
	if err != nil {
		t.Fatal(err)
	}
	if *got.Cost.InputTokens != 4 {
		t.Fatalf("stored event was aliased: %d", *got.Cost.InputTokens)
	}
	if _, err := two.Append(context.Background(), event); !errors.Is(err, ErrTenantScope) {
		t.Fatalf("cross-tenant append error = %v", err)
	}
	if _, err := two.Get(context.Background(), "event-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get error = %v", err)
	}
	event = testEvent("tenant-a", "event-1")
	event.Reason = "changed"
	if _, err := one.Append(context.Background(), event); !errors.Is(err, ErrConflict) {
		t.Fatalf("digest conflict = %v", err)
	}
}

func TestInMemoryConcurrentAppendAndAggregation(t *testing.T) {
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	errorsOut := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			event := testEvent("tenant-a", fmt.Sprintf("event-%02d", i))
			_, err := store.Append(context.Background(), event)
			if err != nil {
				errorsOut <- err
			}
		}(i)
	}
	wg.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
	events, err := store.List(context.Background(), Query{})
	if err != nil || len(events) != workers {
		t.Fatalf("list = %d, %v", len(events), err)
	}
	totals, err := store.AggregateUsage(context.Background(), UsageQuery{GroupBy: []GroupBy{GroupApp, GroupChannel, GroupProvider, GroupModel}})
	if err != nil {
		t.Fatal(err)
	}
	if len(totals) != 1 || totals[0].InputTokens != workers*4 || totals[0].OutputTokens != workers*6 {
		t.Fatalf("totals = %#v", totals)
	}
}

func TestEventValidationAndCancellation(t *testing.T) {
	event := testEvent("tenant-a", "event-1")
	event.SchemaVersion = 2
	if !errors.Is(event.Validate(), ErrInvalid) {
		t.Fatal("unknown schema version accepted")
	}
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Append(ctx, testEvent("tenant-a", "event-1")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled append = %v", err)
	}
}

func TestEventRejectsSensitiveValuesAndUnknownErrorTypes(t *testing.T) {
	for _, value := range []string{"Authorization: Bearer secret", "API key abc", "token abc", "dsn=postgres://user:pass@db", "provider error: secret"} {
		event := testEvent("tenant-a", "event-1")
		event.ErrorType = value
		if !errors.Is(event.Validate(), ErrInvalid) {
			t.Fatalf("sensitive error type accepted: %q", value)
		}
	}
	event := testEvent("tenant-a", "event-1")
	event.Reason = "password=secret"
	if !errors.Is(event.Validate(), ErrInvalid) {
		t.Fatal("sensitive reason accepted")
	}
	event = testEvent("tenant-a", "event-1")
	event.ErrorType = "made_up"
	if !errors.Is(event.Validate(), ErrInvalid) {
		t.Fatal("unknown error type accepted")
	}
	usage := Usage{BudgetUsedMinor: ptr(1), Currency: "ZZZ"}
	if !errors.Is(usage.Validate(), ErrInvalid) {
		t.Fatal("unknown currency accepted")
	}
	for _, currency := range []string{"PLN", "BRL", "RUB", "TRY", "THB", "TWD"} {
		if err := (Usage{ModelCostMinor: ptr(1), Currency: currency}).Validate(); err != nil {
			t.Fatalf("valid ISO currency %s rejected: %v", currency, err)
		}
	}
}

func TestEventAllowsSensitiveLookingOpaqueIdentifierFragments(t *testing.T) {
	event := testEvent("tenant-a", "event-opaque-id")
	event.SessionID = "28:t_01M1QXA4YFVGZK0D0CTJFW4DSN:54:3:api30:app_01M1QXA4YF9J49ARY63APBJ2736:direct4:peer0:"
	if err := event.Validate(); err != nil {
		t.Fatalf("opaque identifier was rejected: %v", err)
	}
}

func TestContainsSensitiveWordsHonorsWordBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "api key phrase", value: "API key", want: true},
		{name: "punctuated api key phrase", value: "(API, key.)", want: true},
		{name: "sensitive word at end", value: "metadata dsn", want: true},
		{name: "api without key", value: "api", want: false},
		{name: "opaque identifier fragment", value: "opaque:dsn:fragment", want: false},
		{name: "empty value", value: "   ", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := containsSensitiveWords(tt.value); got != tt.want {
				t.Fatalf("containsSensitiveWords(%q) = %t, want %t", tt.value, got, tt.want)
			}
		})
	}
}

func TestAggregationSeparatesCurrenciesAndIDsCannotCollide(t *testing.T) {
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	usd := testEvent("tenant-a", "event-a")
	eur := testEvent("tenant-a", "event-b")
	eur.Cost.Currency = "EUR"
	if _, err := store.Append(context.Background(), usd); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), eur); err != nil {
		t.Fatal(err)
	}
	totals, err := store.AggregateUsage(context.Background(), UsageQuery{GroupBy: []GroupBy{GroupApp}})
	if err != nil || len(totals) != 2 {
		t.Fatalf("currency totals = %#v, %v", totals, err)
	}
	backend := &Backend{}
	one, err := NewInMemoryWithBackend("tenant", backend)
	if err != nil {
		t.Fatal(err)
	}
	two, err := NewInMemoryWithBackend("tenant\\x00event", backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := one.Append(context.Background(), testEvent("tenant", "event\\x00b")); err != nil {
		t.Fatal(err)
	}
	if _, err := two.Append(context.Background(), testEvent("tenant\\x00event", "b")); err != nil {
		t.Fatal(err)
	}
}

func TestAuditValidationAndQueryBoundaries(t *testing.T) {
	base := testEvent("tenant-a", "event-1")
	for _, mutate := range []func(*Event){
		func(e *Event) { e.Decision = DecisionAllow },
		func(e *Event) { e.Decision = Decision("unknown") },
		func(e *Event) { e.EventType = EventType("unknown") },
		func(e *Event) { e.EventID = " event-1" },
		func(e *Event) { e.OccurredAt = time.Time{} },
		func(e *Event) { e.OccurredAt = time.Now() },
		func(e *Event) { e.PreviousVersion = ptr(2) },
		func(e *Event) { e.PreviousVersion, e.NextVersion = ptr(2), ptr(2) },
		func(e *Event) { e.PreviousVersion, e.NextVersion = ptr(math.MaxInt64), ptr(math.MinInt64) },
		func(e *Event) { e.LatencyMS = ptr(-1) },
		func(e *Event) { e.Revision = ptr(-1) },
	} {
		event := base
		mutate(&event)
		if event.Decision == DecisionAllow {
			if err := event.Validate(); err != nil {
				t.Fatalf("valid decision rejected: %v", err)
			}
		} else if !errors.Is(event.Validate(), ErrInvalid) {
			t.Fatalf("invalid event accepted: %#v", event)
		}
	}
	for _, usage := range []Usage{
		{InputTokens: ptr(-1)}, {ModelCostMinor: ptr(1)}, {ToolCostMinor: ptr(1), Currency: "usd"},
		{ExecutionResult: ExecutionResult("unknown")}, {Provider: "https://provider"}, {BudgetUsedMinor: ptr(1)},
	} {
		if usage.Validate() == nil {
			t.Fatalf("invalid usage accepted: %#v", usage)
		}
	}
	if _, err := NewInMemory(""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty tenant error = %v", err)
	}
	if _, err := NewInMemoryWithBackend("tenant", nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil backend error = %v", err)
	}
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	if _, err := store.Append(nilContext, base); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context append = %v", err)
	}
	if _, err := store.Get(context.Background(), " "); !errors.Is(err, ErrTenantScope) {
		t.Fatalf("empty get = %v", err)
	}
	if _, err := store.List(context.Background(), Query{EventTypes: []EventType{"unknown"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid query type = %v", err)
	}
	if _, err := store.AggregateUsage(context.Background(), UsageQuery{GroupBy: []GroupBy{"unknown"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid group = %v", err)
	}
	if _, err := store.AggregateUsage(context.Background(), UsageQuery{GroupBy: []GroupBy{GroupTenant}}); err != nil {
		t.Fatal(err)
	}
}

func TestAuditValidationBranches(t *testing.T) {
	base := testEvent("tenant-a", "event-1")
	base.Cost = nil
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := base.Digest(); err != nil {
		t.Fatal(err)
	}

	invalidEvents := []func(*Event){
		func(e *Event) { e.EventID = "event-1\x00bad" },
		func(e *Event) { e.TenantID = "tenant-a\x00bad" },
		func(e *Event) { e.Channel = "bad\x00channel" },
		func(e *Event) { e.Reason = string(make([]rune, 1001)) },
		func(e *Event) { e.EventType = EventControlPlaneChanged },
	}
	for i, mutate := range invalidEvents {
		event := base
		mutate(&event)
		if !errors.Is(event.Validate(), ErrInvalid) {
			t.Fatalf("invalid event accepted at %d", i)
		}
	}
	validControlPlane := base
	validControlPlane.EventType = EventControlPlaneChanged
	validControlPlane.ActorType, validControlPlane.ActorID, validControlPlane.Reason, validControlPlane.CorrelationID = "user", "actor", "changed", "corr"
	validControlPlane.PreviousVersion, validControlPlane.NextVersion = ptr(1), ptr(2)
	if err := validControlPlane.Validate(); err != nil {
		t.Fatalf("valid control-plane event rejected: %v", err)
	}
	validError := base
	validError.ErrorType = string(ErrorTimeout)
	if err := validError.Validate(); err != nil {
		t.Fatalf("valid error type rejected: %v", err)
	}

	for _, usage := range []Usage{
		{InputTokens: ptr(1), OutputTokens: ptr(2), ModelCostMinor: ptr(3), ToolCostMinor: ptr(4), BudgetUsedTokens: ptr(5), BudgetUsedMinor: ptr(6), Currency: "USD", Provider: "provider", Model: "model", ExecutionResult: ResultFailure},
		{Provider: "provider", Model: "model"},
	} {
		if err := usage.Validate(); err != nil {
			t.Fatalf("valid usage rejected: %v", err)
		}
	}
	for _, value := range []string{"bad\x00value", "https://secret", "authorization secret", "secret=abc", "token=abc"} {
		usage := Usage{Provider: value}
		if !errors.Is(usage.Validate(), ErrInvalid) {
			t.Fatalf("invalid provider accepted: %q", value)
		}
	}

	cloned := testEvent("tenant-a", "event-1")
	cloned.Revision, cloned.LatencyMS, cloned.PreviousVersion, cloned.NextVersion = ptr(1), ptr(2), ptr(3), ptr(4)
	copy := cloned.Clone()
	*copy.Revision, *copy.LatencyMS, *copy.PreviousVersion, *copy.NextVersion = 9, 9, 9, 10
	if *cloned.Revision != 1 || *cloned.LatencyMS != 2 || *cloned.PreviousVersion != 3 || *cloned.NextVersion != 4 {
		t.Fatal("event clone was aliased")
	}
}

func TestStoreQueryAndContextBranches(t *testing.T) {
	store, err := NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	early := testEvent("tenant-a", "early")
	early.EventType = EventExecutionStarted
	early.OccurredAt = time.Unix(1, 0).UTC()
	late := testEvent("tenant-a", "late")
	late.OccurredAt = time.Unix(3, 0).UTC()
	if _, err := store.Append(context.Background(), early); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if got, err := store.List(context.Background(), Query{EventTypes: []EventType{EventExecutionStarted}, Since: time.Unix(0, 0).UTC(), Until: time.Unix(2, 0).UTC()}); err != nil || len(got) != 1 || got[0].EventID != "early" {
		t.Fatalf("filtered list = %#v, %v", got, err)
	}
	if _, err := store.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing get = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(canceled, "early"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled get = %v", err)
	}
	if _, err := store.List(canceled, Query{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled list = %v", err)
	}
	if _, err := store.AggregateUsage(canceled, UsageQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled aggregate = %v", err)
	}
	var nilStore *Store
	if _, err := nilStore.Get(context.Background(), "event"); !errors.Is(err, ErrTenantScope) {
		t.Fatalf("nil get = %v", err)
	}
	if _, err := nilStore.List(context.Background(), Query{}); !errors.Is(err, ErrTenantScope) {
		t.Fatalf("nil list = %v", err)
	}
	if _, err := nilStore.AggregateUsage(context.Background(), UsageQuery{}); !errors.Is(err, ErrTenantScope) {
		t.Fatalf("nil aggregate = %v", err)
	}
	backend := &Backend{}
	lockedStore, err := NewInMemoryWithBackend("tenant-a", backend)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &cancelOnSecondCheck{}
	if _, err := lockedStore.Append(ctx, testEvent("tenant-a", "canceled-before-commit")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled before commit = %v", err)
	}
	if _, err := lockedStore.Get(context.Background(), "canceled-before-commit"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canceled append persisted = %v", err)
	}
}

type cancelOnSecondCheck struct{ calls int }

func (c *cancelOnSecondCheck) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelOnSecondCheck) Done() <-chan struct{}       { return nil }
func (c *cancelOnSecondCheck) Err() error {
	c.calls++
	if c.calls > 1 {
		return context.Canceled
	}
	return nil
}
func (c *cancelOnSecondCheck) Value(any) any { return nil }

func ptr(value int64) *int64 { return &value }
