package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var attemptSDKKey = memory.UserKey{AppName: "session-app", UserID: "session-user"}
var attemptBoundKey = memory.UserKey{AppName: "tenant-agent", UserID: "trusted-subject"}

func freshMemoryAttempt(t *testing.T) *MemoryAttempt {
	t.Helper()
	a, err := NewMemoryAttempt(context.Background(), attemptSDKKey, attemptBoundKey, nil, 7)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}
func attemptEntries(t *testing.T, a *MemoryAttempt) []*memory.Entry {
	t.Helper()
	entries, err := a.ReadMemories(context.Background(), attemptSDKKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}
func attemptMemoryKey(id string) memory.Key {
	return memory.Key{AppName: attemptSDKKey.AppName, UserID: attemptSDKKey.UserID, MemoryID: id}
}

func TestMemoryAttemptRealSixSDKTools(t *testing.T) {
	a := freshMemoryAttempt(t)
	ctx := agent.NewInvocationContext(context.Background(), &agent.Invocation{MemoryService: a, Session: &session.Session{AppName: attemptSDKKey.AppName, UserID: attemptSDKKey.UserID}})
	tools := map[string]tool.CallableTool{}
	for _, value := range a.Tools() {
		tools[value.Declaration().Name] = value.(tool.CallableTool)
	}
	if len(tools) != 6 {
		t.Fatal("missing SDK tools")
	}
	call := func(name string, input any) {
		t.Helper()
		body, _ := json.Marshal(input)
		if _, err := tools[name].Call(ctx, body); err != nil {
			t.Fatal(err)
		}
	}
	call(memory.AddToolName, map[string]any{"memory": "likes green tea", "topics": []string{"tea"}, "user_id": "spoof"})
	entries := attemptEntries(t, a)
	if len(entries) != 1 {
		t.Fatal("read-your-writes missing")
	}
	call(memory.SearchToolName, map[string]any{"query": "green tea"})
	call(memory.LoadToolName, map[string]any{"limit": 10})
	call(memory.UpdateToolName, map[string]any{"memory_id": entries[0].ID, "memory": "likes black tea", "topics": []string{"tea"}})
	entries = attemptEntries(t, a)
	if entries[0].Memory.Memory != "likes black tea" {
		t.Fatal("update missing")
	}
	call(memory.DeleteToolName, map[string]any{"memory_id": entries[0].ID})
	if len(attemptEntries(t, a)) != 0 {
		t.Fatal("delete missing")
	}
	call(memory.AddToolName, map[string]any{"memory": "discard"})
	call(memory.ClearToolName, map[string]any{})
	call(memory.AddToolName, map[string]any{"memory": "keep"})
	candidate, err := a.Seal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Scope != attemptBoundKey || candidate.BaseRevision != 7 || len(candidate.Entries) != 1 || candidate.Entries[0].Memory.Memory != "keep" || candidate.Entries[0].UserID != attemptBoundKey.UserID {
		t.Fatalf("wrong candidate %+v", candidate)
	}
}
func TestMemoryAttemptDetachedSnapshotsAndOptions(t *testing.T) {
	ctx := context.Background()
	seed := inmemory.NewMemoryService()
	defer seed.Close()
	when := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := seed.AddMemory(ctx, attemptBoundKey, "tea", []string{"drink"}, memory.WithMetadata(&memory.Metadata{Kind: memory.KindEpisode, EventTime: &when, Participants: []string{"alice"}})); err != nil {
		t.Fatal(err)
	}
	entries, _ := seed.ReadMemories(ctx, attemptBoundKey, 0)
	entries[0].CreatedAt = when
	entries[0].UpdatedAt = when
	entries[0].Memory.LastUpdated = &when
	original := cloneMemoryEntries(entries)
	a, err := NewMemoryAttempt(ctx, attemptSDKKey, attemptBoundKey, entries, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	entries[0].Memory.Topics[0] = "leak"
	entries[0].Memory.Participants[0] = "leak"
	entries[0].Memory.EventTime = &time.Time{}
	read := attemptEntries(t, a)
	if read[0].Memory.Topics[0] != "drink" || read[0].Memory.Participants[0] != "alice" || !read[0].CreatedAt.Equal(when) {
		t.Fatal("seed aliases or timestamps lost")
	}
	read[0].Memory.Memory = "corrupt"
	*read[0].Memory.LastUpdated = time.Time{}
	found, err := a.SearchMemories(ctx, attemptSDKKey, "tea")
	if err != nil || len(found) != 1 {
		t.Fatalf("search %v %v", found, err)
	}
	found[0].Memory.Topics[0] = "search leak"
	c, err := a.Seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Entries, original) {
		t.Fatalf("snapshot changed: got %+v want %+v", c.Entries[0], original[0])
	}
	c.Entries[0].Memory.Topics[0] = "candidate leak"
	again, _ := a.Seal(ctx)
	if again.Entries[0].Memory.Topics[0] != "drink" {
		t.Fatal("candidate aliases view")
	}
}
func TestMemoryAttemptMutationMetadataDetached(t *testing.T) {
	a := freshMemoryAttempt(t)
	ctx := context.Background()
	when := time.Now()
	topics := []string{"tea"}
	meta := &memory.Metadata{Kind: memory.KindEpisode, EventTime: &when, Participants: []string{"alice"}}
	if err := a.AddMemory(ctx, attemptSDKKey, "tea", topics, memory.WithMetadata(meta)); err != nil {
		t.Fatal(err)
	}
	topics[0] = "bad"
	meta.Participants[0] = "bad"
	when = time.Time{}
	entry := attemptEntries(t, a)[0]
	if entry.Memory.Topics[0] != "tea" || entry.Memory.Participants[0] != "alice" || entry.Memory.EventTime.IsZero() {
		t.Fatal("add alias")
	}
	updateWhen := time.Now()
	updateMeta := &memory.Metadata{Kind: memory.KindEpisode, EventTime: &updateWhen, Participants: []string{"bob"}}
	result := memory.UpdateResult{}
	if err := a.UpdateMemory(ctx, attemptMemoryKey(entry.ID), "coffee", []string{"coffee"}, memory.WithUpdateMetadata(updateMeta), memory.WithUpdateResult(&result)); err != nil {
		t.Fatal(err)
	}
	updateMeta.Participants[0] = "bad"
	updateWhen = time.Time{}
	entry = attemptEntries(t, a)[0]
	if entry.ID != result.MemoryID || entry.Memory.Participants[0] != "bob" || entry.Memory.EventTime.IsZero() {
		t.Fatal("update alias/result")
	}
	result.MemoryID = "external"
	if attemptEntries(t, a)[0].ID == "external" {
		t.Fatal("result alias")
	}
}
func TestMemoryAttemptScopeLifecycleAndCancel(t *testing.T) {
	ctx := context.Background()
	a := freshMemoryAttempt(t)
	wrong := memory.UserKey{AppName: attemptSDKKey.AppName, UserID: "spoof"}
	if err := a.AddMemory(ctx, wrong, "bad", nil); !errors.Is(err, ErrMemoryScope) {
		t.Fatal(err)
	}
	if _, err := a.ReadMemories(ctx, wrong, 0); !errors.Is(err, ErrMemoryScope) {
		t.Fatal(err)
	}
	if _, err := a.SearchMemories(ctx, wrong, "tea"); !errors.Is(err, ErrMemoryScope) {
		t.Fatal(err)
	}
	if err := a.ClearMemories(ctx, wrong); !errors.Is(err, ErrMemoryScope) {
		t.Fatal(err)
	}
	key := memory.Key{AppName: wrong.AppName, UserID: wrong.UserID, MemoryID: "id"}
	if err := a.UpdateMemory(ctx, key, "bad", nil); !errors.Is(err, ErrMemoryScope) {
		t.Fatal(err)
	}
	if err := a.DeleteMemory(ctx, key); !errors.Is(err, ErrMemoryScope) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := a.AddMemory(canceled, attemptSDKKey, "bad", nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := a.Seal(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := a.EnqueueAutoMemoryJob(ctx, &session.Session{AppName: attemptSDKKey.AppName, UserID: attemptSDKKey.UserID}); err != nil {
		t.Fatal(err)
	}
	if len(attemptEntries(t, a)) != 0 {
		t.Fatal("auto extraction ran")
	}
	a.Seal(ctx)
	if err := a.AddMemory(ctx, attemptSDKKey, "bad", nil); !errors.Is(err, ErrMemorySealed) {
		t.Fatal(err)
	}
	if err := a.ClearMemories(ctx, attemptSDKKey); !errors.Is(err, ErrMemorySealed) {
		t.Fatal(err)
	}
	a.Close()
	a.Close()
	if _, err := a.ReadMemories(ctx, attemptSDKKey, 0); !errors.Is(err, ErrMemoryClosed) {
		t.Fatal(err)
	}
	if _, err := a.Seal(ctx); !errors.Is(err, ErrMemoryClosed) {
		t.Fatal(err)
	}
	if a.Tools() != nil {
		t.Fatal("closed tools exposed")
	}
}
func TestMemoryAttemptRejectsBadSnapshotAndRRFTuning(t *testing.T) {
	ctx := context.Background()
	a := freshMemoryAttempt(t)
	a.AddMemory(ctx, attemptSDKKey, "tea", nil)
	candidate, _ := a.Seal(ctx)
	for _, mode := range []string{"id", "scope", "nil", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			entries := cloneMemoryEntries(candidate.Entries)
			switch mode {
			case "id":
				entries[0].ID = "invalid"
			case "scope":
				entries[0].UserID = "other"
			case "nil":
				entries[0].Memory = nil
			case "duplicate":
				entries = append(entries, entries[0])
			}
			if _, err := NewMemoryAttempt(ctx, attemptSDKKey, attemptBoundKey, entries, 0); !errors.Is(err, ErrMemorySnapshot) {
				t.Fatal(err)
			}
		})
	}
	if _, err := a.SearchMemories(ctx, attemptSDKKey, "tea", memory.WithSearchOptions(memory.SearchOptions{HybridSearch: true, HybridRRFK: 60})); !errors.Is(err, ErrMemorySearch) {
		t.Fatal(err)
	}
}
func TestMemoryAttemptConcurrentWritesAndSeal(t *testing.T) {
	a := freshMemoryAttempt(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 150; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := a.AddMemory(ctx, attemptSDKKey, fmt.Sprintf("tea %d", i), nil); err != nil {
				t.Error(err)
			}
			if _, err := a.ReadMemories(ctx, attemptSDKKey, 0); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	c, err := a.Seal(ctx)
	if err != nil || len(c.Entries) != 150 {
		t.Fatalf("unexpected implicit cap/count %d %v", len(c.Entries), err)
	}
	found, err := a.SearchMemories(ctx, attemptSDKKey, "tea")
	if err != nil || len(found) != 150 {
		t.Fatalf("implicit search cap: %d %v", len(found), err)
	}
}

func TestMemoryAttemptSDKRunnerToCandidate(t *testing.T) {
	a := freshMemoryAttempt(t)
	options, err := BuildCapabilityOptions(CapabilityConfig{MemoryTools: []string{memory.AddToolName}}, CapabilityServices{Memory: a})
	if err != nil {
		t.Fatal(err)
	}
	configured := applyCapabilityAgent(options.Agent)
	observer := &capabilityObserver{seen: make(chan *agent.Invocation, 1)}
	result := make(chan error, 1)
	observer.call = func(ctx context.Context, inv *agent.Invocation) error {
		_, err := configured.Tools[0].(tool.CallableTool).Call(agent.NewInvocationContext(ctx, inv), []byte(`{"memory":"runner remembers tea"}`))
		result <- err
		return err
	}
	r := runner.NewRunner(attemptSDKKey.AppName, observer, options.Runner...)
	defer r.Close()
	events, err := r.Run(context.Background(), attemptSDKKey.UserID, "test-session", model.NewUserMessage("remember tea"))
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("tool not executed")
	}
	c, err := a.Seal(context.Background())
	if err != nil || len(c.Entries) != 1 || c.Entries[0].Memory.Memory != "runner remembers tea" {
		t.Fatalf("candidate %+v %v", c, err)
	}
	if inv := <-observer.seen; inv.MemoryService != a {
		t.Fatal("Runner bypassed Attempt view")
	}
	// No persistence service is passed or reachable: this result is a candidate,
	// not proof of accepted-Run integration or a formal backend write.
}

func TestMemoryAttemptCanceledMethodsAndDiscard(t *testing.T) {
	a := freshMemoryAttempt(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewMemoryAttempt(ctx, attemptSDKKey, attemptBoundKey, nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	calls := []func() error{
		func() error { return a.AddMemory(ctx, attemptSDKKey, "bad", nil) },
		func() error { return a.UpdateMemory(ctx, attemptMemoryKey("id"), "bad", nil) },
		func() error { return a.DeleteMemory(ctx, attemptMemoryKey("id")) },
		func() error { return a.ClearMemories(ctx, attemptSDKKey) },
		func() error { _, err := a.ReadMemories(ctx, attemptSDKKey, 0); return err },
		func() error { _, err := a.SearchMemories(ctx, attemptSDKKey, "bad"); return err },
		func() error {
			return a.EnqueueAutoMemoryJob(ctx, &session.Session{AppName: attemptSDKKey.AppName, UserID: attemptSDKKey.UserID})
		},
	}
	for i, call := range calls {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Fatalf("method %d: %v", i, err)
		}
	}
	if err := a.AddMemory(context.Background(), attemptSDKKey, "unaccepted", nil); err != nil {
		t.Fatal(err)
	}
	a.Close()
	if _, err := a.Seal(context.Background()); !errors.Is(err, ErrMemoryClosed) {
		t.Fatal("closed attempt exported candidate", err)
	}
	independent := freshMemoryAttempt(t)
	if len(attemptEntries(t, independent)) != 0 {
		t.Fatal("failed attempt contaminated another view")
	}
}

func TestMemoryAttemptSealSerializesConcurrentMutation(t *testing.T) {
	a := freshMemoryAttempt(t)
	ctx := context.Background()
	start := make(chan struct{})
	outcomes := make(chan error, 60)
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			outcomes <- a.AddMemory(ctx, attemptSDKKey, fmt.Sprintf("concurrent %d", i), nil)
		}(i)
	}
	close(start)
	candidate, err := a.Seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(outcomes)
	accepted := 0
	for err := range outcomes {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrMemorySealed) {
			t.Fatal(err)
		}
	}
	if len(candidate.Entries) != accepted {
		t.Fatalf("snapshot not atomic: entries=%d mutations=%d", len(candidate.Entries), accepted)
	}
	again, err := a.Seal(ctx)
	if err != nil || !reflect.DeepEqual(candidate, again) {
		t.Fatal("sealed candidate changed", err)
	}
}

func TestMemoryAttemptToolDeclarationsDetached(t *testing.T) {
	a := freshMemoryAttempt(t)
	tools := a.Tools()
	tools[0].Declaration().Name = "tampered"
	tools[1] = nil
	fresh := a.Tools()
	if fresh[0].Declaration().Name != memory.AddToolName || fresh[1] == nil {
		t.Fatal("SDK tool declarations aliased")
	}
}

func TestMemoryAttemptRejectsSwappedCanonicalIDs(t *testing.T) {
	for _, sameText := range []bool{false, true} {
		t.Run(fmt.Sprint(sameText), func(t *testing.T) {
			ctx := context.Background()
			a := freshMemoryAttempt(t)
			for i, location := range []string{"north", "south"} {
				value := "same text"
				if !sameText {
					value = fmt.Sprintf("text %d", i)
				}
				if err := a.AddMemory(ctx, attemptSDKKey, value, nil, memory.WithMetadata(&memory.Metadata{Location: location})); err != nil {
					t.Fatal(err)
				}
			}
			candidate, err := a.Seal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			entries := candidate.Entries
			if len(entries) != 2 {
				t.Fatal(len(entries))
			}
			entries[0].ID, entries[1].ID = entries[1].ID, entries[0].ID
			before := cloneMemoryEntries(entries)
			got, err := NewMemoryAttempt(ctx, attemptSDKKey, attemptBoundKey, entries, 7)
			if got != nil {
				got.Close()
			}
			if !errors.Is(err, ErrMemorySnapshot) {
				t.Fatalf("swapped canonical IDs accepted: %v", err)
			}
			if !reflect.DeepEqual(entries, before) {
				t.Fatal("input snapshot mutated")
			}
		})
	}
}
