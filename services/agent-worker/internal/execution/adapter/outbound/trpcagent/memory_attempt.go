package trpcagent

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	memorytool "trpc.group/trpc-go/trpc-agent-go/memory/tool"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	ErrMemoryScope    = errors.New("memory attempt scope mismatch")
	ErrMemoryClosed   = errors.New("memory attempt closed")
	ErrMemorySealed   = errors.New("memory attempt sealed")
	ErrMemorySnapshot = errors.New("invalid memory snapshot")
	ErrMemorySearch   = errors.New("unsupported memory search options")
)

// MemoryCandidate is only a detached candidate, not a committed Memory revision.
// The completion owner must durably associate it with an accepted Attempt and
// apply it using BaseRevision CAS. This module has no persistence dependency.
type MemoryCandidate struct {
	Scope        memory.UserKey
	BaseRevision uint64
	Entries      []*memory.Entry
}

// MemoryAttempt confines a root SDK inmemory service to one Attempt and a fixed
// trusted scope. It never accepts a persistent store/service or an extractor.
// All SDK pointers stay private; callers must not concurrently mutate arguments
// during a method call. Close discards the private view, not a formal revision.
type MemoryAttempt struct {
	mu               sync.Mutex
	sdkKey, boundKey memory.UserKey
	baseRevision     uint64
	service          *inmemory.MemoryService
	sealed, closed   bool
}

var _ memory.Service = (*MemoryAttempt)(nil)

func NewMemoryAttempt(ctx context.Context, sdkKey, boundKey memory.UserKey, entries []*memory.Entry, baseRevision uint64) (*MemoryAttempt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sdkKey.CheckUserKey() != nil || boundKey.CheckUserKey() != nil {
		return nil, ErrMemoryScope
	}
	a := &MemoryAttempt{sdkKey: sdkKey, boundKey: boundKey, baseRevision: baseRevision, service: inmemory.NewMemoryService(inmemory.WithMemoryLimit(math.MaxInt), inmemory.WithMaxResults(0))}
	detached := make(map[string]*memory.Entry, len(entries))
	for _, input := range entries {
		if err := ctx.Err(); err != nil {
			a.Close()
			return nil, err
		}
		if input == nil || input.Memory == nil || input.AppName != boundKey.AppName || input.UserID != boundKey.UserID || detached[input.ID] != nil {
			a.Close()
			return nil, ErrMemorySnapshot
		}
		entry := cloneMemoryEntry(input)
		metadata := &memory.Metadata{Kind: entry.Memory.Kind, EventTime: entry.Memory.EventTime, Participants: entry.Memory.Participants, Location: entry.Memory.Location}
		// Validate each claimed ID against this entry using the SDK generator.
		// A batch ID-set comparison alone would accept two swapped IDs.
		probe := inmemory.NewMemoryService()
		err := probe.AddMemory(ctx, boundKey, entry.Memory.Memory, entry.Memory.Topics, memory.WithMetadata(metadata))
		var canonical []*memory.Entry
		if err == nil {
			canonical, err = probe.ReadMemories(ctx, boundKey, 0)
		}
		probe.Close()
		if err != nil {
			a.Close()
			return nil, err
		}
		if len(canonical) != 1 || canonical[0].ID != entry.ID {
			a.Close()
			return nil, ErrMemorySnapshot
		}
		if err := a.service.AddMemory(ctx, boundKey, entry.Memory.Memory, entry.Memory.Topics, memory.WithMetadata(metadata)); err != nil {
			a.Close()
			return nil, err
		}
		detached[entry.ID] = entry
	}
	// v1.11.2 ReadMemories returns owned pointers. Seed through the public SDK
	// canonical-ID generator, then restore the detached persisted timestamps.
	// Read once rather than sorting the growing snapshot after every insertion.
	// No pointer from this narrow compatibility seam escapes our mutex/module.
	seeded, err := a.service.ReadMemories(ctx, boundKey, 0)
	if err != nil || len(seeded) != len(detached) {
		a.Close()
		return nil, ErrMemorySnapshot
	}
	for _, value := range seeded {
		entry := detached[value.ID]
		if entry == nil {
			a.Close()
			return nil, ErrMemorySnapshot
		}
		*value = *entry
	}
	return a, nil
}

func (a *MemoryAttempt) check(ctx context.Context, key memory.UserKey, write bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.closed {
		return ErrMemoryClosed
	}
	if key != a.sdkKey {
		return ErrMemoryScope
	}
	if write && a.sealed {
		return ErrMemorySealed
	}
	return nil
}
func (a *MemoryAttempt) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, opts ...memory.AddOption) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx, key, true); err != nil {
		return err
	}
	return a.service.AddMemory(ctx, a.boundKey, value, append([]string(nil), topics...), memory.WithMetadata(cloneMemoryMetadata(memory.ResolveAddOptions(opts))))
}
func (a *MemoryAttempt) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, opts ...memory.UpdateOption) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx, memory.UserKey{AppName: key.AppName, UserID: key.UserID}, true); err != nil {
		return err
	}
	result := memory.UpdateResult{}
	// The result sink is copied back only after success; never retained by SDK.
	metadata := cloneMemoryMetadata(memory.ResolveUpdateOptions(opts))
	sink := memory.ResolveUpdateResult(opts)
	err := a.service.UpdateMemory(ctx, memory.Key{AppName: a.boundKey.AppName, UserID: a.boundKey.UserID, MemoryID: key.MemoryID}, value, append([]string(nil), topics...), memory.WithUpdateMetadata(metadata), memory.WithUpdateResult(&result))
	if err == nil && sink != nil {
		*sink = result
	}
	return err
}
func (a *MemoryAttempt) DeleteMemory(ctx context.Context, key memory.Key) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx, memory.UserKey{AppName: key.AppName, UserID: key.UserID}, true); err != nil {
		return err
	}
	return a.service.DeleteMemory(ctx, memory.Key{AppName: a.boundKey.AppName, UserID: a.boundKey.UserID, MemoryID: key.MemoryID})
}
func (a *MemoryAttempt) ClearMemories(ctx context.Context, key memory.UserKey) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx, key, true); err != nil {
		return err
	}
	return a.service.ClearMemories(ctx, a.boundKey)
}
func (a *MemoryAttempt) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx, key, false); err != nil {
		return nil, err
	}
	entries, err := a.service.ReadMemories(ctx, a.boundKey, limit)
	return a.readResult(entries), err
}

// SearchMemories uses SDK keyword search (default score threshold 0.3), kind,
// temporal filters, explicit result/score options, tie ordering, fallback and
// deduplication. The SDK search tool always sets HybridSearch=true, even for
// keyword-only backends. Accept this SDK hint using the inmemory keyword
// semantics; no vector index exists here. Explicit RRF tuning is unsupported.
func (a *MemoryAttempt) SearchMemories(ctx context.Context, key memory.UserKey, query string, opts ...memory.SearchOption) ([]*memory.Entry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx, key, false); err != nil {
		return nil, err
	}
	resolved := memory.ResolveSearchOptions(query, opts)
	if resolved.HybridRRFK != 0 {
		return nil, ErrMemorySearch
	}
	resolved.HybridSearch = false // Explicit keyword-only backend contract.
	resolved.TimeAfter = cloneMemoryTime(resolved.TimeAfter)
	resolved.TimeBefore = cloneMemoryTime(resolved.TimeBefore)
	entries, err := a.service.SearchMemories(ctx, a.boundKey, query, memory.WithSearchOptions(resolved))
	return a.readResult(entries), err
}
func (a *MemoryAttempt) readResult(entries []*memory.Entry) []*memory.Entry {
	out := cloneMemoryEntries(entries)
	for _, entry := range out {
		entry.AppName = a.sdkKey.AppName
		entry.UserID = a.sdkKey.UserID
	}
	return out
}
func (a *MemoryAttempt) Tools() []tool.Tool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	// Fresh SDK tools prevent declaration/slice aliases; each tool resolves this
	// final service from Invocation, not from the private in-memory backend.
	return []tool.Tool{memorytool.NewAddTool(), memorytool.NewUpdateTool(), memorytool.NewDeleteTool(), memorytool.NewClearTool(), memorytool.NewSearchTool(), memorytool.NewLoadTool()}
}

// EnqueueAutoMemoryJob intentionally performs no extraction and starts no worker.
func (a *MemoryAttempt) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if sess == nil {
		return ErrMemoryScope
	}
	return a.check(ctx, memory.UserKey{AppName: sess.AppName, UserID: sess.UserID}, false)
}
func (a *MemoryAttempt) Seal(ctx context.Context) (MemoryCandidate, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx, a.sdkKey, false); err != nil {
		return MemoryCandidate{}, err
	}
	entries, err := a.service.ReadMemories(ctx, a.boundKey, 0)
	if err != nil {
		return MemoryCandidate{}, err
	}
	out := cloneMemoryEntries(entries)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	a.sealed = true
	return MemoryCandidate{Scope: a.boundKey, BaseRevision: a.baseRevision, Entries: out}, nil
}
func (a *MemoryAttempt) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	err := a.service.Close()
	a.service = nil
	return err
}
func cloneMemoryTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}
func cloneMemoryMetadata(value *memory.Metadata) *memory.Metadata {
	if value == nil {
		return nil
	}
	out := *value
	out.EventTime = cloneMemoryTime(value.EventTime)
	out.Participants = append([]string(nil), value.Participants...)
	return &out
}
func cloneMemoryEntry(value *memory.Entry) *memory.Entry {
	out := *value
	if value.Memory != nil {
		m := *value.Memory
		m.Topics = append([]string(nil), m.Topics...)
		m.Participants = append([]string(nil), m.Participants...)
		m.EventTime = cloneMemoryTime(m.EventTime)
		m.LastUpdated = cloneMemoryTime(m.LastUpdated)
		out.Memory = &m
	}
	return &out
}
func cloneMemoryEntries(values []*memory.Entry) []*memory.Entry {
	out := make([]*memory.Entry, len(values))
	for i, value := range values {
		out[i] = cloneMemoryEntry(value)
	}
	return out
}
