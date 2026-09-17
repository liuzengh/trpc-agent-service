// Package inmemory provides immutable profile snapshots for local tests.
package inmemory

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Resolver struct {
	mu        sync.RWMutex
	snapshots map[profile.ExecutionProfileKey]profile.ExecutionProfileSnapshot
}

func NewResolver(snapshots ...profile.ExecutionProfileSnapshot) *Resolver {
	r := &Resolver{snapshots: make(map[profile.ExecutionProfileKey]profile.ExecutionProfileSnapshot)}
	for _, snapshot := range snapshots {
		r.Put(snapshot)
	}
	return r
}

func (r *Resolver) Put(snapshot profile.ExecutionProfileSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshots[snapshot.Key] = clone(snapshot)
}

func (r *Resolver) Resolve(ctx context.Context, key profile.ExecutionProfileKey) (profile.ExecutionProfileSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return profile.ExecutionProfileSnapshot{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot, ok := r.snapshots[key]
	if !ok {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrNotFound
	}
	if snapshot.Key != key || snapshot.ContentDigest != key.ContentDigest {
		return profile.ExecutionProfileSnapshot{}, runtime.ErrVersionMismatch
	}
	return clone(snapshot), nil
}

func clone(in profile.ExecutionProfileSnapshot) profile.ExecutionProfileSnapshot {
	b, _ := json.Marshal(in)
	var out profile.ExecutionProfileSnapshot
	_ = json.Unmarshal(b, &out)
	return out
}
