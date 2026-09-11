// Package backendmigration copies explicitly selected accepted data between
// interchangeable Worker storage adapters. Discovery and cutover remain owned
// by the caller so this module never scans a backend or changes a Manifest.
package backendmigration

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
)

var (
	ErrInvalidPlan = errors.New("backend migration plan is invalid")
	ErrVerify      = errors.New("backend migration verification failed")
)

type SessionStore interface {
	Load(context.Context, string, string, sessionstore.Head) (sessionstore.Candidate, error)
	Put(context.Context, sessionstore.Candidate) (sessionstore.Head, error)
}

type MemorySource interface {
	Load(context.Context, memorystore.Scope) (memorystore.Snapshot, error)
}

type MemoryTarget interface {
	MemorySource
	ImportSnapshot(context.Context, memorystore.Scope, memorystore.Snapshot) error
}

type SessionRoot struct {
	TenantID  string
	SessionID string
	Head      sessionstore.Head
}

type Plan struct {
	Sessions []SessionRoot
	Memories []memorystore.Scope
}

type Result struct {
	SessionsCopied int
	MemoriesCopied int
}

// Migrate is fail-fast and safely retryable. A failure can leave verified
// immutable Session candidates or exact Memory snapshots in the target; a
// rerun treats those identical values as success. Callers must quiesce writes
// before taking the formal roots used to construct plan.
func Migrate(
	ctx context.Context,
	plan Plan,
	sessionSource, sessionTarget SessionStore,
	memorySource MemorySource,
	memoryTarget MemoryTarget,
) (Result, error) {
	if err := validatePlan(plan, sessionSource, sessionTarget, memorySource, memoryTarget); err != nil {
		return Result{}, err
	}
	var result Result
	for index, root := range plan.Sessions {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		candidate, err := sessionSource.Load(ctx, root.TenantID, root.SessionID, root.Head)
		if err != nil {
			return result, fmt.Errorf("session[%d] source: %w", index, err)
		}
		head, err := sessionTarget.Put(ctx, candidate)
		if err != nil {
			return result, fmt.Errorf("session[%d] target: %w", index, err)
		}
		if head != root.Head {
			return result, fmt.Errorf("session[%d] head: %w", index, ErrVerify)
		}
		loaded, err := sessionTarget.Load(ctx, root.TenantID, root.SessionID, root.Head)
		if err != nil {
			return result, fmt.Errorf("session[%d] verify: %w", index, err)
		}
		sourceBody, sourceHead, sourceErr := candidate.Encode(maxInt)
		targetBody, targetHead, targetErr := loaded.Encode(maxInt)
		if sourceErr != nil || targetErr != nil || sourceHead != targetHead || string(sourceBody) != string(targetBody) {
			return result, fmt.Errorf("session[%d] content: %w", index, ErrVerify)
		}
		result.SessionsCopied++
	}
	for index, scope := range plan.Memories {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		snapshot, err := memorySource.Load(ctx, scope)
		if err != nil {
			return result, fmt.Errorf("memory[%d] source: %w", index, err)
		}
		if err = memoryTarget.ImportSnapshot(ctx, scope, snapshot); err != nil {
			return result, fmt.Errorf("memory[%d] target: %w", index, err)
		}
		loaded, err := memoryTarget.Load(ctx, scope)
		if err != nil {
			return result, fmt.Errorf("memory[%d] verify: %w", index, err)
		}
		sourceDigest, sourceErr := memorystore.SnapshotDigest(scope, snapshot)
		targetDigest, targetErr := memorystore.SnapshotDigest(scope, loaded)
		if sourceErr != nil || targetErr != nil || snapshot.Revision != loaded.Revision || sourceDigest != targetDigest {
			return result, fmt.Errorf("memory[%d] content: %w", index, ErrVerify)
		}
		result.MemoriesCopied++
	}
	return result, nil
}

const maxInt = int(^uint(0) >> 1)

func validatePlan(plan Plan, ss, st SessionStore, ms MemorySource, mt MemoryTarget) error {
	if (len(plan.Sessions) > 0 && (ss == nil || st == nil)) ||
		(len(plan.Memories) > 0 && (ms == nil || mt == nil)) ||
		(len(plan.Sessions) == 0 && len(plan.Memories) == 0) {
		return ErrInvalidPlan
	}
	seenSessions := map[string]bool{}
	for _, root := range plan.Sessions {
		key := root.TenantID + "\x00" + root.SessionID
		if root.TenantID == "" || root.SessionID == "" || root.Head.Validate() != nil || root.Head.Ref == "" || seenSessions[key] {
			return ErrInvalidPlan
		}
		seenSessions[key] = true
	}
	seenMemories := map[memorystore.Scope]bool{}
	for _, scope := range plan.Memories {
		if _, err := memorystore.SnapshotDigest(scope, memorystore.Snapshot{}); err != nil || seenMemories[scope] {
			return ErrInvalidPlan
		}
		seenMemories[scope] = true
	}
	return nil
}
