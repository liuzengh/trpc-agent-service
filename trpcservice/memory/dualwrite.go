package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// DualWritePlan is immutable bundle state projected from the migration
// authority.  The primary service remains authoritative; a target failure is
// recovered from the durable ledger rather than being hidden in memory.
type DualWritePlan struct {
	TenantID, MigrationID string
	Epoch, ConfigVersion  int64
	Direction             memorydriver.Direction
	Ledger                memorydriver.MutationLedger
	Target                memorydriver.UserApplier
}

// NewDualWriteService decorates a scoped primary service.  It is deliberately
// constructed only by Worker composition after the exact ConfigSnapshot and
// migration authority have been checked.
func NewDualWriteService(primary agentmemory.Service, plan DualWritePlan) (agentmemory.Service, error) {
	if primary == nil || plan.TenantID == "" || plan.MigrationID == "" || plan.Epoch < 1 || plan.ConfigVersion < 1 ||
		(plan.Direction != memorydriver.DirectionForward && plan.Direction != memorydriver.DirectionReverse) || plan.Ledger == nil || plan.Target == nil {
		return nil, runtime.ErrInvariantViolation
	}
	return dualWriteService{primary: primary, plan: plan}, nil
}

type dualWriteService struct {
	primary agentmemory.Service
	plan    DualWritePlan
}

func (s dualWriteService) ReadMemories(ctx context.Context, key agentmemory.UserKey, limit int) ([]*agentmemory.Entry, error) {
	return s.primary.ReadMemories(ctx, key, limit)
}
func (s dualWriteService) SearchMemories(ctx context.Context, key agentmemory.UserKey, query string, opts ...agentmemory.SearchOption) ([]*agentmemory.Entry, error) {
	return s.primary.SearchMemories(ctx, key, query, opts...)
}
func (s dualWriteService) AddMemory(ctx context.Context, key agentmemory.UserKey, value string, topics []string, opts ...agentmemory.AddOption) error {
	if err := s.validateWriteContext(ctx, key); err != nil {
		return err
	}
	if err := s.primary.AddMemory(ctx, key, value, topics, opts...); err != nil {
		return err
	}
	return s.sync(ctx, key, "add")
}
func (s dualWriteService) UpdateMemory(ctx context.Context, key agentmemory.Key, value string, topics []string, opts ...agentmemory.UpdateOption) error {
	if err := s.validateWriteContext(ctx, agentmemory.UserKey{AppName: key.AppName, UserID: key.UserID}); err != nil {
		return err
	}
	if err := s.primary.UpdateMemory(ctx, key, value, topics, opts...); err != nil {
		return err
	}
	return s.sync(ctx, agentmemory.UserKey{AppName: key.AppName, UserID: key.UserID}, "update")
}
func (s dualWriteService) DeleteMemory(ctx context.Context, key agentmemory.Key) error {
	if err := s.validateWriteContext(ctx, agentmemory.UserKey{AppName: key.AppName, UserID: key.UserID}); err != nil {
		return err
	}
	if err := s.primary.DeleteMemory(ctx, key); err != nil {
		return err
	}
	return s.sync(ctx, agentmemory.UserKey{AppName: key.AppName, UserID: key.UserID}, "delete")
}
func (s dualWriteService) ClearMemories(ctx context.Context, key agentmemory.UserKey) error {
	if err := s.validateWriteContext(ctx, key); err != nil {
		return err
	}
	if err := s.primary.ClearMemories(ctx, key); err != nil {
		return err
	}
	return s.sync(ctx, key, "clear")
}
func (s dualWriteService) Tools() []tool.Tool { return s.primary.Tools() }
func (s dualWriteService) EnqueueAutoMemoryJob(ctx context.Context, value *session.Session) error {
	// Upstream auto extraction performs writes on its own asynchronous worker,
	// outside the request context that supplies the durable mutation id.  Do
	// not permit an unobservable source-only write while a migration is active.
	return runtime.ErrCapabilityUnsupported
}
func (s dualWriteService) Close() error { return s.primary.Close() }

func (s dualWriteService) sync(ctx context.Context, key agentmemory.UserKey, operation string) error {
	execution, err := s.writeExecution(ctx, key)
	if err != nil {
		return err
	}
	// 10,000 is an explicit migration-window safety ceiling: the target needs
	// an exact user image to make clear/delete repairable. Tenants with users
	// beyond this limit must raise the limit through a paginated image adapter
	// before enabling an online migration.
	entries, err := s.primary.ReadMemories(ctx, key, 10_000)
	if err != nil {
		return err
	}
	images := make([]memorydriver.Image, 0, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.AppName != key.AppName || entry.UserID != key.UserID {
			return runtime.ErrTenantScope
		}
		images = append(images, memorydriver.Image{Entry: *entry})
	}
	digest, err := memorydriver.Digest(images)
	if err != nil {
		return err
	}
	if targetDigest, applyErr := s.plan.Target.ApplyUser(ctx, memorydriver.UserKey{TenantID: s.plan.TenantID, AppName: key.AppName, UserID: key.UserID}, images); applyErr == nil {
		if targetDigest == digest {
			return nil
		}
		return runtime.ErrInvariantViolation
	}
	mutationID := stableMutationID(execution.RequestID, operation, key, digest)
	_, recordErr := s.plan.Ledger.Record(ctx, memorydriver.RecordRequest{TenantID: s.plan.TenantID, MigrationID: s.plan.MigrationID,
		MutationID: mutationID, Epoch: s.plan.Epoch, ConfigVersion: s.plan.ConfigVersion, Direction: s.plan.Direction,
		Key: memorydriver.UserKey{TenantID: s.plan.TenantID, AppName: key.AppName, UserID: key.UserID}, SourceDigest: digest, CreatedAt: time.Now().UTC()})
	if recordErr != nil {
		return fmt.Errorf("record memory migration repair: %w", recordErr)
	}
	return nil
}

func (s dualWriteService) validateWriteContext(ctx context.Context, key agentmemory.UserKey) error {
	_, err := s.writeExecution(ctx, key)
	return err
}

func (s dualWriteService) writeExecution(ctx context.Context, key agentmemory.UserKey) (runtime.ExecutionContext, error) {
	if key.AppName == "" || key.UserID == "" || !strings.HasPrefix(key.AppName, s.plan.TenantID+"/") {
		return runtime.ExecutionContext{}, runtime.ErrTenantScope
	}
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if !ok || execution.TenantID != s.plan.TenantID || execution.RequestID == "" {
		return runtime.ExecutionContext{}, runtime.ErrInvariantViolation
	}
	return execution, nil
}

func stableMutationID(requestID, operation string, key agentmemory.UserKey, digest string) string {
	sum := sha256.Sum256([]byte("memory-dual-write-v1\x00" + requestID + "\x00" + operation + "\x00" + key.AppName + "\x00" + key.UserID + "\x00" + digest))
	return "mem_" + hex.EncodeToString(sum[:16])
}

var _ agentmemory.Service = dualWriteService{}
