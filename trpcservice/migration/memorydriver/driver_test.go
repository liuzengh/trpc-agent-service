package memorydriver

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationmemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
)

type images struct{ values []Image }

func (i images) ExportTenant(_ context.Context, _ string) ([]Image, string, error) {
	digest, err := Digest(i.values)
	return i.values, digest, err
}
func (i images) Apply(_ context.Context, values []Image) (string, error) { return Digest(values) }
func (i images) ApplyUser(_ context.Context, _ UserKey, values []Image) (string, error) {
	return Digest(values)
}
func (i images) LoadUser(_ context.Context, key UserKey) ([]Image, string, error) {
	result := make([]Image, 0)
	for _, image := range i.values {
		if image.Entry.AppName == key.AppName && image.Entry.UserID == key.UserID {
			result = append(result, image)
		}
	}
	digest, err := Digest(result)
	return result, digest, err
}

type repairLedger struct{ items []Mutation }

func (l *repairLedger) Record(_ context.Context, _ RecordRequest) (Mutation, error) {
	panic("not used")
}
func (l *repairLedger) Claim(_ context.Context, in ClaimRequest) ([]Mutation, error) {
	result := make([]Mutation, 0, in.Limit)
	for index := range l.items {
		if len(result) == in.Limit || l.items[index].State == StateApplied {
			continue
		}
		l.items[index].State, l.items[index].LeaseOwner = StateApplying, in.WorkerID
		l.items[index].Version++
		result = append(result, l.items[index])
	}
	return result, nil
}
func (l *repairLedger) MarkApplied(_ context.Context, in CompleteRequest) (Mutation, error) {
	for index := range l.items {
		item := &l.items[index]
		if item.MutationID == in.MutationID && item.Version == in.ExpectedVersion && item.LeaseOwner == in.WorkerID {
			item.State, item.TargetDigest, item.Version = StateApplied, in.TargetDigest, item.Version+1
			return *item, nil
		}
	}
	return Mutation{}, runtime.ErrNotFound
}
func (l *repairLedger) MarkRetry(_ context.Context, _ RetryRequest) (Mutation, error) {
	panic("not used")
}
func (l *repairLedger) Outstanding(_ context.Context, _, _ string) (int64, error) {
	var count int64
	for _, item := range l.items {
		if item.State != StateApplied {
			count++
		}
	}
	return count, nil
}

func entry(appName, userID, id, value string) agentmemory.Entry {
	now := time.Unix(100, 0).UTC()
	return agentmemory.Entry{ID: id, AppName: appName, UserID: userID, Memory: &agentmemory.Memory{Memory: value}, CreatedAt: now, UpdatedAt: now}
}

func TestDriverBackfillVerifyDoesNotCutover(t *testing.T) {
	ctx, now := context.Background(), time.Unix(100, 0).UTC()
	authority := migrationmemory.New()
	current, err := authority.Create(ctx, migration.CreateRequest{TenantID: "tenant-a", MigrationID: "memory-move", Domain: Domain, Epoch: 2,
		Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "redis", BackendVersion: 1}, Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "postgres", BackendVersion: 1}, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	for index, state := range []migration.State{migration.StateSnapshot, migration.StateDualWrite, migration.StateBackfill} {
		request := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: state, At: now.Add(time.Duration(index+1) * time.Second)}
		if state == migration.StateSnapshot {
			request.SnapshotWatermark = "memory-snapshot"
		}
		if state == migration.StateDualWrite {
			request.DualWriteRef = "memory-dual-write"
		}
		current, err = authority.Transition(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
	}
	value := Image{Entry: entry("tenant-a/app", "u", "m1", "value")}
	driver := Driver{Authority: authority, Source: images{values: []Image{value}}, Target: images{}, Ledger: &repairLedger{}}
	backfill, err := driver.BackfillOnce(ctx, "tenant-a", "memory-move", now.Add(4*time.Second))
	if err != nil || !backfill.Migration.BackfillComplete || backfill.Migration.State != migration.StateBackfill {
		t.Fatalf("backfill=%#v error=%v", backfill, err)
	}
	verified, err := driver.EnterVerify(ctx, "tenant-a", "memory-move", now.Add(5*time.Second))
	if err != nil || verified.State != migration.StateVerify || verified.CutoverConfigVersion != 0 {
		t.Fatalf("verify transition=%#v error=%v", verified, err)
	}
	evidence, err := driver.Verify(ctx, "tenant-a", "memory-move", images{values: []Image{value}})
	if err != nil || evidence.SourceCount != 1 || evidence.SourceDigest != evidence.TargetDigest {
		t.Fatalf("evidence=%#v error=%v", evidence, err)
	}
}

func TestDriverRepairReplaysCurrentUserImage(t *testing.T) {
	ctx, now := context.Background(), time.Unix(200, 0).UTC()
	authority := migrationmemory.New()
	current, err := authority.Create(ctx, migration.CreateRequest{TenantID: "tenant-a", MigrationID: "memory-repair", Domain: Domain, Epoch: 2,
		Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "redis", BackendVersion: 1}, Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "postgres", BackendVersion: 1}, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	current, err = authority.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateSnapshot, SnapshotWatermark: "snapshot", At: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	current, err = authority.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateDualWrite, DualWriteRef: "ledger", At: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	value := Image{Entry: entry("tenant-a/app", "u", "m1", "new value")}
	digest, err := Digest([]Image{value})
	if err != nil {
		t.Fatal(err)
	}
	ledger := &repairLedger{items: []Mutation{{TenantID: "tenant-a", MigrationID: "memory-repair", MutationID: "request-1", Epoch: current.Epoch, Direction: DirectionForward,
		Key: UserKey{TenantID: "tenant-a", AppName: "tenant-a/app", UserID: "u"}, SourceDigest: digest, State: StatePending, Version: 1}}}
	driver := Driver{Authority: authority, Ledger: ledger, SourceUser: images{values: []Image{value}}, TargetUser: images{}}
	result, err := driver.Repair(ctx, RepairRequest{TenantID: "tenant-a", MigrationID: "memory-repair", WorkerID: "worker-1", Limit: 1, Now: now.Add(3 * time.Second), Lease: time.Minute, RetryDelay: time.Second})
	if err != nil || result != (RepairResult{Claimed: 1, Applied: 1}) || ledger.items[0].State != StateApplied || ledger.items[0].TargetDigest != digest {
		t.Fatalf("repair=%+v item=%+v err=%v", result, ledger.items[0], err)
	}
}
