package knowledgedriver_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationmemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	ledgermemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestRepairBackfillAndSemanticVerify(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	authority, current := authorityAtBackfill(t, ctx, clock)
	ledger := ledgermemory.New()
	image := fixtureImage()
	digest, err := knowledgedriver.ImageDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ledger.Record(ctx, knowledgedriver.RecordRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		MutationID: "ingest-1", Epoch: current.Epoch, ConfigVersion: current.Source.ConfigVersion, Key: image.Key, Operation: image.Operation,
		SourceRevision: image.Revision, MutationDigest: digest, CreatedAt: clock.Add(4 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	target := newFakeTarget()
	target.fail = true
	driver := knowledgedriver.Driver{Authority: authority, Ledger: ledger, Source: fakeReader{image}, Backfill: fakePage{image},
		Target: target, SourceInventory: fakeInventory{fingerprint()}, TargetInventory: fakeInventory{fingerprint()},
		ProbeSource: fakeProbes{[]knowledgedriver.Probe{fixtureProbe()}}, SearchTarget: fakeSearch{[]knowledgedriver.ChunkKey{image.Key}}}
	first, err := driver.Repair(ctx, knowledgedriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		WorkerID: "repair-1", Limit: 10, Now: clock.Add(5 * time.Minute), Lease: time.Minute, RetryDelay: 30 * time.Second})
	if err != nil || first.Retried != 1 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	pending, err := ledger.Claim(ctx, knowledgedriver.ClaimRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		WorkerID: "diagnostic-read", Limit: 1, Now: clock.Add(5*time.Minute + 30*time.Second), Lease: time.Second})
	if err != nil || len(pending) != 1 || pending[0].LastErrorClass != "target_unavailable" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	target.fail = false
	second, err := driver.Repair(ctx, knowledgedriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		WorkerID: "repair-2", Limit: 10, Now: clock.Add(6 * time.Minute), Lease: time.Minute})
	if err != nil || second.Applied != 1 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	batch, err := driver.BackfillOnce(ctx, knowledgedriver.BackfillRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, Limit: 10, At: clock.Add(7 * time.Minute)})
	if err != nil || !batch.Migration.BackfillComplete || batch.Migration.BackfillCount != 1 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	verified, err := driver.EnterVerify(ctx, current.TenantID, current.MigrationID, clock.Add(8*time.Minute))
	if err != nil || verified.State != migration.StateVerify {
		t.Fatalf("state=%+v err=%v", verified, err)
	}
	evidence, err := driver.ShadowVerify(ctx, current.TenantID, current.MigrationID)
	if err != nil || evidence.SourceCount != 1 || len(evidence.SampleDigest) != 64 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
}

func TestBackfillFailureCannotAdvanceCheckpoint(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	authority, current := authorityAtBackfill(t, ctx, clock)
	target := newFakeTarget()
	target.fail = true
	driver := knowledgedriver.Driver{Authority: authority, Backfill: fakePage{fixtureImage()}, Target: target}
	_, err := driver.BackfillOnce(ctx, knowledgedriver.BackfillRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, Limit: 10, At: clock.Add(5 * time.Minute)})
	if !errors.Is(err, runtime.ErrBackendUnavailable) {
		t.Fatalf("failure=%v", err)
	}
	unchanged, _ := authority.Get(ctx, current.TenantID, current.MigrationID)
	if unchanged.Version != current.Version || unchanged.BackfillCheckpoint != "" {
		t.Fatalf("advanced=%+v", unchanged)
	}
}

func TestRepairReverseMutationIntoSource(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	authority, current := authorityAtBackfill(t, ctx, clock)
	ledger := ledgermemory.New()
	image := fixtureImage()
	digest, err := knowledgedriver.ImageDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Record(ctx, knowledgedriver.RecordRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		MutationID: "target-write", Epoch: current.Epoch, ConfigVersion: current.Target.ConfigVersion,
		Direction: knowledgedriver.DirectionReverse, Key: image.Key, Operation: image.Operation,
		SourceRevision: image.Revision, MutationDigest: digest, CreatedAt: clock.Add(4 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	sourceReplica := newFakeTarget()
	driver := knowledgedriver.Driver{Authority: authority, Ledger: ledger, Source: fakeReader{image}, Target: newFakeTarget(),
		ReverseSource: fakeReader{image}, ReverseReplica: sourceReplica}
	result, err := driver.Repair(ctx, knowledgedriver.RepairRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		WorkerID: "reverse-repair", Limit: 1, Now: clock.Add(5 * time.Minute), Lease: time.Minute})
	if err != nil || result.Applied != 1 || sourceReplica.applied["target-write"].MutationID == "" {
		t.Fatalf("result=%+v applied=%+v err=%v", result, sourceReplica.applied, err)
	}
}

func TestSemanticVerificationRejectsScopeLeakAndLowRecall(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	for name, hits := range map[string][]knowledgedriver.ChunkKey{
		"tenant leak": {{TenantID: "tenant-b", KnowledgeID: "kb-a", KnowledgeVersion: 3, ChunkID: "chunk-a"}},
		"low recall":  {},
		"duplicate":   {fixtureImage().Key, fixtureImage().Key},
	} {
		t.Run(name, func(t *testing.T) {
			authority, current := authorityAtVerify(t, ctx, clock)
			driver := knowledgedriver.Driver{Authority: authority, Ledger: ledgermemory.New(), SourceInventory: fakeInventory{fingerprint()},
				TargetInventory: fakeInventory{fingerprint()}, ProbeSource: fakeProbes{[]knowledgedriver.Probe{fixtureProbe()}}, SearchTarget: fakeSearch{hits}}
			if _, err := driver.ShadowVerify(ctx, current.TenantID, current.MigrationID); err == nil {
				t.Fatal("invalid probe accepted")
			}
		})
	}
}

func TestKnowledgePhaseJournalRecoversAuthorityCommitCrashWindow(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	store, current := authorityAtBackfill(t, ctx, clock)
	batch, err := store.CommitBatch(ctx, migration.BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		BatchID: "journal-empty", Epoch: current.Epoch, ExpectedVersion: current.Version, BatchSeq: current.NextBatchSeq,
		ToCheckpoint: "knowledge-chunk-v1:eof", Digest: digest('e'), Complete: true, CommittedAt: clock.Add(4 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	request := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		ExpectedVersion: batch.Migration.Version, To: migration.StateVerify, At: clock.Add(5 * time.Minute)}
	if _, err := store.BeginPhaseIntent(ctx, request); err != nil {
		t.Fatal(err)
	}
	// Fault injection: the state write commits but process shutdown prevents
	// journal completion. The same service journal protects both domains.
	if _, err := store.Transition(ctx, request); err != nil {
		t.Fatal(err)
	}
	journaled := migration.NewJournaledRepository(store, store)
	recovered, err := journaled.RecoverPending(ctx, current.TenantID, current.MigrationID)
	if err != nil || len(recovered) != 1 || recovered[0].State != migration.StateVerify || recovered[0].Version != request.ExpectedVersion+1 {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
}

func fixtureImage() knowledgedriver.ChunkImage {
	return knowledgedriver.ChunkImage{Key: knowledgedriver.ChunkKey{TenantID: "tenant-a", KnowledgeID: "kb-a", KnowledgeVersion: 3, ChunkID: "chunk-a"},
		Revision: 2, Operation: knowledgedriver.OperationUpsert, SourceDigest: digest('a'), ContentDigest: hashText("bounded content"), MetadataDigest: hashValue(map[string]string{"source": "doc-a"}),
		EmbeddingProfileID: "embed-a", EmbeddingVersion: 4, VectorGeneration: "generation-b", Content: "bounded content",
		Metadata: map[string]string{"source": "doc-a"}, Vector: []float32{0.25, -0.75}}
}
func fixtureProbe() knowledgedriver.Probe {
	image := fixtureImage()
	return knowledgedriver.Probe{ProbeID: "probe-a", TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID, KnowledgeVersion: image.Key.KnowledgeVersion, Query: "bounded", Expected: []knowledgedriver.ChunkKey{image.Key}, MinRecallPPM: 1_000_000}
}
func fingerprint() knowledgedriver.Fingerprint {
	return knowledgedriver.Fingerprint{Count: 1, Digest: digest('d'), Watermark: "knowledge-chunk-v1:eof"}
}
func digest(value byte) string {
	result := make([]byte, 64)
	for index := range result {
		result[index] = value
	}
	return string(result)
}
func hashValue(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func authorityAtBackfill(t *testing.T, ctx context.Context, clock time.Time) (*migrationmemory.Store, migration.Migration) {
	t.Helper()
	store := migrationmemory.New()
	current, err := store.Create(ctx, migration.CreateRequest{TenantID: "tenant-a", MigrationID: "knowledge-migration", Domain: knowledgedriver.Domain, Epoch: 1,
		Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1}, Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, CreatedAt: clock})
	if err != nil {
		t.Fatal(err)
	}
	current, err = store.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateSnapshot, At: clock.Add(time.Minute), SnapshotWatermark: "knowledge-chunk-v1:snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	current, err = store.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateDualWrite, At: clock.Add(2 * time.Minute), DualWriteRef: "knowledge-mutation://knowledge-migration"})
	if err != nil {
		t.Fatal(err)
	}
	current, err = store.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateBackfill, At: clock.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return store, current
}
func authorityAtVerify(t *testing.T, ctx context.Context, clock time.Time) (*migrationmemory.Store, migration.Migration) {
	store, current := authorityAtBackfill(t, ctx, clock)
	result, err := store.CommitBatch(ctx, migration.BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, BatchID: "empty", Epoch: current.Epoch, ExpectedVersion: current.Version, BatchSeq: current.NextBatchSeq, ToCheckpoint: "eof", Digest: digest('e'), Complete: true, CommittedAt: clock.Add(4 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	current, err = store.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: result.Migration.Version, To: migration.StateVerify, At: clock.Add(5 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return store, current
}

type fakeReader struct{ image knowledgedriver.ChunkImage }

func (f fakeReader) LoadChunk(_ context.Context, key knowledgedriver.ChunkKey) (knowledgedriver.ChunkImage, error) {
	if key != f.image.Key {
		return knowledgedriver.ChunkImage{}, runtime.ErrNotFound
	}
	return f.image, nil
}

type fakePage struct{ image knowledgedriver.ChunkImage }

func (f fakePage) PageChunks(_ context.Context, _ knowledgedriver.PageRequest) (knowledgedriver.Page, error) {
	return knowledgedriver.Page{Chunks: []knowledgedriver.ChunkImage{f.image}, NextCheckpoint: "6b622d61:00000000000000000003:6368756e6b2d61", Complete: true}, nil
}

type fakeTarget struct {
	fail    bool
	applied map[string]knowledgedriver.ApplyRequest
}

func newFakeTarget() *fakeTarget {
	return &fakeTarget{applied: make(map[string]knowledgedriver.ApplyRequest)}
}
func (f *fakeTarget) ApplyChunk(_ context.Context, in knowledgedriver.ApplyRequest) (knowledgedriver.ApplyResult, error) {
	if f.fail {
		return knowledgedriver.ApplyResult{}, runtime.ErrBackendUnavailable
	}
	if old, ok := f.applied[in.MutationID]; ok && (old.Epoch != in.Epoch || old.Image.Key != in.Image.Key || old.ImageDigest != in.ImageDigest) {
		return knowledgedriver.ApplyResult{}, runtime.ErrIdempotencyCollision
	}
	f.applied[in.MutationID] = in
	return knowledgedriver.ApplyResult{Revision: in.Image.Revision, Digest: in.ImageDigest}, nil
}

type fakeInventory struct{ value knowledgedriver.Fingerprint }

func (f fakeInventory) Fingerprint(_ context.Context, _, _ string) (knowledgedriver.Fingerprint, error) {
	return f.value, nil
}

type fakeProbes struct{ value []knowledgedriver.Probe }

func (f fakeProbes) Probes(_ context.Context, _, _ string) ([]knowledgedriver.Probe, error) {
	return f.value, nil
}

type fakeSearch struct{ hits []knowledgedriver.ChunkKey }

func (f fakeSearch) Search(_ context.Context, _ knowledgedriver.SearchRequest) ([]knowledgedriver.ChunkKey, error) {
	return f.hits, nil
}
