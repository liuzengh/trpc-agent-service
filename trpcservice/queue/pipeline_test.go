package queue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/store"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
	"github.com/google/uuid"
)

type pipelineAtomicStore struct {
	*atomicMemoryStore
	identity string
}

func (s *pipelineAtomicStore) DatabaseIdentity() string { return s.identity }

func insertRawLegacyTask(t *testing.T, st store.Store, task worker.Task) store.InboxRecord {
	t.Helper()
	task.Message.TenantID = task.Tenant.TenantID
	task.Message.BindingID = task.Binding.BindingID
	task.Message.Channel = task.Binding.Type
	task.Deliver = false
	task.Pipeline = worker.DurablePipelineMetadata{}
	currentPayload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	var legacyEnvelope map[string]json.RawMessage
	if err := json.Unmarshal(currentPayload, &legacyEnvelope); err != nil {
		t.Fatal(err)
	}
	delete(legacyEnvelope, "pipeline")
	payload, err := json.Marshal(legacyEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	rec := store.InboxRecord{
		InboxID: uuid.NewString(), TenantID: task.Tenant.TenantID,
		ChannelType: task.Binding.Type, BindingID: task.Binding.BindingID,
		ExternalMessageID: task.Message.ExternalMessageID,
		DedupKey:          worker.MessageDedupKey(task.Message),
		PartitionKey:      domain.SessionPartitionKey(task.Message, task.Tenant.App.Name),
		Payload:           payload,
	}
	if err := st.InsertInbox(context.Background(), &rec, time.Now()); err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestStampDurablePipelineRequiresCompleteAtomicCapability(t *testing.T) {
	atomicStore := &pipelineAtomicStore{
		atomicMemoryStore: &atomicMemoryStore{Memory: store.NewMemory()},
		identity:          "postgres-binding-v1:test",
	}
	d := NewDurable(atomicStore, &fakeProcessor{}, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	task := testTask()
	task.Tenant.Data.Session.Type = "sql"
	if err := d.stampDurablePipeline(&task); err != nil {
		t.Fatal(err)
	}
	if task.Pipeline.SchemaVersion != worker.DurablePipelineVersion ||
		task.Pipeline.AtomicCommitMode != worker.AtomicCommitRequired ||
		task.Pipeline.DatabaseIdentity != atomicStore.identity {
		t.Fatalf("stamped metadata = %+v", task.Pipeline)
	}

	incomplete := NewDurable(
		&atomicMemoryStore{Memory: store.NewMemory()},
		&fakeProcessor{}, fakeResolver{}, &fakeDeliverer{}, nil, testOptions(),
	)
	if err := incomplete.stampDurablePipeline(&task); !errors.Is(err, worker.ErrAtomicCommitUnavailable) {
		t.Fatalf("incomplete capability = %v, want ErrAtomicCommitUnavailable", err)
	}
}

func TestPrepareAtomicParticipantKeepsLegacyAndRejectsIdentityMismatch(t *testing.T) {
	atomicStore := &pipelineAtomicStore{
		atomicMemoryStore: &atomicMemoryStore{Memory: store.NewMemory()},
		identity:          "postgres-binding-v1:current",
	}
	d := NewDurable(atomicStore, &fakeProcessor{}, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	legacyTask := testTask()
	legacyRecord := &store.InboxRecord{}
	participant, err := d.prepareAtomicParticipant(legacyRecord, &legacyTask, "owner")
	if err != nil || participant != nil {
		t.Fatalf("legacy participant=%v err=%v", participant, err)
	}

	currentTask := testTask()
	currentTask.Tenant.Data.Session.Type = "sql"
	currentTask.Pipeline = worker.DurablePipelineMetadata{
		SchemaVersion: worker.DurablePipelineVersion, AtomicCommitMode: worker.AtomicCommitRequired,
		DatabaseIdentity: "postgres-binding-v1:previous",
	}
	currentRecord := &store.InboxRecord{}
	mirrorPipelineOnInbox(currentRecord, currentTask)
	if _, err := d.prepareAtomicParticipant(currentRecord, &currentTask, "owner"); !errors.Is(err, worker.ErrAtomicDatabaseMismatch) {
		t.Fatalf("identity mismatch = %v, want ErrAtomicDatabaseMismatch", err)
	}
}

func TestPrepareAtomicParticipantRejectsPayloadColumnDrift(t *testing.T) {
	d := NewDurable(store.NewMemory(), &fakeProcessor{}, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	task := testTask()
	task.Pipeline = worker.DurablePipelineMetadata{
		SchemaVersion: worker.DurablePipelineVersion, AtomicCommitMode: worker.AtomicCommitDisabled,
	}
	rec := &store.InboxRecord{PipelineSchemaVersion: worker.DurablePipelineVersion + 1, AtomicCommitMode: string(worker.AtomicCommitDisabled)}
	if _, err := d.prepareAtomicParticipant(rec, &task, "owner"); !errors.Is(err, worker.ErrUnsupportedPipeline) {
		t.Fatalf("metadata drift = %v, want ErrUnsupportedPipeline", err)
	}
}

func TestCurrentPipelineMetadataDriftDeadLettersBeforeProcessor(t *testing.T) {
	st := store.NewMemory()
	processor := &fakeProcessor{}
	d := NewDurable(st, processor, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	task := testTask()
	task.Message.TenantID = task.Tenant.TenantID
	task.Message.BindingID = task.Binding.BindingID
	task.Message.Channel = task.Binding.Type
	task.Pipeline = worker.DurablePipelineMetadata{
		SchemaVersion:    worker.DurablePipelineVersion,
		AtomicCommitMode: worker.AtomicCommitDisabled,
	}
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	rec := &store.InboxRecord{
		InboxID: uuid.NewString(), TenantID: task.Tenant.TenantID,
		ChannelType: task.Binding.Type, BindingID: task.Binding.BindingID,
		ExternalMessageID: task.Message.ExternalMessageID,
		DedupKey:          worker.MessageDedupKey(task.Message),
		PartitionKey:      domain.SessionPartitionKey(task.Message, task.Tenant.App.Name),
		Payload:           payload,
		// Both sides are individually valid v2 metadata, but they disagree.
		PipelineSchemaVersion: worker.DurablePipelineVersion,
		AtomicCommitMode:      string(worker.AtomicCommitRequired),
		DatabaseIdentity:      "postgres-binding-v1:tampered",
	}
	if err := st.InsertInbox(context.Background(), rec, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !d.relayOnce(context.Background()) {
		t.Fatal("drifted Inbox was not leased")
	}
	processor.mu.Lock()
	runs := processor.runs
	processor.mu.Unlock()
	if runs != 0 {
		t.Fatalf("processor ran for drifted metadata: %d", runs)
	}
	runnable, dead, pending, _, _, err := st.Depths(context.Background())
	if err != nil || runnable != 0 || dead != 1 || pending != 0 {
		t.Fatalf("drifted metadata depths = %d/%d/%d, err=%v", runnable, dead, pending, err)
	}
}

func TestLegacySQLBacklogDrainsWithoutAtomicParticipant(t *testing.T) {
	base := store.NewMemory()
	atomicStore := &pipelineAtomicStore{
		atomicMemoryStore: &atomicMemoryStore{Memory: base},
		identity:          "postgres-binding-v1:upgraded",
	}
	processor := &fakeProcessor{}
	d := NewDurable(atomicStore, processor, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	task := testTask()
	task.Tenant.Data.Session = config.BackendConfig{Type: "sql"}
	// Simulate the exact pre-upgrade JSON and Inbox columns: Pipeline is absent
	// from the payload and the newly added SQL columns retain zero/empty values.
	insertRawLegacyTask(t, atomicStore, task)
	if !d.relayOnce(context.Background()) {
		t.Fatal("legacy Inbox was not leased")
	}
	if tx, legacy := atomicStore.txCompletes.Load(), atomicStore.legacyCompletes.Load(); tx != 0 || legacy != 1 {
		t.Fatalf("legacy drain transaction/legacy completions = %d/%d, want 0/1", tx, legacy)
	}
	processor.mu.Lock()
	defer processor.mu.Unlock()
	if len(processor.tasks) != 1 || processor.tasks[0].TurnCommitParticipant != nil || !processor.tasks[0].Pipeline.IsLegacy() {
		t.Fatalf("legacy task was retroactively upgraded: %+v", processor.tasks)
	}
}

func TestRequiredPipelineMismatchParksPastAttemptBudgetUntilCompatibleWorker(t *testing.T) {
	base := store.NewMemory()
	atomicStore := &atomicMemoryStore{Memory: base}
	requiredIdentity := "postgres-binding-v1:required"
	incompatible := &pipelineAtomicStore{
		atomicMemoryStore: atomicStore,
		identity:          "postgres-binding-v1:other",
	}
	incompatibleProcessor := &fakeProcessor{}
	opts := testOptions()
	opts.InboxMaxAttempts = 1
	d := NewDurable(incompatible, incompatibleProcessor, fakeResolver{}, &fakeDeliverer{}, nil, opts)
	task := testTask()
	task.Tenant.Data.Session = config.BackendConfig{Type: "sql"}
	task.Pipeline = worker.DurablePipelineMetadata{
		SchemaVersion: worker.DurablePipelineVersion, AtomicCommitMode: worker.AtomicCommitRequired,
		DatabaseIdentity: requiredIdentity,
	}
	task.Message.TenantID = task.Tenant.TenantID
	task.Message.BindingID = task.Binding.BindingID
	task.Message.Channel = task.Binding.Type
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	rec := &store.InboxRecord{
		InboxID: uuid.NewString(), TenantID: task.Tenant.TenantID,
		ChannelType: task.Binding.Type, BindingID: task.Binding.BindingID,
		ExternalMessageID: task.Message.ExternalMessageID,
		DedupKey:          worker.MessageDedupKey(task.Message),
		PartitionKey:      domain.SessionPartitionKey(task.Message, task.Tenant.App.Name),
		Payload:           payload,
	}
	mirrorPipelineOnInbox(rec, task)
	if err := base.InsertInbox(context.Background(), rec, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Repeatedly landing on the wrong replica must not exhaust the ordinary
	// attempt budget or poison the durable record.
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(2 * opts.RetryMax)
		}
		if !d.relayOnce(context.Background()) {
			t.Fatalf("required Inbox was not leased on incompatible attempt %d", attempt+1)
		}
	}
	incompatibleProcessor.mu.Lock()
	runs := incompatibleProcessor.runs
	incompatibleProcessor.mu.Unlock()
	if runs != 0 {
		t.Fatalf("processor ran without required capability: %d", runs)
	}
	runnable, dead, pending, _, _, err := base.Depths(context.Background())
	if err != nil || runnable != 1 || dead != 0 || pending != 0 {
		t.Fatalf("parked required Inbox depths = %d/%d/%d, err=%v", runnable, dead, pending, err)
	}

	time.Sleep(2 * opts.RetryMax)
	compatibleProcessor := &atomicFinalizingProcessor{}
	compatible := &pipelineAtomicStore{
		atomicMemoryStore: atomicStore,
		identity:          requiredIdentity,
	}
	compatibleDurable := NewDurable(compatible, compatibleProcessor, fakeResolver{}, &fakeDeliverer{}, nil, opts)
	if !compatibleDurable.relayOnce(context.Background()) {
		t.Fatal("compatible worker did not take over parked Inbox")
	}
	if got := compatibleProcessor.calls.Load(); got != 1 {
		t.Fatalf("compatible processor calls = %d, want 1", got)
	}
	if got := atomicStore.txCompletes.Load(); got != 1 {
		t.Fatalf("atomic completions = %d, want 1", got)
	}
	runnable, dead, pending, _, _, err = base.Depths(context.Background())
	if err != nil || runnable != 0 || dead != 0 || pending != 2 {
		t.Fatalf("compatible handoff depths = %d/%d/%d, err=%v", runnable, dead, pending, err)
	}
}

func TestFuturePipelineVersionParksWithoutRunningProcessor(t *testing.T) {
	st := store.NewMemory()
	processor := &fakeProcessor{}
	opts := testOptions()
	opts.InboxMaxAttempts = 1
	d := NewDurable(st, processor, fakeResolver{}, &fakeDeliverer{}, nil, opts)
	task := testTask()
	task.Pipeline = worker.DurablePipelineMetadata{
		SchemaVersion:    worker.DurablePipelineVersion + 1,
		AtomicCommitMode: worker.AtomicCommitDisabled,
	}
	task.Message.TenantID = task.Tenant.TenantID
	task.Message.BindingID = task.Binding.BindingID
	task.Message.Channel = task.Binding.Type
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	rec := &store.InboxRecord{
		InboxID: uuid.NewString(), TenantID: task.Tenant.TenantID,
		ChannelType: task.Binding.Type, BindingID: task.Binding.BindingID,
		ExternalMessageID: task.Message.ExternalMessageID,
		DedupKey:          worker.MessageDedupKey(task.Message),
		PartitionKey:      domain.SessionPartitionKey(task.Message, task.Tenant.App.Name),
		Payload:           payload,
	}
	mirrorPipelineOnInbox(rec, task)
	if err := st.InsertInbox(context.Background(), rec, time.Now()); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			time.Sleep(2 * opts.RetryMax)
		}
		if !d.relayOnce(context.Background()) {
			t.Fatalf("future Inbox was not leased on attempt %d", attempt+1)
		}
	}
	processor.mu.Lock()
	runs := processor.runs
	processor.mu.Unlock()
	if runs != 0 {
		t.Fatalf("future-version processor runs = %d, want 0", runs)
	}
	runnable, dead, _, _, _, err := st.Depths(context.Background())
	if err != nil || runnable != 1 || dead != 0 {
		t.Fatalf("future-version depths runnable/dead = %d/%d, err=%v", runnable, dead, err)
	}
}
