package knowledgeingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
)

type fakeQueue struct {
	job        storage.KnowledgeIngestJob
	found      bool
	completion storage.KnowledgeIngestCompletion
	snapshot   []byte
	failed     bool
	terminal   bool
	onComplete func()
}

func (*fakeQueue) EnqueueKnowledgeIngest(context.Context, storage.KnowledgeIngestRequest) (string, error) {
	return "job", nil
}
func (*fakeQueue) CancelKnowledgeIngest(context.Context, string, string, string) error { return nil }
func (q *fakeQueue) ClaimKnowledgeIngest(context.Context, string, time.Duration) (storage.KnowledgeIngestJob, bool, error) {
	if !q.found {
		return storage.KnowledgeIngestJob{}, false, nil
	}
	q.found = false
	return q.job, true, nil
}
func (q *fakeQueue) CompleteKnowledgeIngest(_ context.Context, completion storage.KnowledgeIngestCompletion) error {
	q.completion = completion
	if q.onComplete != nil {
		q.onComplete()
	}
	return nil
}
func (q *fakeQueue) FailKnowledgeIngest(context.Context, string, string, string, int) (bool, error) {
	q.failed = true
	return q.terminal, nil
}
func (q *fakeQueue) ListKnowledgeDocumentSources(context.Context, string, string) ([]storage.KnowledgeDocumentSource, error) {
	return nil, nil
}
func (q *fakeQueue) SaveKnowledgeDocumentSnapshot(_ context.Context, _, _, _ string, snapshot []byte) error {
	q.snapshot = append([]byte(nil), snapshot...)
	return nil
}

type fakePipeline struct {
	request storage.KnowledgeLoadRequest
	loaded  bool
	count   int
	err     error
}

func (p *fakePipeline) LoadKnowledgeSource(ctx context.Context, request storage.KnowledgeLoadRequest, src source.Source) (int, error) {
	p.request = request
	p.loaded = src != nil
	return p.count, p.err
}

func TestWorkerDelegatesWholeIngestPipelineToFramework(t *testing.T) {
	queue := &fakeQueue{found: true, job: storage.KnowledgeIngestJob{
		ID: "job-1", TenantID: "tenant", AppCode: "bot", DocumentID: "guide", Name: "Guide",
		Filename: "guide.txt", Data: []byte("knowledge content"), Attempts: 1,
	}}
	pipeline := &fakePipeline{count: 3}
	sources, _ := NewSourceFactory(config.KnowledgeConfig{}, "", 0)
	worker, err := NewWorker(queue, pipeline, sources)
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || !processed || !pipeline.loaded || queue.failed {
		t.Fatalf("RunOnce() = processed:%v err:%v loaded:%v failed:%v", processed, err, pipeline.loaded, queue.failed)
	}
	if queue.completion.TotalChunks != 3 || pipeline.request.DocumentID != "guide" || pipeline.request.Owner == "" || len(queue.snapshot) == 0 {
		t.Fatalf("pipeline/completion = %#v / %#v", pipeline.request, queue.completion)
	}
}

func TestWorkerStopsRetryingPermanentSourcePolicyFailure(t *testing.T) {
	queue := &fakeQueue{found: true, terminal: true, job: storage.KnowledgeIngestJob{
		ID: "job-1", TenantID: "tenant", AppCode: "bot", DocumentID: "bad", Attempts: 1,
		Metadata: map[string]string{"source_type": "url", "source_url": "http://example.com"},
	}}
	pipeline := &fakePipeline{count: 1, err: errors.New("must not run")}
	sources, _ := NewSourceFactory(config.KnowledgeConfig{AllowedSourceHosts: []string{"example.com"}}, "", 0)
	worker, err := NewWorker(queue, pipeline, sources)
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if !processed || err == nil || !queue.failed || pipeline.loaded {
		t.Fatalf("RunOnce() = processed:%v err:%v failed:%v loaded:%v", processed, err, queue.failed, pipeline.loaded)
	}
}

func TestWorkerRunHonorsPreCancelledContext(t *testing.T) {
	t.Parallel()
	queue := &fakeQueue{}
	sources, _ := NewSourceFactory(config.KnowledgeConfig{}, "", 0)
	worker, err := NewWorker(queue, &fakePipeline{}, sources)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Worker.Run() did not stop after cancellation")
	}
}

func TestWorkerRunProcessesQueuedDocumentThenStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	queue := &fakeQueue{
		found: true,
		job: storage.KnowledgeIngestJob{
			ID: "job-run", TenantID: "tenant", AppCode: "support", DocumentID: "guide", Name: "Guide",
			Filename: "guide.txt", Data: []byte("knowledge content"), Attempts: 1,
		},
		onComplete: cancel,
	}
	pipeline := &fakePipeline{count: 2}
	sources, err := NewSourceFactory(config.KnowledgeConfig{}, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(queue, pipeline, sources)
	if err != nil {
		t.Fatal(err)
	}
	worker.Run(ctx)
	if queue.completion.JobID != "job-run" || queue.completion.TotalChunks != 2 || !pipeline.loaded || len(queue.snapshot) == 0 {
		t.Fatalf("worker run completion=%#v loaded=%v snapshot=%d", queue.completion, pipeline.loaded, len(queue.snapshot))
	}
}

func TestSnapshotSourceReturnsIndependentDocumentsAndMetadata(t *testing.T) {
	t.Parallel()
	source := &snapshotSource{
		name: "Guide", sourceType: "snapshot",
		metadata:  map[string]any{"category": "support"},
		documents: []*document.Document{{ID: "doc-1", Content: "original"}, nil},
	}
	if source.Name() != "Guide" || source.Type() != "snapshot" {
		t.Fatalf("source identity = %q/%q", source.Name(), source.Type())
	}
	metadata := source.GetMetadata()
	metadata["category"] = "mutated"
	if source.metadata["category"] != "support" {
		t.Fatal("GetMetadata() aliases source metadata")
	}
	documents, err := source.ReadDocuments(context.Background())
	if err != nil || len(documents) != 1 || documents[0].Content != "original" {
		t.Fatalf("ReadDocuments() = %#v, %v", documents, err)
	}
	documents[0].Content = "mutated"
	if source.documents[0].Content != "original" {
		t.Fatal("ReadDocuments() aliases canonical snapshot")
	}
}
