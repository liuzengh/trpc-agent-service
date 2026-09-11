package knowledgeingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
)

const (
	defaultLease       = 10 * time.Minute
	defaultMaxAttempts = 3
	idlePollInterval   = 500 * time.Millisecond
)

type Pipeline interface {
	LoadKnowledgeSource(context.Context, storage.KnowledgeLoadRequest, source.Source) (int, error)
}

type Worker struct {
	queue       storage.KnowledgeIngestQueue
	snapshots   storage.KnowledgeSourceStore
	pipeline    Pipeline
	sources     *SourceFactory
	owner       string
	lease       time.Duration
	maxAttempts int
}

func NewWorker(queue storage.KnowledgeIngestQueue, pipeline Pipeline, sources *SourceFactory) (*Worker, error) {
	if queue == nil || pipeline == nil || sources == nil {
		return nil, errors.New("knowledge ingest queue, framework pipeline, and source factory are required")
	}
	snapshots, ok := queue.(storage.KnowledgeSourceStore)
	if !ok {
		return nil, errors.New("knowledge ingest queue must persist canonical document snapshots")
	}
	return &Worker{
		queue: queue, snapshots: snapshots, pipeline: pipeline, sources: sources, owner: uuid.NewString(),
		lease: defaultLease, maxAttempts: defaultMaxAttempts,
	}, nil
}

func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		processed, err := w.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("knowledge ingest failed", "error", err)
		}
		if processed {
			continue
		}
		timer := time.NewTimer(idlePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	job, found, err := w.queue.ClaimKnowledgeIngest(ctx, w.owner, w.lease)
	if err != nil || !found {
		return false, err
	}
	src, cleanup, err := w.sources.Build(ctx, job)
	if err != nil {
		return true, w.fail(ctx, job, err)
	}
	defer cleanup()
	documents, err := src.ReadDocuments(ctx)
	if err != nil {
		return true, w.fail(ctx, job, fmt.Errorf("read native framework knowledge source: %w", err))
	}
	if len(documents) == 0 {
		return true, w.fail(ctx, job, permanent(errors.New("source produced no indexable framework documents")))
	}
	encoded, err := json.Marshal(documents)
	if err != nil {
		return true, w.fail(ctx, job, fmt.Errorf("encode canonical framework documents: %w", err))
	}
	if err := w.snapshots.SaveKnowledgeDocumentSnapshot(ctx, job.TenantID, job.AppCode, job.DocumentID, encoded); err != nil {
		return true, w.fail(ctx, job, fmt.Errorf("persist canonical framework documents: %w", err))
	}
	src = &snapshotSource{name: src.Name(), sourceType: src.Type(), metadata: src.GetMetadata(), documents: documents}
	totalChunks, err := w.pipeline.LoadKnowledgeSource(ctx, storage.KnowledgeLoadRequest{
		TenantID: job.TenantID, AppCode: job.AppCode, DocumentID: job.DocumentID, JobID: job.ID, Owner: w.owner,
		ProfileID: job.ProfileID,
		Backend:   job.Backend,
	}, src)
	if err != nil {
		return true, w.fail(ctx, job, fmt.Errorf("load native framework knowledge source: %w", err))
	}
	if totalChunks == 0 {
		return true, w.fail(ctx, job, permanent(errors.New("source produced no indexable framework documents")))
	}
	if err := w.queue.CompleteKnowledgeIngest(ctx, storage.KnowledgeIngestCompletion{
		JobID: job.ID, Owner: w.owner, TenantID: job.TenantID, AppCode: job.AppCode,
		DocumentID: job.DocumentID, TotalChunks: totalChunks,
	}); err != nil {
		return true, w.fail(ctx, job, fmt.Errorf("complete knowledge ingest: %w", err))
	}
	return true, nil
}

type snapshotSource struct {
	name       string
	sourceType string
	metadata   map[string]any
	documents  []*document.Document
}

func (s *snapshotSource) ReadDocuments(context.Context) ([]*document.Document, error) {
	result := make([]*document.Document, 0, len(s.documents))
	for _, current := range s.documents {
		if current != nil {
			result = append(result, current.Clone())
		}
	}
	return result, nil
}
func (s *snapshotSource) Name() string { return s.name }
func (s *snapshotSource) Type() string { return s.sourceType }
func (s *snapshotSource) GetMetadata() map[string]any {
	result := make(map[string]any, len(s.metadata))
	for key, value := range s.metadata {
		result[key] = value
	}
	return result
}

var _ source.Source = (*snapshotSource)(nil)

func (w *Worker) fail(ctx context.Context, job storage.KnowledgeIngestJob, cause error) error {
	maxAttempts := w.maxAttempts
	if isPermanent(cause) {
		maxAttempts = job.Attempts
	}
	terminal, err := w.queue.FailKnowledgeIngest(ctx, job.ID, w.owner, cause.Error(), maxAttempts)
	if err != nil {
		return errors.Join(cause, err)
	}
	if terminal {
		return fmt.Errorf("knowledge document %s/%s failed permanently: %w", job.AppCode, job.DocumentID, cause)
	}
	return fmt.Errorf("knowledge document %s/%s will retry: %w", job.AppCode, job.DocumentID, cause)
}
