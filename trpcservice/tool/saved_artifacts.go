package tool

import (
	"context"
	"sync"
)

// SavedArtifact identifies one artifact version produced during the current
// invocation. It contains no bytes and is safe to carry into the durable reply
// projection after the framework Artifact service has accepted the write.
type SavedArtifact struct {
	Filename string
	Version  int
	MimeType string
	Name     string
}

type savedArtifactRecorder struct {
	mu    sync.Mutex
	items []SavedArtifact
}

func NewSavedArtifactRecorder() *savedArtifactRecorder { return &savedArtifactRecorder{} }

func (r *savedArtifactRecorder) Record(item SavedArtifact) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.items = append(r.items, item)
	r.mu.Unlock()
}

func (r *savedArtifactRecorder) Snapshot() []SavedArtifact {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]SavedArtifact(nil), r.items...)
}

type savedArtifactRecorderKey struct{}

func WithSavedArtifactRecorder(ctx context.Context, recorder *savedArtifactRecorder) context.Context {
	if recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, savedArtifactRecorderKey{}, recorder)
}

func recordSavedArtifact(ctx context.Context, item SavedArtifact) {
	if ctx == nil {
		return
	}
	if recorder, ok := ctx.Value(savedArtifactRecorderKey{}).(*savedArtifactRecorder); ok {
		recorder.Record(item)
	}
}
