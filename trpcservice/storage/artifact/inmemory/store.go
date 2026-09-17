package inmemory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
)

type Store struct {
	mu      sync.Mutex
	records map[string]artifact.Record
}

func New() *Store { return &Store{records: make(map[string]artifact.Record)} }

func (s *Store) PutArtifact(ctx context.Context, in artifact.Record) (artifact.Record, error) {
	if err := ctx.Err(); err != nil {
		return artifact.Record{}, err
	}
	id, ref, err := artifact.StableIdentity(in.TenantID, in.RequestID, in.Ordinal, in.SourceDigest)
	contentSum := sha256.Sum256(in.Content)
	if err != nil || in.ArtifactID != id || in.ArtifactRef != ref || in.ContentDigest == "" ||
		in.ContentDigest != hex.EncodeToString(contentSum[:]) ||
		in.MediaType == "" || in.Kind == "" || len(in.Content) == 0 || in.MalwareScanVersion == "" || in.DLPVersion == "" {
		return artifact.Record{}, runtime.ErrInvalidEnvelope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.ArtifactID
	if old, ok := s.records[key]; ok {
		if old.RequestID != in.RequestID || old.Ordinal != in.Ordinal || old.SourceDigest != in.SourceDigest ||
			old.ContentDigest != in.ContentDigest || old.MediaType != in.MediaType || old.Kind != in.Kind ||
			old.MalwareScanVersion != in.MalwareScanVersion || old.DLPVersion != in.DLPVersion || !bytes.Equal(old.Content, in.Content) {
			return artifact.Record{}, runtime.ErrIdempotencyCollision
		}
		return clone(old), nil
	}
	in.Content = append([]byte(nil), in.Content...)
	if in.CreatedAt.IsZero() {
		in.CreatedAt = time.Now().UTC()
	}
	s.records[key] = in
	return clone(in), nil
}

func (s *Store) GetArtifact(ctx context.Context, tenantID, artifactID string) (artifact.Record, error) {
	if err := ctx.Err(); err != nil {
		return artifact.Record{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.records[tenantID+"\x00"+artifactID]
	if !ok {
		return artifact.Record{}, runtime.ErrNotFound
	}
	return clone(value), nil
}

func clone(value artifact.Record) artifact.Record {
	value.Content = append([]byte(nil), value.Content...)
	return value
}

var _ artifact.Store = (*Store)(nil)
