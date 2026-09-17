package inmemory

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
)

type Store struct {
	mu      sync.Mutex
	objects map[string]objectstore.Object
}

func New() *Store { return &Store{objects: make(map[string]objectstore.Object)} }

func (s *Store) PutObject(ctx context.Context, in objectstore.Object) (objectstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}
	if err := objectstore.Validate(in); err != nil {
		return objectstore.Object{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.ObjectKey
	if stored, ok := s.objects[key]; ok {
		if stored.ContentDigest != in.ContentDigest || !bytes.Equal(stored.Content, in.Content) {
			return objectstore.Object{}, runtime.ErrIdempotencyCollision
		}
		return clone(stored), nil
	}
	in.Content = append([]byte(nil), in.Content...)
	if in.CreatedAt.IsZero() {
		in.CreatedAt = time.Now().UTC()
	}
	s.objects[key] = in
	return clone(in), nil
}

func (s *Store) GetObject(ctx context.Context, tenantID, objectKey string) (objectstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}
	if err := objectstore.ValidateKey(tenantID, objectKey); err != nil {
		return objectstore.Object{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.objects[tenantID+"\x00"+objectKey]
	if !ok {
		return objectstore.Object{}, runtime.ErrNotFound
	}
	return clone(stored), nil
}

func (s *Store) DeleteObject(ctx context.Context, tenantID, objectKey, contentDigest string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := objectstore.ValidateKey(tenantID, objectKey); err != nil {
		return err
	}
	if contentDigest == "" {
		return runtime.ErrInvalidEnvelope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenantID + "\x00" + objectKey
	stored, ok := s.objects[key]
	if !ok {
		return nil
	}
	if stored.ContentDigest != contentDigest {
		return runtime.ErrVersionMismatch
	}
	delete(s.objects, key)
	return nil
}

func clone(value objectstore.Object) objectstore.Object {
	value.Content = append([]byte(nil), value.Content...)
	return value
}

var _ objectstore.Store = (*Store)(nil)
