package knowledgeplane

import (
	"context"
	"errors"
	"sync"

	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/protobuf/proto"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	qdrantstore "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
	qdrantstorage "trpc.group/trpc-go/trpc-agent-go/storage/qdrant"
)

// Synchronous acknowledgement is required before a migration captures the
// source value. An accepted but unapplied Qdrant operation is insufficient.
type synchronousQdrantClient struct{ qdrantstorage.Client }

func completedQdrantWrite(result *qdrant.UpdateResult, err error) (*qdrant.UpdateResult, error) {
	if err != nil {
		return nil, err
	}
	if result == nil || result.Status != qdrant.UpdateStatus_Completed {
		return nil, errors.New("qdrant write did not complete")
	}
	return result, nil
}
func (c synchronousQdrantClient) Upsert(ctx context.Context, request *qdrant.UpsertPoints) (*qdrant.UpdateResult, error) {
	copyRequest := proto.Clone(request).(*qdrant.UpsertPoints)
	copyRequest.Wait = qdrant.PtrOf(true)
	return completedQdrantWrite(c.Client.Upsert(ctx, copyRequest))
}
func (c synchronousQdrantClient) Delete(ctx context.Context, request *qdrant.DeletePoints) (*qdrant.UpdateResult, error) {
	copyRequest := proto.Clone(request).(*qdrant.DeletePoints)
	copyRequest.Wait = qdrant.PtrOf(true)
	return completedQdrantWrite(c.Client.Delete(ctx, copyRequest))
}
func (c synchronousQdrantClient) SetPayload(ctx context.Context, request *qdrant.SetPayloadPoints) (*qdrant.UpdateResult, error) {
	copyRequest := proto.Clone(request).(*qdrant.SetPayloadPoints)
	copyRequest.Wait = qdrant.PtrOf(true)
	return completedQdrantWrite(c.Client.SetPayload(ctx, copyRequest))
}

type ownedQdrantStore struct {
	vectorstore.VectorStore
	client   qdrantstorage.Client
	once     sync.Once
	closeErr error
}

func (s *ownedQdrantStore) Close() error {
	s.once.Do(func() { s.closeErr = errors.Join(s.VectorStore.Close(), s.client.Close()) })
	return s.closeErr
}

func NewSynchronousQdrantScopedStore(ctx context.Context, tenantID, appName string, config QdrantConfig) (*ScopedStore, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	client, err := qdrant.NewClient(&qdrant.Config{Host: config.Host, Port: config.Port, APIKey: config.APIKey, UseTLS: config.TLS})
	if err != nil {
		return nil, err
	}
	backend, err := qdrantstore.New(ctx, qdrantstore.WithCollectionName(config.Collection), qdrantstore.WithDimension(config.Dimension), qdrantstore.WithClient(synchronousQdrantClient{Client: client}))
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	owned := &ownedQdrantStore{VectorStore: backend, client: client}
	store, err := NewScopedStore(tenantID, appName, owned)
	if err != nil {
		_ = owned.Close()
		return nil, err
	}
	return store, nil
}
