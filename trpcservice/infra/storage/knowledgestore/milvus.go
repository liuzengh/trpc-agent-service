// milvus.go provides the production VectorStoreFactory backed by a real
// Milvus deployment. It lives here (infra) rather than in domain/knowledge so
// the domain layer stays free of network clients; the in-memory factory stays
// in the domain as the zero-IO dev default.
package knowledgestore

import (
	"context"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	milvusvs "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/milvus"
)

// MilvusVectorStoreFactory builds one cached Milvus VectorStore per KB
// collection (auto-created on first use, idempotent on restart).
//
// The metric stays at the framework default (IP): the store also creates a
// sparse field for hybrid search whose index only supports IP, and OpenAI-style
// embeddings are unit-normalized so IP ordering equals cosine ordering.
//
// One instance per collection is cached so the ephemeral ingest pipeline and
// the long-lived search instance share the same gRPC client.
func MilvusVectorStoreFactory(address, username, password string) knowledge.VectorStoreFactory {
	var mu sync.Mutex
	stores := make(map[string]vectorstore.VectorStore)
	return func(ctx context.Context, kb *knowledge.KnowledgeBase) (vectorstore.VectorStore, error) {
		mu.Lock()
		defer mu.Unlock()
		if vs, ok := stores[kb.CollectionName]; ok {
			return vs, nil
		}
		vs, err := milvusvs.New(ctx,
			milvusvs.WithAddress(address),
			milvusvs.WithUsername(username),
			milvusvs.WithPassword(password),
			milvusvs.WithCollectionName(kb.CollectionName),
			milvusvs.WithDimension(kb.Dimension),
		)
		if err != nil {
			return nil, fmt.Errorf("knowledge: milvus %s: %w", kb.CollectionName, err)
		}
		stores[kb.CollectionName] = vs
		return vs, nil
	}
}
