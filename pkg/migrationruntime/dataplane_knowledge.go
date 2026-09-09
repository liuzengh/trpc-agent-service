package migrationruntime

import (
	"context"
	"sort"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

const maxDataPlaneMutationKeys = 10000

type liveKnowledgeStore struct {
	runtime                    *DataPlaneRuntime
	tenantID, appName, profile string
	cache                      *dataPlaneCache[vectorstore.VectorStore]
}

func (s *liveKnowledgeStore) raw(ctx context.Context, profile string, fn func(vectorstore.VectorStore) error) error {
	store, release, err := s.cache.borrow(ctx, profile)
	if err != nil {
		return err
	}
	defer release()
	return fn(store)
}

func (s *liveKnowledgeStore) read(ctx context.Context, fn func(context.Context, vectorstore.VectorStore) error) error {
	return s.runtime.coordinator.WithRead(ctx, s.tenantID, datamigration.DomainKnowledge, s.profile, func(ctx context.Context, profile string) error {
		return s.raw(ctx, profile, func(store vectorstore.VectorStore) error { return fn(ctx, store) })
	})
}

func (s *liveKnowledgeStore) write(ctx context.Context, id string, fn func(context.Context, vectorstore.VectorStore) error) error {
	key, err := knowledgeKey(s.appName, id)
	if err != nil {
		return err
	}
	return s.runtime.coordinator.WithWrite(ctx, s.tenantID, datamigration.DomainKnowledge, s.profile, []string{key}, func(ctx context.Context, profile string) error {
		return s.raw(ctx, profile, func(store vectorstore.VectorStore) error { return fn(ctx, store) })
	})
}

func (s *liveKnowledgeStore) Add(ctx context.Context, doc *document.Document, embedding []float64) error {
	if doc == nil {
		return knowledgeplane.ErrScopeViolation
	}
	copyDoc := doc.Clone()
	copyEmbedding := append([]float64(nil), embedding...)
	return s.write(ctx, copyDoc.ID, func(ctx context.Context, store vectorstore.VectorStore) error {
		return store.Add(ctx, copyDoc, copyEmbedding)
	})
}
func (s *liveKnowledgeStore) Update(ctx context.Context, doc *document.Document, embedding []float64) error {
	if doc == nil {
		return knowledgeplane.ErrScopeViolation
	}
	copyDoc := doc.Clone()
	copyEmbedding := append([]float64(nil), embedding...)
	return s.write(ctx, copyDoc.ID, func(ctx context.Context, store vectorstore.VectorStore) error {
		return store.Update(ctx, copyDoc, copyEmbedding)
	})
}
func (s *liveKnowledgeStore) Delete(ctx context.Context, id string) error {
	return s.write(ctx, id, func(ctx context.Context, store vectorstore.VectorStore) error { return store.Delete(ctx, id) })
}
func (s *liveKnowledgeStore) Get(ctx context.Context, id string) (doc *document.Document, embedding []float64, err error) {
	err = s.read(ctx, func(ctx context.Context, store vectorstore.VectorStore) error {
		var err error
		doc, embedding, err = store.Get(ctx, id)
		return err
	})
	return
}
func (s *liveKnowledgeStore) Search(ctx context.Context, query *vectorstore.SearchQuery) (result *vectorstore.SearchResult, err error) {
	err = s.read(ctx, func(ctx context.Context, store vectorstore.VectorStore) error {
		var err error
		result, err = store.Search(ctx, query)
		return err
	})
	return
}
func (s *liveKnowledgeStore) Count(ctx context.Context, options ...vectorstore.CountOption) (count int, err error) {
	err = s.read(ctx, func(ctx context.Context, store vectorstore.VectorStore) error {
		var err error
		count, err = store.Count(ctx, options...)
		return err
	})
	return
}
func (s *liveKnowledgeStore) GetMetadata(ctx context.Context, options ...vectorstore.GetMetadataOption) (result map[string]vectorstore.DocumentMetadata, err error) {
	err = s.read(ctx, func(ctx context.Context, store vectorstore.VectorStore) error {
		var err error
		result, err = store.GetMetadata(ctx, options...)
		return err
	})
	return
}

func (s *liveKnowledgeStore) DeleteByFilter(ctx context.Context, options ...vectorstore.DeleteOption) error {
	config := vectorstore.ApplyDeleteOptions(options...)
	ids := append([]string(nil), config.DocumentIDs...)
	if len(ids) == 0 && len(config.Filter) == 0 && !config.DeleteAll {
		return knowledgeplane.ErrScopeViolation
	}
	keysFn := func(ctx context.Context, profile string) ([]string, error) {
		selected := append([]string(nil), ids...)
		if len(selected) == 0 {
			err := s.raw(ctx, profile, func(store vectorstore.VectorStore) error {
				count, err := store.Count(ctx, vectorstore.WithCountFilter(config.Filter))
				if err != nil {
					return err
				}
				if count > maxDataPlaneMutationKeys {
					return datamigration.ErrMigrationCapability
				}
				metadata, err := store.GetMetadata(ctx, vectorstore.WithGetMetadataFilter(config.Filter))
				if err != nil {
					return err
				}
				for id := range metadata {
					selected = append(selected, id)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
		return s.keys(selected)
	}
	return s.runtime.coordinator.WithWriteKeys(ctx, s.tenantID, datamigration.DomainKnowledge, s.profile, keysFn, func(ctx context.Context, profile string) error {
		return s.raw(ctx, profile, func(store vectorstore.VectorStore) error {
			return store.DeleteByFilter(ctx,
				vectorstore.WithDeleteDocumentIDs(ids), vectorstore.WithDeleteFilter(config.Filter), vectorstore.WithDeleteAll(config.DeleteAll))
		})
	})
}

func (s *liveKnowledgeStore) UpdateByFilter(ctx context.Context, options ...vectorstore.UpdateByFilterOption) (updated int64, err error) {
	config, err := vectorstore.ApplyUpdateByFilterOptions(options...)
	if err != nil {
		return 0, err
	}
	if len(config.DocumentIDs) == 0 {
		return 0, knowledgeplane.ErrScopeViolation
	}
	for key := range config.Updates {
		if strings.HasPrefix(key, "metadata._tsa_") {
			return 0, knowledgeplane.ErrScopeViolation
		}
	}
	ids := append([]string(nil), config.DocumentIDs...)
	keys, err := s.keys(ids)
	if err != nil {
		return 0, err
	}
	err = s.runtime.coordinator.WithWrite(ctx, s.tenantID, datamigration.DomainKnowledge, s.profile, keys, func(ctx context.Context, profile string) error {
		return s.raw(ctx, profile, func(store vectorstore.VectorStore) error {
			var err error
			updated, err = store.UpdateByFilter(ctx, vectorstore.WithUpdateByFilterDocumentIDs(ids),
				vectorstore.WithUpdateByFilterCondition(config.FilterCondition), vectorstore.WithUpdateByFilterUpdates(config.Updates))
			return err
		})
	})
	return updated, err
}

func (s *liveKnowledgeStore) keys(ids []string) ([]string, error) {
	if len(ids) > maxDataPlaneMutationKeys {
		return nil, datamigration.ErrMigrationCapability
	}
	keys := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		key, err := knowledgeKey(s.appName, id)
		if err != nil {
			return nil, err
		}
		if !seen[key] {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *liveKnowledgeStore) Close() error { s.runtime.unregister(s); return s.cache.Close() }

var _ vectorstore.VectorStore = (*liveKnowledgeStore)(nil)
