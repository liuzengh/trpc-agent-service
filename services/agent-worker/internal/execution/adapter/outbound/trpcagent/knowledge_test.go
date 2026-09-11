package trpcagent

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/knowledgestore"
	"testing"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
)

type knowledgeFailure struct{ err error }

func (k knowledgeFailure) Search(context.Context, *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	return nil, k.err
}
func TestKnowledgeNoMatchesDistinctFromBackendFailure(t *testing.T) {
	k := &tracedKnowledge{service: knowledgeFailure{knowledgestore.ErrNoResults}}
	r, err := k.Search(context.Background(), &knowledge.SearchRequest{Query: "empty"})
	if err != nil || r == nil || len(r.Documents) != 0 || k.failed.Load() {
		t.Fatal("empty result became failed execution")
	}
	k = &tracedKnowledge{service: knowledgeFailure{knowledgestore.ErrUnavailable}}
	_, err = k.Search(context.Background(), &knowledge.SearchRequest{Query: "failed"})
	if !errors.Is(err, ErrKnowledge) || !k.failed.Load() {
		t.Fatal("backend failure not recorded")
	}
}
