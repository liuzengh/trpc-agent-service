package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
	upstreamknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

type knowledgeResolver struct{ value upstreamknowledge.Knowledge }

func (r knowledgeResolver) ResolveKnowledge(context.Context, string, string, []profile.VersionedRef, int64) (upstreamknowledge.Knowledge, error) {
	return r.value, nil
}

type noopKnowledge struct{}

func (noopKnowledge) Search(context.Context, *upstreamknowledge.SearchRequest) (*upstreamknowledge.SearchResult, error) {
	return &upstreamknowledge.SearchResult{Document: &document.Document{ID: "d", Content: "c"}, Score: 1}, nil
}

// TestFactoryWiresKnowledgeAndFailsClosed proves the Agent runtime consumes the
// framework Knowledge contract: a revision with knowledge refs resolves and
// builds, and one without a configured resolver refuses to build.
func TestFactoryWiresKnowledgeAndFailsClosed(t *testing.T) {
	const digest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	key := profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "app", AgentAppRevision: 1, ContentDigest: digest, ConfigVersion: 2, PolicyVersion: 3}
	withKnowledge := profile.ExecutionProfileSnapshot{Key: key, ContentDigest: digest, AgentKind: agentapp.AgentKindLLM,
		Instruction: "answer", ModelProfileRef: profile.VersionedRef{ID: "m", Version: 1},
		KnowledgeRefs: []profile.VersionedRef{{ID: "kb", Version: 1}}}

	profiles := profilememory.NewResolver(withKnowledge)
	base := func() Factory {
		policies := allowPlatformTools(t, key.TenantID, key.PolicyVersion, []governance.VersionedRef{knowledgeToolRef})
		return Factory{Profiles: profiles, Models: modelResolver{value: mockmodel.New()},
			Policies: policies, Confirmations: stubConfirmations{}, ToolResults: messagingmemory.New()}
	}

	factory := base()
	factory.Knowledge = knowledgeResolver{value: noopKnowledge{}}
	if _, err := factory.Build(context.Background(), withKnowledge); err != nil {
		t.Fatalf("build with knowledge resolver: %v", err)
	}

	factory = base()
	if _, err := factory.Build(context.Background(), withKnowledge); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("build without knowledge resolver = %v, want ErrCapabilityUnsupported", err)
	}
}
