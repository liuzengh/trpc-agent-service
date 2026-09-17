package condition

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/graph"
)

func TestLastResponsePresenceSelectsOnlyReviewedBranches(t *testing.T) {
	selector, err := DefaultRegistry().ResolveGraphCondition(context.Background(), "tenant-a", profile.VersionedRef{
		ID: LastResponsePresence, Version: Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, response, want string
	}{
		{name: "empty", want: BranchEmpty},
		{name: "whitespace", response: " \n", want: BranchEmpty},
		{name: "present", response: "answer", want: BranchPresent},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, selectErr := selector.Select(context.Background(), graph.State{graph.StateKeyLastResponse: test.response})
			if selectErr != nil || got != test.want {
				t.Fatalf("branch=%q err=%v, want %q", got, selectErr, test.want)
			}
		})
	}
	if _, err := DefaultRegistry().ResolveGraphCondition(context.Background(), "tenant-a", profile.VersionedRef{ID: "dynamic", Version: 1}); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("unknown condition error=%v", err)
	}
}
