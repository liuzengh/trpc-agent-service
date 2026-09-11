package manifestadapter

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

// A Worker pinned to a different platform contract release reports release skew
// so the Run waits for the matching release instead of being rejected as a
// capability failure.
func TestReaderResolveReleaseSkewWaitsForMatchingRelease(t *testing.T) {
	reader, route, _, p := readerDataInput(t, nil)
	reader.ContractDigest = "sha256:" + strings.Repeat("0", 64)
	plan, err := reader.Resolve(context.Background(), route)
	if !errors.Is(err, application.ErrManifestContractMismatch) || !reflect.DeepEqual(plan, domain.Plan{}) || p.reads != 1 {
		t.Fatalf("expected contract wait/empty Plan, got %+v %v reads=%d", plan, err, p.reads)
	}
}
