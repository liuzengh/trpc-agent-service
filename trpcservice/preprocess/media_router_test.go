package preprocess

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type routerFetcher struct{ calls int }

func (f *routerFetcher) Fetch(context.Context, MediaFetchRequest) (MediaDownload, error) {
	f.calls++
	return MediaDownload{}, nil
}

func TestMediaRouterKeepsProviderIDsOffGenericFetcher(t *testing.T) {
	generic := &routerFetcher{}
	router := MediaRouter{Generic: generic}
	if _, err := router.Fetch(context.Background(), MediaFetchRequest{Channel: "feishu"}); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("expected provider capability error, got %v", err)
	}
	if generic.calls != 0 {
		t.Fatal("provider media unexpectedly routed to generic URL fetcher")
	}
}

func TestMediaRouterRoutesUnknownChannelToGeneric(t *testing.T) {
	generic := &routerFetcher{}
	router := MediaRouter{Generic: generic}
	if _, err := router.Fetch(context.Background(), MediaFetchRequest{Channel: "https"}); err != nil {
		t.Fatal(err)
	}
	if generic.calls != 1 {
		t.Fatalf("generic calls=%d", generic.calls)
	}
}
