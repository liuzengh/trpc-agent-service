package preprocess

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// MediaRouter keeps provider-specific downloaders at the composition edge.
// Provider IDs are never interpreted as URLs by this type: a provider
// downloader must be explicitly installed for channels that use opaque IDs;
// the generic HTTPS fetcher is only used for channels without a provider
// adapter (and still enforces its own host/SSRF policy).
type MediaRouter struct {
	Feishu, WeCom MediaFetcher
	Generic       MediaFetcher
}

func (r MediaRouter) Fetch(ctx context.Context, request MediaFetchRequest) (MediaDownload, error) {
	var fetcher MediaFetcher
	switch request.Channel {
	case "feishu":
		fetcher = r.Feishu
	case "wecom":
		fetcher = r.WeCom
	default:
		fetcher = r.Generic
	}
	if fetcher == nil {
		return MediaDownload{}, runtime.ErrCapabilityUnsupported
	}
	return fetcher.Fetch(ctx, request)
}

var _ MediaFetcher = MediaRouter{}
