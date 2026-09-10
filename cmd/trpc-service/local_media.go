package main

import (
	"context"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// localDisabledDLP is intentionally restricted to the disposable local
// profile, whose seeded policy sets InputDLP to disabled. It makes that choice
// explicit in durable artifact metadata instead of pretending a remote DLP
// service ran. Malware scanning remains mandatory through ClamAV.
type localDisabledDLP struct{}

func (localDisabledDLP) ScanMediaInput(_ context.Context, tenantID string, content []byte, mediaType string) (preprocess.ScanResult, error) {
	if strings.TrimSpace(tenantID) == "" || len(content) == 0 || strings.TrimSpace(mediaType) == "" {
		return preprocess.ScanResult{}, runtime.ErrInvalidEnvelope
	}
	return preprocess.ScanResult{Verdict: preprocess.ScanClean, Version: "local-policy-disabled"}, nil
}
