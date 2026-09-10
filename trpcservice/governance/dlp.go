package governance

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
)

type InputScanner interface {
	ScanMediaInput(context.Context, string, []byte, string) (preprocess.ScanResult, error)
}

type ScannerContentGuard struct{ Scanner InputScanner }

func (g ScannerContentGuard) Inspect(ctx context.Context, tenantID, _, _ string, content []byte) (DLPVerdict, string, error) {
	result, err := g.Scanner.ScanMediaInput(ctx, tenantID, content, "text/plain; charset=utf-8")
	if err != nil {
		return DLPVerdictUnknown, result.Version, err
	}
	switch result.Verdict {
	case preprocess.ScanClean:
		return DLPVerdictClean, result.Version, nil
	case preprocess.ScanRejected:
		return DLPVerdictRejected, result.Version, nil
	default:
		return DLPVerdictUnknown, result.Version, nil
	}
}

var _ ContentGuard = ScannerContentGuard{}
