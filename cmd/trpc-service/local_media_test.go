package main

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestLocalDisabledDLPMarksDisabledPolicyWithoutTreatingInvalidInputAsClean(t *testing.T) {
	result, err := (localDisabledDLP{}).ScanMediaInput(context.Background(), "tenant", []byte("safe"), "image/png")
	if err != nil || result.Verdict != preprocess.ScanClean || result.Version != "local-policy-disabled" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	_, err = (localDisabledDLP{}).ScanMediaInput(context.Background(), "", []byte("safe"), "image/png")
	if !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("err=%v", err)
	}
}
