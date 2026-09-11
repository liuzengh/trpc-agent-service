package assembly

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/modelusage"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestUsageIdentityModelRecordsActualCandidateUsage(t *testing.T) {
	recorder := modelusage.NewRecorder()
	ctx := modelusage.WithRecorder(context.Background(), recorder)
	delegate := &backendHealthProbeModel{name: "configured-fallback", response: &model.Response{
		Done: true, Model: "provider-reported-v2",
		Usage: &model.Usage{PromptTokens: 120, CompletionTokens: 30, TotalTokens: 150,
			PromptTokensDetails: model.PromptTokensDetails{CachedTokens: 40}},
	}}
	observed := observeActualModelUsage(delegate, "fallback-provider", "configured-fallback")
	responses, err := observed.GenerateContent(ctx, &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	for range responses {
	}
	segments := recorder.Snapshot()
	if len(segments) != 1 {
		t.Fatalf("usage segments = %#v, want one", segments)
	}
	segment := segments[0]
	if segment.ProviderID != "fallback-provider" || segment.ModelName != "configured-fallback" || segment.ReportedModel != "provider-reported-v2" ||
		segment.PromptTokens != 120 || segment.CachedPromptTokens != 40 || segment.CompletionTokens != 30 || segment.TotalTokens != 150 {
		t.Fatalf("usage segment = %#v", segment)
	}
}
