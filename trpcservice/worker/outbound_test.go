package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestRenderOutboundDefaultsToExplicitText(t *testing.T) {
	got, err := renderOutbound(context.Background(), nil, runtime.ExecutionEnvelope{}, "hello")
	if err != nil || got.ContentType != messaging.ContentTypeText || string(got.Content) != "hello" {
		t.Fatalf("outbound=%#v err=%v", got, err)
	}
}

func TestRenderOutboundRejectsInvalidRichContent(t *testing.T) {
	for _, value := range []OutboundContent{
		{Content: []byte(`not-json`), ContentType: messaging.ContentTypeCard},
		{Content: []byte("content"), ContentType: "application/json"},
		{Content: nil, ContentType: "image/png"},
	} {
		_, err := renderOutbound(context.Background(), OutboundRendererFunc(func(context.Context, runtime.ExecutionEnvelope, string) (OutboundContent, error) {
			return value, nil
		}), runtime.ExecutionEnvelope{}, "model output")
		if !errors.Is(err, runtime.ErrInvariantViolation) {
			t.Fatalf("value=%#v err=%v", value, err)
		}
	}
}

func TestRenderOutboundAcceptsExplicitImage(t *testing.T) {
	image := []byte{0x89, 'P', 'N', 'G'}
	got, err := renderOutbound(context.Background(), OutboundRendererFunc(func(context.Context, runtime.ExecutionEnvelope, string) (OutboundContent, error) {
		return OutboundContent{Content: image, ContentType: "image/png"}, nil
	}), runtime.ExecutionEnvelope{}, "model output")
	if err != nil || got.ContentType != "image/png" || string(got.Content) != string(image) {
		t.Fatalf("outbound=%#v err=%v", got, err)
	}
}
