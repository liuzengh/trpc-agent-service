package worker

import (
	"context"
	"encoding/json"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

// OutboundContent is the service-owned representation of a terminal reply.
// Rich content is deliberately produced only by an explicit renderer; the
// worker never interprets ordinary model text as a card or an image.
type OutboundContent struct {
	Content     []byte
	ContentType string
}

// OutboundRenderer converts an already-governed model result to the exact
// reply representation that is persisted and delivered. Implementations are
// application code and may, for example, construct a platform card or select
// an already-scanned image artifact. They must be deterministic for the same
// execution envelope so worker retries retain their idempotent result.
type OutboundRenderer interface {
	RenderOutbound(context.Context, runtime.ExecutionEnvelope, string) (OutboundContent, error)
}

// OutboundRendererFunc lets applications install a small, explicit renderer
// without declaring a dedicated type.
type OutboundRendererFunc func(context.Context, runtime.ExecutionEnvelope, string) (OutboundContent, error)

func (f OutboundRendererFunc) RenderOutbound(ctx context.Context, envelope runtime.ExecutionEnvelope, modelContent string) (OutboundContent, error) {
	return f(ctx, envelope, modelContent)
}

func renderOutbound(ctx context.Context, renderer OutboundRenderer, envelope runtime.ExecutionEnvelope, modelContent string) (OutboundContent, error) {
	value := OutboundContent{Content: []byte(modelContent), ContentType: messaging.ContentTypeText}
	var err error
	if renderer != nil {
		value, err = renderer.RenderOutbound(ctx, envelope, modelContent)
		if err != nil {
			return OutboundContent{}, err
		}
	}
	contentType, err := messaging.NormalizeContentType(value.ContentType)
	if err != nil || len(value.Content) == 0 {
		return OutboundContent{}, runtime.ErrInvariantViolation
	}
	if contentType == messaging.ContentTypeText && !utf8.Valid(value.Content) {
		return OutboundContent{}, runtime.ErrInvariantViolation
	}
	if contentType == messaging.ContentTypeCard && !json.Valid(value.Content) {
		return OutboundContent{}, runtime.ErrInvariantViolation
	}
	value.Content = append([]byte(nil), value.Content...)
	value.ContentType = contentType
	return value, nil
}
