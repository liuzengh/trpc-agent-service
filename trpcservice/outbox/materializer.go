package outbox

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// ErrMaterialization wraps failures while creating durable reply segments.
var ErrMaterialization = errors.New("reply materialization failed")

// defaultSegmentRunes is deliberately conservative: 512 Unicode code points
// fit within the 2048-byte text limit of currently supported IM providers.
const defaultSegmentRunes = 512

// Materializer turns one completed Runner reply into durable, idempotent
// segments. It is deliberately independent of any channel SDK.
type Materializer struct {
	store       runtimestorage.ReplyBatchEnqueuer
	segmentSize int
	telemetry   observability.Provider
	metrics     metrics.Catalog
	backend     string
}

// MaterializerConfig controls durable reply segmentation.
type MaterializerConfig struct {
	// BatchStore is the reply materialization capability owned by the outbox.
	BatchStore    runtimestorage.ReplyBatchEnqueuer
	SegmentSize   int
	Observability observability.Provider
	Backend       string
}

// MaterializeInput identifies the completed reply to segment.
type MaterializeInput struct {
	TenantID    string
	EventID     string
	ReplyID     string
	RequestID   string
	TraceID     string
	TraceParent string
	Payload     string
	Segments    []ReplySegment
	ReplyTarget runtimestorage.ReplyTarget
}

// ReplySegment is one protocol-neutral reply selected by the completed Runner
// turn. Text segments retain historical rune splitting; media segments remain
// atomic so the target channel can send their verified attachment natively.
type ReplySegment struct {
	Kind       runtimestorage.ReplyKind
	Payload    string
	Attachment attachment.Reference
	Fallback   string
}

// NewMaterializer creates a reply materializer with a default segment size.
func NewMaterializer(config MaterializerConfig) (*Materializer, error) {
	if config.BatchStore == nil {
		return nil, ErrInvalid
	}
	if config.SegmentSize <= 0 {
		config.SegmentSize = defaultSegmentRunes
	}
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	if config.Backend == "" {
		config.Backend = "other"
	}
	return &Materializer{store: config.BatchStore, segmentSize: config.SegmentSize, telemetry: config.Observability, metrics: metrics.New(config.Observability), backend: config.Backend}, nil
}

// Materialize writes all segments under the stable reply identity. A repeated
// call is idempotent when the existing rows have the same event and payload.
func (m *Materializer) Materialize(ctx context.Context, input MaterializeInput) (count int, err error) {
	if m == nil || ctx == nil || runtimestorage.ValidateTenant(input.TenantID) != nil || input.EventID == "" || input.ReplyID == "" || runtimestorage.ValidateReplyTarget(input.ReplyTarget) != nil {
		return 0, ErrInvalid
	}
	replies, err := m.buildReplies(input)
	if err != nil {
		return 0, ErrInvalid
	}
	if m.store == nil {
		return 0, errors.Join(ErrMaterialization, runtimestorage.ErrInvalid)
	}
	batchStore := m.store
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, m.telemetry, observability.OperationStorageOperation, "storage")
	labels := map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": m.backend}
	_ = m.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": m.backend, "status": "started"})
	defer func() {
		finish(err)
		_ = m.metrics.Operation(operationCtx, started, labels, err)
		status := "success"
		if err != nil {
			status = observability.ErrorClass(err)
			if status == "" {
				status = "error"
			}
		}
		_ = m.metrics.BackendDuration(operationCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "provider": m.backend, "status": status, "error_class": observability.ErrorClass(err)})
	}()
	if input.RequestID != "" {
		correlatedStore, correlated := m.store.(runtimestorage.ReplyBatchCorrelationEnqueuer)
		if !correlated {
			return 0, errors.Join(ErrMaterialization, runtimestorage.ErrInvalid)
		}
		traceParent := input.TraceParent
		if traceParent == "" {
			traceParent = observability.TraceParentFromContext(operationCtx)
		} else {
			traceParent = observability.TraceParentFromContext(observability.ContextWithTraceParent(context.Background(), traceParent))
		}
		_, err = correlatedStore.EnqueueRepliesWithCorrelation(operationCtx, runtimestorage.ReplyCorrelation{TenantID: input.TenantID, EventID: input.EventID, RequestID: input.RequestID, TraceID: input.TraceID, TraceParent: traceParent}, replies)
	} else {
		_, err = batchStore.EnqueueReplies(operationCtx, replies)
	}
	if err != nil {
		return 0, redactedMaterializationError(err)
	}
	return len(replies), nil
}

func (m *Materializer) buildReplies(input MaterializeInput) ([]runtimestorage.ReplyOutbox, error) {
	if len(input.Segments) == 0 {
		parts := splitRunes(input.Payload, m.segmentSize)
		if len(parts) == 0 {
			return nil, ErrInvalid
		}
		return textReplies(input, parts), nil
	}
	if input.Payload != "" {
		return nil, ErrInvalid
	}
	segments := make([]runtimestorage.ReplyOutbox, 0, len(input.Segments))
	for _, segment := range input.Segments {
		kind := segment.Kind
		if kind == "" {
			kind = runtimestorage.ReplyKindText
		}
		if kind == runtimestorage.ReplyKindText {
			parts := splitRunes(segment.Payload, m.segmentSize)
			if len(parts) == 0 {
				return nil, ErrInvalid
			}
			segments = append(segments, textReplies(input, parts)...)
			continue
		}
		value, err := runtimestorage.NormalizeReplyOutbox(runtimestorage.ReplyOutbox{
			Kind: kind, Payload: segment.Payload, Attachment: segment.Attachment, Fallback: segment.Fallback,
		})
		if err != nil {
			return nil, err
		}
		segments = append(segments, runtimestorage.ReplyOutbox{
			TenantID: input.TenantID, ReplyID: input.ReplyID, EventID: input.EventID,
			Kind: value.Kind, Payload: value.Payload, Attachment: value.Attachment, Fallback: value.Fallback,
			ReplyTarget: input.ReplyTarget,
		})
	}
	if len(segments) == 0 {
		return nil, ErrInvalid
	}
	for index := range segments {
		segments[index].SegmentIndex = index
		segments[index].SegmentCount = len(segments)
	}
	return segments, nil
}

func textReplies(input MaterializeInput, payloads []string) []runtimestorage.ReplyOutbox {
	replies := make([]runtimestorage.ReplyOutbox, 0, len(payloads))
	for index, payload := range payloads {
		replies = append(replies, runtimestorage.ReplyOutbox{
			TenantID: input.TenantID, ReplyID: input.ReplyID, EventID: input.EventID,
			SegmentIndex: index, SegmentCount: len(payloads), Payload: payload, ReplyTarget: input.ReplyTarget,
		})
	}
	return replies
}

// redactedMaterializationError keeps only stable, caller-actionable classes.
// Storage adapters and provider fakes may return driver details, SQL text, or
// credentials; those values must never cross the materialization boundary.
func redactedMaterializationError(err error) error {
	if err == nil {
		return nil
	}
	for _, stable := range []error{context.Canceled, context.DeadlineExceeded, runtimestorage.ErrConflict, runtimestorage.ErrDuplicate, runtimestorage.ErrInvalid, runtimestorage.ErrNotFound, runtimestorage.ErrStorage} {
		if errors.Is(err, stable) {
			return errors.Join(ErrMaterialization, stable)
		}
	}
	return ErrMaterialization
}

func splitRunes(value string, size int) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	runes := []rune(value)
	result := make([]string, 0, (len(runes)+size-1)/size)
	for start := 0; start < len(runes); start += size {
		end := start + size
		if end > len(runes) {
			end = len(runes)
		}
		result = append(result, string(runes[start:end]))
	}
	return result
}
