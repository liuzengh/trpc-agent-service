// Package delivery owns provider-neutral reliable reply delivery after the
// account queue. Provider adapters are called only after a Delivery Ledger
// segment claim succeeds.
package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type AdapterResolver interface {
	ResolveAdapter(context.Context, string, string) (channel.Adapter, error)
}

type VersionedAdapterResolver interface {
	ResolveVersionedAdapter(context.Context, string, string, int64) (channel.Adapter, error)
}

// Compatibility aliases keep delivery callers source-compatible while the
// error taxonomy itself lives in the provider-neutral Adapter contract.
type AmbiguousError = channel.AmbiguousDeliveryError
type RetryAfterError = channel.RetryableDeliveryError
type PermanentError = channel.PermanentDeliveryError

// DeferredError keeps the account-queue entry pending while another delivery
// attempt is active or its durable retry deadline has not arrived.
type DeferredError struct{ NotBefore time.Time }

func (e DeferredError) Error() string { return "reply delivery is deferred" }

// TerminalError means the failure was durably recorded as DeliveryFailed and
// the account-queue entry can be ACKed after the error is reported.
type TerminalError struct{ Err error }

func (e TerminalError) Error() string {
	if e.Err == nil {
		return "reply delivery permanently failed"
	}
	return e.Err.Error()
}
func (e TerminalError) Unwrap() error { return e.Err }

type Service struct {
	Results              messaging.ResultStore
	Ledger               messaging.DeliveryLedger
	Adapters             AdapterResolver
	Owner                string
	ClaimTTL             time.Duration
	ClaimRenewInterval   time.Duration
	RendererVersion      string
	FormatVersion        string
	DefaultRetryDelay    time.Duration
	MaxRetryDelay        time.Duration
	MaxAttempts          int
	MaxReconcileAttempts int
}

func (s Service) Deliver(ctx context.Context, event channel.ReplyEvent) error {
	if s.Results == nil || s.Ledger == nil || s.Adapters == nil || event.SchemaVersion != 1 ||
		s.Owner == "" || event.TenantID == "" || event.RequestID == "" || event.ChannelBindingID == "" || event.ConfigVersion < 1 || event.DeliveryKey == "" || event.ContentRef == "" ||
		event.Target.Channel == "" || event.Target.ExternalAccountID == "" {
		return runtime.ErrInvariantViolation
	}
	result, err := messaging.ResolveReplyContent(ctx, s.Results, event.TenantID, event.RequestID, event.ContentRef)
	if err != nil {
		return err
	}
	if result.ResultRef != event.ContentRef {
		return runtime.ErrVersionMismatch
	}
	adapter, err := s.resolveAdapter(ctx, event)
	if err != nil {
		return err
	}
	if adapter == nil || adapter.ID() != event.Target.Channel {
		return runtime.ErrTenantScope
	}
	contentType, err := messaging.NormalizeContentType(result.ContentType)
	if err != nil {
		return err
	}
	segments := [][]byte{append([]byte(nil), result.Content...)}
	if contentType == messaging.ContentTypeText {
		segments = splitText(result.Content, maxTextBytes(adapter))
	}
	plan := messaging.DeliveryPlan{RendererVersion: s.rendererVersion(), FormatVersion: s.formatVersion(contentType, len(segments)), ContentDigest: result.ContentDigest, SegmentCount: len(segments)}
	for segmentNo, content := range segments {
		if err := s.deliverSegment(ctx, event, adapter, plan, segmentNo, content, contentType); err != nil {
			return err
		}
	}
	return nil
}

func (s Service) deliverSegment(ctx context.Context, event channel.ReplyEvent, adapter channel.Adapter, plan messaging.DeliveryPlan, segmentNo int, content []byte, contentType string) error {
	key := messaging.DeliveryKey{TenantID: event.TenantID, DeliveryKey: event.DeliveryKey, SegmentNo: segmentNo}
	claimTTL := s.ClaimTTL
	if claimTTL <= 0 {
		claimTTL = 30 * time.Second
	}
	record, acquired, err := s.Ledger.ClaimDelivery(ctx, key, plan, messaging.DeliveryClaim{Owner: s.Owner, TTL: claimTTL})
	if err != nil {
		return err
	}
	if !acquired {
		switch record.State {
		case messaging.DeliverySent, messaging.DeliveryFailed:
			return nil
		case messaging.DeliveryAmbiguous:
			return s.reconcile(ctx, event, record)
		case messaging.DeliveryPending, messaging.DeliverySending, messaging.DeliveryRetryWait:
			return DeferredError{NotBefore: record.NotBefore}
		default:
			return runtime.ErrInvariantViolation
		}
	}
	contentDigest := plan.ContentDigest
	if plan.SegmentCount > 1 {
		sum := sha256.Sum256(content)
		contentDigest = hex.EncodeToString(sum[:])
	}
	record, resultDelivery, deliverErr := s.deliverWithClaimRenewal(ctx, adapter, channel.DeliveryRequest{
		Event: event, ClientRequestID: record.ClientRequestID, Target: event.Target,
		Content: append([]byte(nil), content...), ContentDigest: contentDigest, ContentType: contentType,
	}, record, claimTTL)
	if deliverErr != nil {
		var ambiguous AmbiguousError
		if errors.As(deliverErr, &ambiguous) {
			record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "response_lost"
			_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
			return errors.Join(deliverErr, finishErr)
		}
		var permanent PermanentError
		if errors.As(deliverErr, &permanent) {
			return s.finishFailed(ctx, record, deliverErr, permanent.Class, false)
		}
		var retryAfter RetryAfterError
		if errors.As(deliverErr, &retryAfter) {
			return s.finishRetry(ctx, record, deliverErr, retryAfter.RetryAfter)
		}
		return s.finishRetry(ctx, record, deliverErr, 0)
	}
	if !resultDelivery.Delivered {
		return s.finishRetry(ctx, record, runtime.ErrBackendUnavailable, 0)
	}
	if resultDelivery.ProviderMessageID == "" {
		record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "missing_provider_message_id"
		_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
		return errors.Join(AmbiguousError{Err: runtime.ErrInvariantViolation}, finishErr)
	}
	record.State = messaging.DeliverySent
	record.ProviderMessageID = resultDelivery.ProviderMessageID
	record.LastErrorClass = ""
	_, err = s.Ledger.FinishDelivery(ctx, record, record.Version)
	return err
}

func (s Service) rendererVersion() string {
	if s.RendererVersion != "" {
		return s.RendererVersion
	}
	return "terminal-text-v1"
}

func (s Service) formatVersion(contentType string, segmentCount int) string {
	if s.FormatVersion != "" {
		return s.FormatVersion
	}
	if contentType == messaging.ContentTypeCard {
		return "card-v1"
	}
	if strings.HasPrefix(contentType, "image/") {
		return "image-v1"
	}
	if segmentCount > 1 {
		return "text-segment-v1"
	}
	return "text-v1"
}

func maxTextBytes(adapter channel.Adapter) int {
	if limit, ok := adapter.(channel.TextSegmentLimit); ok && limit.MaxTextBytes() > 0 {
		return limit.MaxTextBytes()
	}
	return 0
}

// splitText preserves every input byte while ensuring that a segment never
// ends in the middle of a UTF-8 rune. Invalid UTF-8 is passed through as one
// segment so the provider adapter can retain its existing permanent-error
// classification instead of silently changing the response content.
func splitText(content []byte, maxBytes int) [][]byte {
	if maxBytes <= 0 || len(content) <= maxBytes || !utf8.Valid(content) {
		return [][]byte{append([]byte(nil), content...)}
	}
	segments := make([][]byte, 0, (len(content)+maxBytes-1)/maxBytes)
	for start := 0; start < len(content); {
		end := start + maxBytes
		if end >= len(content) {
			segments = append(segments, append([]byte(nil), content[start:]...))
			break
		}
		for end > start && !utf8.Valid(content[start:end]) {
			end--
		}
		// maxBytes can be smaller than the first rune only for a malformed
		// adapter declaration. Preserve the original response in that case.
		if end == start {
			return [][]byte{append([]byte(nil), content...)}
		}
		segments = append(segments, append([]byte(nil), content[start:end]...))
		start = end
	}
	return segments
}

func (s Service) reconcile(ctx context.Context, event channel.ReplyEvent, record messaging.DeliveryRecord) error {
	if record.NotBefore.After(time.Now()) {
		return DeferredError{NotBefore: record.NotBefore}
	}
	adapter, err := s.resolveAdapter(ctx, event)
	if err != nil {
		return s.deferReconciliation(ctx, record, err)
	}
	reconciler, ok := adapter.(channel.DeliveryReconciler)
	if !ok {
		return s.deferReconciliation(ctx, record, runtime.ErrCapabilityUnsupported)
	}
	result, err := reconciler.ReconcileDelivery(ctx, channel.ReconciliationRequest{Event: event, ClientRequestID: record.ClientRequestID})
	if err != nil {
		return s.deferReconciliation(ctx, record, err)
	}
	switch result.Status {
	case channel.ReconciliationDelivered:
		if result.ProviderMessageID == "" {
			return s.deferReconciliation(ctx, record, runtime.ErrInvariantViolation)
		}
		record.State, record.ProviderMessageID, record.LastErrorClass = messaging.DeliverySent, result.ProviderMessageID, ""
		_, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version)
		return err
	case channel.ReconciliationNotDelivered:
		record.State, record.NotBefore, record.LastErrorClass = messaging.DeliveryRetryWait, time.Now().UTC(), "reconciled_not_delivered"
		if _, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version); err != nil {
			return err
		}
		return DeferredError{NotBefore: record.NotBefore}
	case channel.ReconciliationUnknown:
		return s.deferReconciliation(ctx, record, runtime.ErrCommitConflict)
	default:
		return s.deferReconciliation(ctx, record, runtime.ErrInvariantViolation)
	}
}

func (s Service) resolveAdapter(ctx context.Context, event channel.ReplyEvent) (channel.Adapter, error) {
	if resolver, ok := s.Adapters.(VersionedAdapterResolver); ok {
		return resolver.ResolveVersionedAdapter(ctx, event.TenantID, event.ChannelBindingID, event.ConfigVersion)
	}
	return s.Adapters.ResolveAdapter(ctx, event.TenantID, event.ChannelBindingID)
}

func (s Service) finishRetry(ctx context.Context, record messaging.DeliveryRecord, cause error, delay time.Duration) error {
	if record.Attempt >= s.maxAttempts() {
		return s.finishFailed(ctx, record, cause, "retry_exhausted", false)
	}
	delay = s.backoff(record.Attempt, delay)
	record.State = messaging.DeliveryRetryWait
	record.NotBefore = time.Now().Add(delay)
	record.LastErrorClass = "retryable"
	_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
	return errors.Join(cause, finishErr)
}

type renewalResult struct {
	record messaging.DeliveryRecord
	err    error
}

func (s Service) deliverWithClaimRenewal(ctx context.Context, adapter channel.Adapter, request channel.DeliveryRequest, record messaging.DeliveryRecord, claimTTL time.Duration) (messaging.DeliveryRecord, channel.DeliveryResult, error) {
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	interval := s.ClaimRenewInterval
	if interval <= 0 || interval >= claimTTL {
		interval = claimTTL / 3
	}
	if interval <= 0 {
		interval = time.Nanosecond
	}
	stop := make(chan struct{})
	done := make(chan renewalResult, 1)
	go func() {
		latest := record
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				done <- renewalResult{record: latest}
				return
			case <-callCtx.Done():
				done <- renewalResult{record: latest, err: callCtx.Err()}
				return
			case <-ticker.C:
				renewed, err := s.Ledger.RenewDeliveryClaim(callCtx, latest, claimTTL)
				if err != nil {
					cancel()
					done <- renewalResult{record: latest, err: err}
					return
				}
				latest = renewed
			}
		}
	}()
	result, deliverErr := adapter.Deliver(callCtx, request)
	close(stop)
	renewal := <-done
	if renewal.err != nil {
		return renewal.record, result, errors.Join(deliverErr, renewal.err)
	}
	return renewal.record, result, deliverErr
}

func (s Service) deferReconciliation(ctx context.Context, record messaging.DeliveryRecord, cause error) error {
	nextAttempt := record.ReconcileAttempt + 1
	if nextAttempt >= s.maxReconcileAttempts() {
		return s.finishFailed(ctx, record, cause, "reconcile_exhausted", true)
	}
	record.ReconcileAttempt = nextAttempt
	record.NotBefore = time.Now().Add(s.backoff(nextAttempt, 0))
	record.LastErrorClass = "reconcile_retryable"
	updated, err := s.Ledger.DeferDeliveryReconciliation(ctx, record, record.Version)
	if err != nil {
		return errors.Join(cause, err)
	}
	return DeferredError{NotBefore: updated.NotBefore}
}

func (s Service) finishFailed(ctx context.Context, record messaging.DeliveryRecord, cause error, class string, reconcile bool) error {
	record.State = messaging.DeliveryFailed
	record.LastErrorClass = normalizeErrorClass(class)
	var err error
	if reconcile {
		_, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version)
	} else {
		_, err = s.Ledger.FinishDelivery(ctx, record, record.Version)
	}
	if err != nil {
		return errors.Join(cause, err)
	}
	return TerminalError{Err: cause}
}

func (s Service) backoff(attempt int, requested time.Duration) time.Duration {
	base := s.DefaultRetryDelay
	if base <= 0 {
		base = time.Second
	}
	if requested > 0 {
		base = requested
	} else if attempt > 1 {
		shift := attempt - 1
		if shift > 20 {
			shift = 20
		}
		factor := time.Duration(1 << shift)
		maximum := s.MaxRetryDelay
		if maximum <= 0 {
			maximum = time.Minute
		}
		if base > maximum/factor {
			return maximum
		}
		base *= factor
	}
	maximum := s.MaxRetryDelay
	if maximum <= 0 {
		maximum = time.Minute
	}
	if base > maximum {
		return maximum
	}
	return base
}

func (s Service) maxAttempts() int {
	if s.MaxAttempts > 0 {
		return s.MaxAttempts
	}
	return 8
}

func (s Service) maxReconcileAttempts() int {
	if s.MaxReconcileAttempts > 0 {
		return s.MaxReconcileAttempts
	}
	return 8
}

func normalizeErrorClass(class string) string {
	class = strings.TrimSpace(class)
	if class == "" {
		return "permanent"
	}
	for _, char := range class {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.' {
			continue
		}
		return "permanent"
	}
	return class
}
