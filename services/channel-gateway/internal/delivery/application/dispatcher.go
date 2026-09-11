package application

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type DispatchOptions struct {
	Tracer                       trace.Tracer
	CallTimeout, EvidenceTimeout time.Duration
	MaxConcurrent                int
}
type Dispatcher struct {
	ledger  Ledger
	senders SenderProvider
	options DispatchOptions
	slots   chan struct{}
}

func NewDispatcher(ledger Ledger, senders SenderProvider, o DispatchOptions) (*Dispatcher, error) {
	if ledger == nil || senders == nil {
		return nil, domain.ErrUnavailable
	}
	if o.CallTimeout == 0 {
		o.CallTimeout = 5 * time.Second
	}
	if o.EvidenceTimeout == 0 {
		o.EvidenceTimeout = 2 * time.Second
	}
	if o.MaxConcurrent == 0 {
		o.MaxConcurrent = 4
	}
	if o.CallTimeout < time.Millisecond || o.CallTimeout > time.Minute || o.EvidenceTimeout < time.Millisecond || o.EvidenceTimeout > time.Minute || o.MaxConcurrent < 1 || o.MaxConcurrent > 128 {
		return nil, domain.ErrInvalid
	}
	return &Dispatcher{ledger: ledger, senders: senders, options: o, slots: make(chan struct{}, o.MaxConcurrent)}, nil
}

// DispatchAccount performs a bounded A1 batch for one authenticated account. It
// is a use case, not a bootstrap loop; account discovery belongs to its caller.
func (s *Dispatcher) DispatchAccount(ctx context.Context, request domain.ClaimRequest) (int, error) {
	if ctx == nil {
		return 0, domain.ErrInvalid
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return 0, domain.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var claims []TracedClaim
	var err error
	if ledger, ok := s.ledger.(TracedClaimer); ok {
		claims, err = ledger.ClaimTraced(ctx, request)
	} else {
		// Nonpersistent ledger implementations retain their existing contract.
		var rows []domain.Claim
		rows, err = s.ledger.ClaimDue(ctx, request)
		for _, row := range rows {
			claims = append(claims, TracedClaim{Claim: row})
		}
	}
	if err != nil {
		return 0, err
	}
	if len(claims) > 1000 {
		return 0, domain.ErrInvalid
	}
	var combined error
	processed := 0
	for _, claim := range claims {
		if err = ctx.Err(); err != nil {
			return processed, errors.Join(combined, err)
		}
		if err = s.dispatch(ctx, claim); err != nil {
			combined = errors.Join(combined, err)
		} else {
			processed++
		}
	}
	return processed, combined
}
func (s *Dispatcher) dispatch(ctx context.Context, item TracedClaim) (resultErr error) {
	claim := item.Claim
	ctx, span := telemetrytrace.Resume(s.options.Tracer, ctx, item.Carrier, "gateway.reply.deliver", trace.WithAttributes(attribute.String("app.intent.id", claim.Intent.ID), attribute.String("app.run.id", claim.Intent.RunID), attribute.Int("app.part.index", claim.Part.Index)))
	defer func() { telemetrytrace.End(span, resultErr) }()
	digest, err := domain.RequestDigest(claim)
	if err != nil {
		return err
	}
	requestID := claim.Target.CallbackRequestID
	if claim.Target.Provider == "telegram" {
		var b [16]byte
		if _, err = rand.Read(b[:]); err != nil {
			return err
		}
		requestID = "telegram_" + hex.EncodeToString(b[:])
	}
	sender, err := s.senders.Reserve(ctx, SendRequest{Claim: claim, RequestID: requestID, RequestDigest: digest})
	// Partial setup can return a handle together with an error. Its reservation
	// must remain bounded and be released even when preparation persistence fails.
	if sender != nil {
		defer sender.Release()
	}
	// A broken provider port returning (nil, nil) is still a preparation failure.
	// Charge the ledger's bounded preparation budget instead of leaving CLAIMED
	// work to expire and be reclaimed forever without an attempt being recorded.
	if sender == nil && err == nil {
		err = domain.ErrUnavailable
	}
	if err != nil {
		result := domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorTemporary}
		switch {
		case errors.Is(err, domain.ErrExpired), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			result.ErrorClass = domain.ErrorDeadline
		case errors.Is(err, domain.ErrUnsupported), errors.Is(err, domain.ErrInvalid), errors.Is(err, domain.ErrUnauthorized):
			result.ErrorClass = domain.ErrorPermanent
		case errors.Is(err, domain.ErrNotFound):
			result.ErrorClass = domain.ErrorStaleOrigin
		}
		span.SetAttributes(attribute.String("app.outcome", string(result.Certainty)))
		return s.ledger.FinishPreparation(ctx, claim, result)
	}
	attempt, err := s.ledger.MarkCalling(ctx, domain.CallingRequest{Claim: claim, RequestID: requestID, RequestDigest: digest, Timeout: s.options.CallTimeout + s.options.EvidenceTimeout})
	if err != nil {
		return err
	} // Includes an uncertain COMMIT: never invoke the Provider.
	if attempt.RequestID != requestID || attempt.RequestDigest != digest || attempt.PartID != claim.Part.ID || attempt.IntentID != claim.Intent.ID || attempt.EvidenceToken == "" {
		return domain.ErrInvalid
	}
	callDeadline := time.Now().Add(s.options.CallTimeout)
	if attempt.Intent.Deadline.Before(callDeadline) {
		callDeadline = attempt.Intent.Deadline
	}
	if attempt.CallingUntil.Before(callDeadline) {
		callDeadline = attempt.CallingUntil
	}
	callCtx, cancel := context.WithDeadline(ctx, callDeadline)
	callCtx, sendSpan := telemetrytrace.Start(s.options.Tracer, callCtx, "gateway.im.send", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.Int64("app.retry.number", attempt.Number-1), attribute.String("app.attempt.id", attempt.ID)))
	result := sender.SendFinal(callCtx, attempt)
	cancel()
	if result.Validate() != nil {
		result = domain.Result{Certainty: domain.CertaintyUnknown, ErrorClass: domain.ErrorPermanent}
	}
	span.SetAttributes(attribute.String("app.outcome", string(result.Certainty)))
	sendSpan.SetAttributes(attribute.String("app.outcome", string(result.Certainty)))
	if result.Certainty != domain.CertaintyAccepted {
		sendSpan.SetStatus(codes.Error, "")
	}
	sendSpan.End()
	// Keep the reservation alive through result persistence. Parent cancellation
	// never converts a known ACK into UNKNOWN or skips bounded evidence recording.
	settle, stop := context.WithTimeout(context.WithoutCancel(ctx), s.options.EvidenceTimeout)
	defer stop()
	raw, _ := json.Marshal(result)
	hash := sha256.Sum256(append([]byte(attempt.ID+"\x00"), raw...))
	observation := domain.Observation{ID: "obs_" + hex.EncodeToString(hash[:]), AttemptID: attempt.ID, EvidenceToken: attempt.EvidenceToken, ProviderRequestID: attempt.RequestID, RequestDigest: attempt.RequestDigest, Result: result}
	for n := 0; n < 3; n++ {
		err = s.ledger.Observe(settle, observation)
		if err == nil {
			break
		}
		if errors.Is(err, domain.ErrInvalid) || errors.Is(err, domain.ErrUnauthorized) || errors.Is(err, domain.ErrConflict) || n == 2 {
			return err
		}
		timer := time.NewTimer(time.Duration(n+1) * 20 * time.Millisecond)
		select {
		case <-settle.Done():
			timer.Stop()
			return settle.Err()
		case <-timer.C:
		}
	}
	if err != nil {
		return err
	}
	return s.ledger.Finish(settle, attempt, result)
}
func (s *Dispatcher) Recover(ctx context.Context, limit int) (int, error) {
	if ctx == nil || limit < 1 || limit > 100 {
		return 0, domain.ErrInvalid
	}
	claims, e1 := s.ledger.RecoverExpiredClaims(ctx, limit)
	calls, e2 := s.ledger.RecoverStaleCalling(ctx, limit)
	return claims + calls, errors.Join(e1, e2)
}
