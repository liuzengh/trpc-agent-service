package wecomadapter

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

// RetryPolicy bounds retention of one already-normalized callback. It is not a
// durable queue or a promise that the provider replays a disconnected callback.
type RetryPolicy struct {
	MaxAttempts      int
	Timeout, Backoff time.Duration
}

func normalizeRetry(p RetryPolicy) (RetryPolicy, error) {
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 6
	}
	if p.Timeout == 0 {
		p.Timeout = 2 * time.Second
	}
	if p.Backoff == 0 {
		p.Backoff = 100 * time.Millisecond
	}
	if p.MaxAttempts < 1 || p.MaxAttempts > 20 || p.Timeout < time.Millisecond || p.Timeout > 30*time.Second || p.Backoff < time.Millisecond || p.Backoff > time.Second {
		return p, domain.ErrInvalidInput
	}
	return p, nil
}

type temporaryFailure struct{ cause error }

func (e *temporaryFailure) Error() string { return "WeCom admission retry budget exhausted" }
func (e *temporaryFailure) Unwrap() error { return e.cause }

// IsRetryable exposes only a lifecycle classification to the composition root.
// It does not instruct the SDK to retry and never applies to protocol failures.
func IsRetryable(err error) bool { var temporary *temporaryFailure; return errors.As(err, &temporary) }
func IsRejected(err error) bool {
	return errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrInvalidInput)
}

func (h *Handler) accept(ctx context.Context, in domain.Inbound) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, h.retry.Timeout)
	defer cancel()
	var last error
	delay := h.retry.Backoff
	for attempt := 0; attempt < h.retry.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := attemptCtx.Err(); err != nil {
			return &temporaryFailure{err}
		}
		_, last = h.acceptor.AcceptInbound(attemptCtx, in)
		if last == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if IsRejected(last) {
			return last
		}
		if attempt+1 == h.retry.MaxAttempts {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-attemptCtx.Done():
			timer.Stop()
			if err := ctx.Err(); err != nil {
				return err
			}
			return &temporaryFailure{attemptCtx.Err()}
		case <-timer.C:
		}
		delay = min(2*delay, 400*time.Millisecond)
	}
	return &temporaryFailure{last}
}
