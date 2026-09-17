package main

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// runRecoverableLoop keeps a long-running dependency consumer alive across a
// transient backend failure. Configuration and capability errors remain fatal:
// retrying them would hide a deployment defect. The process stays live while
// its readiness endpoint reports the unavailable dependency as unready.
func runRecoverableLoop(ctx context.Context, name string, operation func(context.Context) error, retryDelay time.Duration, logger *roleLogger) error {
	if ctx == nil || name == "" || operation == nil || logger == nil {
		return runtime.ErrInvariantViolation
	}
	if retryDelay <= 0 {
		retryDelay = 250 * time.Millisecond
	}
	const maximumRetryDelay = 5 * time.Second
	delay := retryDelay
	for {
		err := operation(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, runtime.ErrInvariantViolation) || errors.Is(err, runtime.ErrCapabilityUnsupported) {
			return err
		}
		if err == nil {
			logger.Printf("%s stopped unexpectedly; retrying", name)
		} else {
			logger.Printf("%s degraded; retrying in %s: %v", name, delay, err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
		if delay < maximumRetryDelay {
			delay *= 2
			if delay > maximumRetryDelay {
				delay = maximumRetryDelay
			}
		}
	}
}
