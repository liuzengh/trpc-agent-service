package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/bootstrap"
)

var (
	errUnexpectedInitArguments               = errors.New("unexpected init arguments")
	errUnexpectedDemoArguments               = errors.New("unexpected demo arguments")
	errInvalidServiceSupervisorConfiguration = errors.New("invalid service supervisor configuration")
)

func mapInitCommandError(ctx context.Context, err error, message string) error {
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s", bootstrap.ErrInvalidConfig, message)
}
