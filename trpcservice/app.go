// Package trpcservice wires and runs the multi-tenant Agent platform.
package trpcservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultShutdownTimeout = 10 * time.Second

// ErrAlreadyRun is returned when a single-use App is run more than once.
var ErrAlreadyRun = errors.New("trpcservice: app already run")

// Component is a service dependency managed by App.
//
// Start must return after the component has started. Long-running work should
// run in goroutines owned by the component and stop when ctx is canceled or
// Close is called. Close must honor ctx and release its resources promptly;
// App also stops waiting when the shared shutdown deadline expires.
type Component interface {
	Start(ctx context.Context) error
	Close(ctx context.Context) error
}

// App owns the lifecycle of platform components.
type App struct {
	components      []Component
	shutdownTimeout time.Duration

	mu  sync.Mutex
	ran bool
}

// Option configures an App.
type Option func(*App) error

// NewApp constructs an App.
func NewApp(opts ...Option) (*App, error) {
	app := &App{shutdownTimeout: defaultShutdownTimeout}
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("trpcservice: nil app option")
		}
		if err := opt(app); err != nil {
			return nil, err
		}
	}
	return app, nil
}

// WithComponents registers components in startup order. They are closed in
// reverse order.
func WithComponents(components ...Component) Option {
	return func(app *App) error {
		for i, component := range components {
			if component == nil {
				return fmt.Errorf("trpcservice: component %d is nil", i)
			}
		}
		app.components = append(app.components, components...)
		return nil
	}
}

// WithShutdownTimeout sets the total time allowed to close all components.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(app *App) error {
		if timeout <= 0 {
			return errors.New("trpcservice: shutdown timeout must be positive")
		}
		app.shutdownTimeout = timeout
		return nil
	}
}

// Run starts all components, waits for ctx cancellation, and closes started
// components in reverse order. App instances are single-use.
func (app *App) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("trpcservice: nil run context")
	}
	app.mu.Lock()
	if app.ran {
		app.mu.Unlock()
		return ErrAlreadyRun
	}
	app.ran = true
	app.mu.Unlock()

	started := make([]Component, 0, len(app.components))
	for i, component := range app.components {
		if err := component.Start(ctx); err != nil {
			closeErr := app.closeComponents(started)
			return errors.Join(
				fmt.Errorf("start component %d: %w", i, err),
				closeErr,
			)
		}
		started = append(started, component)
	}

	<-ctx.Done()
	return app.closeComponents(started)
}

func (app *App) closeComponents(components []Component) error {
	ctx, cancel := context.WithTimeout(context.Background(), app.shutdownTimeout)
	defer cancel()

	var errs []error
	for i := len(components) - 1; i >= 0; i-- {
		closeResult := make(chan error, 1)
		go func(component Component) {
			closeResult <- component.Close(ctx)
		}(components[i])

		var err error
		select {
		case err = <-closeResult:
		case <-ctx.Done():
			err = ctx.Err()
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("close component %d: %w", i, err))
		}
		if ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}
