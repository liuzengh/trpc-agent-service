package trpcservice

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type testComponent struct {
	name     string
	events   *[]string
	startErr error
	close    func(context.Context) error
}

func (component *testComponent) Start(context.Context) error {
	*component.events = append(*component.events, "start:"+component.name)
	return component.startErr
}

func (component *testComponent) Close(ctx context.Context) error {
	*component.events = append(*component.events, "close:"+component.name)
	if component.close != nil {
		return component.close(ctx)
	}
	return nil
}

func TestAppRunStartsAndClosesInReverseOrder(t *testing.T) {
	var events []string
	app, err := NewApp(WithComponents(
		&testComponent{name: "gateway", events: &events},
		&testComponent{name: "worker", events: &events},
		&testComponent{name: "sender", events: &events},
	))
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := app.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := []string{
		"start:gateway", "start:worker", "start:sender",
		"close:sender", "close:worker", "close:gateway",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestAppRunClosesStartedComponentsOnStartupFailure(t *testing.T) {
	startErr := errors.New("sender unavailable")
	var events []string
	app, err := NewApp(WithComponents(
		&testComponent{name: "gateway", events: &events},
		&testComponent{name: "worker", events: &events},
		&testComponent{name: "sender", events: &events, startErr: startErr},
	))
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	err = app.Run(context.Background())
	if !errors.Is(err, startErr) {
		t.Fatalf("Run() error = %v, want wrapped %v", err, startErr)
	}
	want := []string{
		"start:gateway", "start:worker", "start:sender",
		"close:worker", "close:gateway",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestAppRunHonorsShutdownTimeout(t *testing.T) {
	var events []string
	app, err := NewApp(
		WithShutdownTimeout(10*time.Millisecond),
		WithComponents(&testComponent{
			name:   "blocked",
			events: &events,
			close: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}),
	)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = app.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want deadline exceeded", err)
	}
}

func TestAppRunStopsWaitingForCloseThatIgnoresContext(t *testing.T) {
	var events []string
	blocked := make(chan struct{})
	app, err := NewApp(
		WithShutdownTimeout(10*time.Millisecond),
		WithComponents(&testComponent{
			name:   "uncooperative",
			events: &events,
			close: func(context.Context) error {
				<-blocked
				return nil
			},
		}),
	)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	t.Cleanup(func() { close(blocked) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err = app.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Run() elapsed = %v, want bounded shutdown", elapsed)
	}
}

func TestAppIsSingleUse(t *testing.T) {
	app, err := NewApp()
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := app.Run(ctx); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if err := app.Run(ctx); !errors.Is(err, ErrAlreadyRun) {
		t.Fatalf("second Run() error = %v, want %v", err, ErrAlreadyRun)
	}
}

func TestNewAppRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opt  Option
	}{
		{name: "nil option", opt: nil},
		{name: "nil component", opt: WithComponents(nil)},
		{name: "non-positive timeout", opt: WithShutdownTimeout(0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewApp(test.opt); err == nil {
				t.Fatal("NewApp() error = nil, want non-nil")
			}
		})
	}
}
