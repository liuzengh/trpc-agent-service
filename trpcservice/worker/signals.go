package worker

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// InstallSignalDrain connects process termination to the worker lifecycle.
// It only announces drain; Consumer owns the bounded completion window and
// durable lease handoff. The returned function unregisters signals and is
// safe to call more than once.
func InstallSignalDrain(ctx context.Context, lifecycle *Lifecycle, signals ...os.Signal) (stop func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	if lifecycle == nil {
		return func() {}
	}
	if len(signals) == 0 {
		signals = []os.Signal{syscall.SIGTERM, syscall.SIGINT}
	}
	channel := make(chan os.Signal, 1)
	signal.Notify(channel, signals...)
	done := make(chan struct{})
	var once sync.Once
	stop = func() {
		once.Do(func() {
			signal.Stop(channel)
			close(done)
		})
	}
	go func() {
		defer stop()
		select {
		case <-ctx.Done():
		case <-done:
		case _, ok := <-channel:
			if ok {
				lifecycle.BeginDrain()
			}
		}
	}()
	return stop
}
