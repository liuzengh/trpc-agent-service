package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Run owns both listeners and background maintenance. Failure of either
// listener shuts down its peer, so internal auth never runs as an orphan process.
func (a *App) Run(ctx context.Context) error {
	if a == nil {
		return errors.New("control-api bootstrap: nil app")
	}
	if a.server == nil {
		return errors.New("control-api bootstrap: nil HTTP server")
	}
	if a.shutdownTimeout <= 0 {
		a.shutdownTimeout = 10 * time.Second
	}
	if a.database != nil {
		defer a.database.Close()
	}
	if a.manifestNATS != nil {
		defer a.manifestNATS.Close()
	}
	if a.natsConnection != nil {
		defer a.natsConnection.Close()
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	servers := []serverLifecycle{a.server}
	if a.internalServer != nil {
		servers = append(servers, a.internalServer)
	}
	if a.runtimeServer != nil {
		servers = append(servers, a.runtimeServer)
	}
	result := make(chan error, len(servers))
	for _, server := range servers {
		go func(s serverLifecycle) { result <- s.ListenAndServe() }(server)
	}
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		if a.routeRelay != nil {
			_ = a.routeRelay.Run(runCtx)
		}
	}()
	manifestRelayDone := make(chan struct{})
	go func() {
		defer close(manifestRelayDone)
		if a.manifestRelay != nil {
			_ = a.manifestRelay.Run(runCtx)
		}
	}()
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		if a.channelBinding == nil {
			return
		}
		ticker := time.NewTicker(time.Second)
		ticks := 0
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				work, cancel := context.WithTimeout(runCtx, 5*time.Second)
				if a.channelBinding.Preflights != nil {
					_ = a.channelBinding.Preflights.Maintain(work)
				}
				ticks++
				if ticks%60 == 0 {
					_ = a.channelBinding.Runtime.PruneObservations(work)
				}
				cancel()
			}
		}
	}()
	pending := len(servers)
	var first error
	select {
	case err := <-result:
		pending--
		if err != nil {
			first = fmt.Errorf("serve control API: %w", err)
		}
	case <-ctx.Done():
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer shutdownCancel()
	for _, server := range servers {
		if err := server.Shutdown(shutdownCtx); err != nil {
			first = errors.Join(first, fmt.Errorf("shutdown control API: %w", err))
		}
	}
	for pending > 0 {
		select {
		case err := <-result:
			pending--
			if err != nil {
				first = errors.Join(first, err)
			}
		case <-shutdownCtx.Done():
			return errors.Join(first, shutdownCtx.Err())
		}
	}
	select {
	case <-maintenanceDone:
	case <-shutdownCtx.Done():
		return errors.Join(first, shutdownCtx.Err())
	}
	select {
	case <-relayDone:
	case <-shutdownCtx.Done():
		return errors.Join(first, shutdownCtx.Err())
	}
	select {
	case <-manifestRelayDone:
	case <-shutdownCtx.Done():
		return errors.Join(first, shutdownCtx.Err())
	}
	return first
}
