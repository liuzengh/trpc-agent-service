package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
)

const (
	defaultWorkerConcurrency = 4
	maxWorkerConcurrency     = 64
)

func workerConcurrency(getenv environment) (int, error) {
	raw := strings.TrimSpace(getenv("WORKER_CONCURRENCY"))
	if raw == "" {
		return defaultWorkerConcurrency, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 || value > maxWorkerConcurrency {
		return 0, fmt.Errorf("WORKER_CONCURRENCY must be between 1 and %d", maxWorkerConcurrency)
	}
	return value, nil
}

// startKafkaWorkers runs one ordered consumer loop per worker. Each Worker is
// backed by an independent Kafka group member, so partitions can execute in
// parallel while records assigned to one member remain serial.
func startKafkaWorkers(ctx context.Context, workers []*messaging.Worker) (<-chan error, <-chan struct{}) {
	errorsC := make(chan error, len(workers))
	done := make(chan struct{})
	var group sync.WaitGroup
	for _, worker := range workers {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			var workerErr error
			panicErr := safego.Run("Kafka worker", func() { workerErr = runKafkaWorkerLoop(ctx, worker) })
			if panicErr != nil {
				errorsC <- panicErr
				return
			}
			if workerErr != nil {
				errorsC <- workerErr
			}
		}()
	}
	safego.Go("Kafka worker join", func() {
		group.Wait()
		close(errorsC)
		close(done)
	})
	return errorsC, done
}

func runKafkaWorkerLoop(ctx context.Context, worker *messaging.Worker) error {
	if worker == nil {
		return errors.New("Kafka worker is required")
	}
	retry := newWorkerRetryBackoff()
	for {
		err := worker.RunOnce(ctx)
		if err == nil {
			retry.Reset()
			continue
		}
		if errors.Is(err, messaging.ErrWorkerDraining) || ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, messaging.ErrRetryScheduled) {
			if !waitRetryDelay(ctx, retry) {
				return nil
			}
			continue
		}
		return err
	}
}

func activeWorkerDeliveries(workers []*messaging.Worker) int {
	total := 0
	for _, worker := range workers {
		total += worker.ActiveDeliveries()
	}
	return total
}

func beginWorkerDrain(workers []*messaging.Worker) {
	for _, worker := range workers {
		worker.BeginDrain()
	}
}

func waitWorkerDrain(ctx context.Context, workers []*messaging.Worker) error {
	var failures []error
	for _, worker := range workers {
		if err := worker.WaitDrain(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
