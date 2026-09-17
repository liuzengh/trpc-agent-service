package delivery

import (
	"context"
	"sort"
	"sync"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type DestinationCatalog interface {
	ListDeliveryDestinations(context.Context) ([]channel.ReplyDestination, error)
}

type ConsumerRunner interface{ Run(context.Context) error }
type ConsumerFactory func(channel.ReplyDestination) (ConsumerRunner, error)

type Supervisor struct {
	Catalog         DestinationCatalog
	NewConsumer     ConsumerFactory
	RefreshInterval time.Duration
	OnError         func(error)
}

type runningConsumer struct {
	cancel     context.CancelFunc
	generation uint64
}

type consumerExit struct {
	key        string
	generation uint64
	err        error
}

func (s Supervisor) Run(ctx context.Context) error {
	if ctx == nil || s.Catalog == nil || s.NewConsumer == nil || s.RefreshInterval <= 0 {
		return runtime.ErrInvariantViolation
	}
	runCtx, cancel := context.WithCancel(ctx)
	var consumers sync.WaitGroup
	defer func() { cancel(); consumers.Wait() }()
	active := make(map[string]runningConsumer)
	exits := make(chan consumerExit)
	var generation uint64
	syncCatalog := func() error {
		destinations, err := s.Catalog.ListDeliveryDestinations(runCtx)
		if err != nil {
			return err
		}
		desired := make(map[string]channel.ReplyDestination, len(destinations))
		for _, destination := range destinations {
			key, err := deliveryDestinationKey(destination)
			if err != nil {
				return err
			}
			if _, exists := desired[key]; exists {
				return runtime.ErrIdempotencyCollision
			}
			desired[key] = destination
		}
		for key, current := range active {
			if _, keep := desired[key]; !keep {
				current.cancel()
				delete(active, key)
			}
		}
		keys := make([]string, 0, len(desired))
		for key := range desired {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, exists := active[key]; exists {
				continue
			}
			runner, err := s.NewConsumer(desired[key])
			if err != nil {
				return err
			}
			generation++
			consumerCtx, consumerCancel := context.WithCancel(runCtx)
			current := runningConsumer{cancel: consumerCancel, generation: generation}
			active[key] = current
			consumers.Add(1)
			go func(key string, generation uint64) {
				defer consumers.Done()
				err := runner.Run(consumerCtx)
				select {
				case exits <- consumerExit{key: key, generation: generation, err: err}:
				case <-runCtx.Done():
				}
			}(key, generation)
		}
		return nil
	}
	if err := syncCatalog(); err != nil {
		return err
	}
	ticker := time.NewTicker(s.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			for _, current := range active {
				current.cancel()
			}
			return runCtx.Err()
		case exited := <-exits:
			if current, ok := active[exited.key]; ok && current.generation == exited.generation {
				delete(active, exited.key)
				if exited.err != nil && s.OnError != nil {
					s.OnError(exited.err)
				}
			}
		case <-ticker.C:
			if err := syncCatalog(); err != nil && s.OnError != nil {
				s.OnError(err)
			}
		}
	}
}

func deliveryDestinationKey(value channel.ReplyDestination) (string, error) {
	if value.TenantID == "" || value.Channel == "" || value.ChannelBindingID == "" || value.ExternalAccountID == "" || value.ConfigVersion != 0 {
		return "", runtime.ErrInvalidEnvelope
	}
	return value.TenantID + "\x00" + value.Channel + "\x00" + value.ChannelBindingID + "\x00" + value.ExternalAccountID, nil
}
