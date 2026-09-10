// Package redis transports best-effort progress events between service roles.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/progress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

type Config struct {
	Environment string
	Buffer      int
}

// Publisher queues progress updates locally and publishes them from Run. A
// Redis outage or a full queue drops only an ephemeral update; it never feeds
// an error back into the Worker execution path.
type Publisher struct {
	client redisclient.UniversalClient
	config Config
	queue  chan progress.Event
}

func NewPublisher(client redisclient.UniversalClient, config Config) (*Publisher, error) {
	if client == nil || !validSegment(config.Environment) {
		return nil, runtime.ErrInvariantViolation
	}
	if config.Buffer <= 0 {
		config.Buffer = 256
	}
	return &Publisher{client: client, config: config, queue: make(chan progress.Event, config.Buffer)}, nil
}

func (p *Publisher) TryPublish(event progress.Event) {
	if p == nil || !validEvent(event) {
		return
	}
	select {
	case p.queue <- event:
	default:
	}
}

func (p *Publisher) Run(ctx context.Context) error {
	if p == nil || ctx == nil {
		return runtime.ErrInvariantViolation
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-p.queue:
			encoded, err := json.Marshal(event)
			if err != nil {
				continue
			}
			// Progress is explicitly lossy. Do not retry here: delayed tokens
			// after a reconnect are more confusing than a missing delta, and
			// terminal recovery remains the durable Result/Outbox responsibility.
			_ = p.client.Publish(ctx, progressTopic(p.config.Environment, event.TenantID), encoded).Err()
		}
	}
}

// Subscriber creates one bounded Pub/Sub subscription per browser stream.
// It deliberately has no replay API: callers recover completed messages from
// the durable reply endpoint after a disconnect.
type Subscriber struct {
	client redisclient.UniversalClient
	config Config
}

func NewSubscriber(client redisclient.UniversalClient, config Config) (*Subscriber, error) {
	if client == nil || !validSegment(config.Environment) {
		return nil, runtime.ErrInvariantViolation
	}
	return &Subscriber{client: client, config: config}, nil
}

func (s *Subscriber) Subscribe(tenantID string, buffer int) (<-chan progress.Event, func()) {
	if s == nil || tenantID == "" {
		return nil, func() {}
	}
	if buffer <= 0 {
		buffer = 32
	}
	pubsub := s.client.Subscribe(context.Background(), progressTopic(s.config.Environment, tenantID))
	output := make(chan progress.Event, buffer)
	var once sync.Once
	cancel := func() {
		once.Do(func() { _ = pubsub.Close() })
	}
	go func() {
		defer close(output)
		defer cancel()
		for message := range pubsub.Channel() {
			var event progress.Event
			if err := json.Unmarshal([]byte(message.Payload), &event); err != nil || !validEvent(event) || event.TenantID != tenantID {
				continue
			}
			select {
			case output <- event:
			default:
			}
		}
	}()
	return output, cancel
}

func progressTopic(environment, tenantID string) string {
	digest := sha256.Sum256([]byte(tenantID))
	return fmt.Sprintf("trpc:{%s}:progress:%s", environment, hex.EncodeToString(digest[:16]))
}

func validEvent(event progress.Event) bool {
	return event.SchemaVersion == 1 && event.TenantID != "" && event.RequestID != "" && event.Sequence > 0 &&
		(event.Kind == progress.RunStarted || event.Kind == progress.MessageDelta)
}

func validSegment(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

var _ progress.Publisher = (*Publisher)(nil)
