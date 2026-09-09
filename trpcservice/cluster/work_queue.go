// Package cluster owns cross-node scheduling and control primitives backed by
// shared Redis and PostgreSQL services.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// StreamMessage is one Redis Stream work item.
type StreamMessage struct {
	ID      string
	Payload []byte
}

// StreamBackend is the minimal reliable consumer-group surface used by the
// scheduler. Implementations must create the stream and make group creation
// idempotent.
type StreamBackend interface {
	CreateConsumerGroup(context.Context, string, string) error
	AddStream(context.Context, string, []byte) error
	ReadGroup(context.Context, string, string, string, string, int64, time.Duration) ([]StreamMessage, error)
	AckStream(context.Context, string, string, string) error
}

// RunHandler executes one durable request after the stream chooses a node.
type RunHandler func(context.Context, gateway.RunRequest) error

// WorkSubmitter is the producer-only side of the shared Redis Stream. Keeping
// it separate from WorkQueue is what lets a Gateway enqueue requests without
// accidentally starting Runner consumers in the same process.
type WorkSubmitter struct {
	backend StreamBackend
	ctx     context.Context
	stream  string
}

// NewWorkSubmitter creates the consumer group before accepting ingress, so
// work published while all Worker processes are down remains available when a
// Worker comes back.
func NewWorkSubmitter(parent context.Context, backend StreamBackend, prefix string) (*WorkSubmitter, error) {
	if parent == nil || backend == nil {
		return nil, errors.New("cluster: submitter context and backend are required")
	}
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	stream, group := prefix+":work", prefix+":workers"
	if err := backend.CreateConsumerGroup(parent, stream, group); err != nil {
		return nil, err
	}
	return &WorkSubmitter{backend: backend, ctx: parent, stream: stream}, nil
}

// Submit appends an immutable claimed request and never consumes it locally.
func (submitter *WorkSubmitter) Submit(request gateway.RunRequest) error {
	if submitter == nil || submitter.backend == nil || request.InboxID == "" || request.ClaimToken == "" {
		return errors.New("cluster: complete claimed request is required")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return errors.New("cluster: encode work request")
	}
	return submitter.backend.AddStream(submitter.ctx, submitter.stream, payload)
}

// WorkQueueConfig bounds Redis stream scheduling and per-node concurrency.
type WorkQueueConfig struct {
	NodeID      string
	Prefix      string
	Concurrency int
	ReadBlock   time.Duration
}

// WorkQueue is a Redis Streams consumer-group scheduler. PostgreSQL Inbox is
// still the source of truth: the stream is the immediate wakeup/delivery path,
// while Inbox lease recovery handles a node dying at any instruction.
type WorkQueue struct {
	backend       StreamBackend
	handler       RunHandler
	config        WorkQueueConfig
	stream        string
	group         string
	ctx           context.Context
	cancel        context.CancelFunc
	handlerCtx    context.Context
	handlerCancel context.CancelFunc
	wg            sync.WaitGroup
	once          sync.Once
}

// NewWorkQueue creates the consumer group and starts this node's consumers.
func NewWorkQueue(parent context.Context, backend StreamBackend, handler RunHandler, config WorkQueueConfig) (*WorkQueue, error) {
	if parent == nil || backend == nil || handler == nil || config.NodeID == "" {
		return nil, errors.New("cluster: queue context, backend, handler, and node id are required")
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 8
	}
	if config.ReadBlock <= 0 {
		config.ReadBlock = time.Second
	}
	prefix := config.Prefix
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	queue := &WorkQueue{backend: backend, handler: handler, config: config, stream: prefix + ":work", group: prefix + ":workers"}
	if err := backend.CreateConsumerGroup(parent, queue.stream, queue.group); err != nil {
		return nil, err
	}
	queue.ctx, queue.cancel = context.WithCancel(parent)
	queue.handlerCtx, queue.handlerCancel = context.WithCancel(parent)
	for index := 0; index < config.Concurrency; index++ {
		queue.wg.Add(1)
		go queue.consume(index)
	}
	return queue, nil
}

// Submit appends an immutable claimed RunRequest to the shared stream.
func (queue *WorkQueue) Submit(request gateway.RunRequest) error {
	if queue == nil || queue.backend == nil || request.InboxID == "" || request.ClaimToken == "" {
		return errors.New("cluster: complete claimed request is required")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return errors.New("cluster: encode work request")
	}
	return queue.backend.AddStream(queue.ctx, queue.stream, payload)
}

func (queue *WorkQueue) consume(index int) {
	defer queue.wg.Done()
	consumer := queue.config.NodeID + ":" + itoa(index)
	// Resume entries that belonged to the same stable node/slot before reading
	// new work. A replacement node with a different ID is recovered by Inbox.
	for queue.ctx.Err() == nil && queue.read(consumer, "0", 0) > 0 {
	}
	for queue.ctx.Err() == nil {
		queue.read(consumer, ">", queue.config.ReadBlock)
	}
}

func (queue *WorkQueue) read(consumer, start string, block time.Duration) int {
	messages, err := queue.backend.ReadGroup(queue.ctx, queue.stream, queue.group, consumer, start, 1, block)
	if err != nil {
		if queue.ctx.Err() == nil {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-queue.ctx.Done():
			case <-timer.C:
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		return 0
	}
	for _, message := range messages {
		var request gateway.RunRequest
		if json.Unmarshal(message.Payload, &request) == nil {
			_ = queue.handler(queue.handlerCtx, request)
		}
		// Processor transitions the exact Inbox claim to completed/retry/DLQ.
		// ACK prevents a stale stream copy from fighting that durable decision.
		ackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = queue.backend.AckStream(ackCtx, queue.stream, queue.group, message.ID)
		cancel()
	}
	return len(messages)
}

// Close stops new consumption and waits for active handlers within ctx.
func (queue *WorkQueue) Close(ctx context.Context) error {
	if queue == nil || ctx == nil {
		return errors.New("cluster: queue and close context are required")
	}
	queue.once.Do(queue.cancel)
	done := make(chan struct{})
	go func() { queue.wg.Wait(); close(done) }()
	select {
	case <-done:
		queue.handlerCancel()
		return nil
	case <-ctx.Done():
		queue.handlerCancel()
		return ctx.Err()
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := [20]byte{}
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
