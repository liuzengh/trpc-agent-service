package workqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

type RedisOptions struct {
	URL          string
	KeyPrefix    string
	Stream       string
	Group        string
	Consumer     string
	BlockTimeout time.Duration
	ClaimMinIdle time.Duration
	MaxLen       int64
}

// RedisQueue uses a Redis Streams consumer group with pending-message reclaim.
type RedisQueue struct {
	client       *redis.Client
	stream       string
	group        string
	consumer     string
	blockTimeout time.Duration
	claimMinIdle time.Duration
	maxLen       int64
	mu           sync.Mutex
	active       map[string]*redisDelivery
	closed       bool
	stop         context.CancelFunc
	lifetime     context.Context
	workers      sync.WaitGroup
	closeOnce    sync.Once
	closeErr     error
}

func NewRedisQueue(ctx context.Context, opts RedisOptions) (*RedisQueue, error) {
	redisOptions, err := redis.ParseURL(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("parse queue Redis URL: %w", err)
	}
	prefix := strings.Trim(strings.TrimSpace(opts.KeyPrefix), ":")
	streamName := strings.Trim(strings.TrimSpace(opts.Stream), ":")
	if prefix == "" || streamName == "" || strings.TrimSpace(opts.Group) == "" {
		return nil, fmt.Errorf("queue Redis prefix, stream and group are required")
	}
	if opts.BlockTimeout <= 0 || opts.ClaimMinIdle <= 0 || opts.MaxLen <= 0 {
		return nil, fmt.Errorf("queue timeouts and max length must be positive")
	}
	consumer := strings.TrimSpace(opts.Consumer)
	if consumer == "" {
		hostname, _ := os.Hostname()
		consumer = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}
	// Configured names are prefixes, not reusable ownership tokens. Two
	// processes configured alike must never acknowledge one another's work.
	consumer += "-" + uuid.NewString()
	lifetime, stop := context.WithCancel(context.Background())
	queue := &RedisQueue{
		client:       redis.NewClient(redisOptions),
		stream:       prefix + ":stream:" + streamName,
		group:        opts.Group,
		consumer:     consumer,
		blockTimeout: opts.BlockTimeout,
		claimMinIdle: opts.ClaimMinIdle,
		maxLen:       opts.MaxLen,
		active:       make(map[string]*redisDelivery),
		lifetime:     lifetime,
		stop:         stop,
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := queue.client.Ping(ctx).Err(); err != nil {
		stop()
		_ = queue.client.Close()
		return nil, fmt.Errorf("ping queue Redis: %w", err)
	}
	if err := initializeStream.Run(ctx, queue.client, []string{queue.stream}, queue.group).Err(); err != nil {
		stop()
		_ = queue.client.Close()
		return nil, fmt.Errorf("create Redis Stream group: %w", err)
	}
	return queue, nil
}

func (q *RedisQueue) Publish(ctx context.Context, task AgentTask) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal Agent task: %w", err)
	}
	added, err := publishTask.Run(ctx, q.client, []string{q.stream}, q.group, q.maxLen, string(payload)).Int()
	if err != nil {
		return fmt.Errorf("publish Agent task: %w", err)
	}
	if added != 1 {
		return ErrQueueFull
	}
	return nil
}

func (q *RedisQueue) Receive(ctx context.Context) (Delivery, error) {
	return q.receive(ctx, q.blockTimeout)
}

func (q *RedisQueue) TryReceive(ctx context.Context) (Delivery, error) {
	return q.receive(ctx, -1)
}

func (q *RedisQueue) receive(ctx context.Context, block time.Duration) (Delivery, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	claimed, _, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.stream,
		Group:    q.group,
		Consumer: q.consumer,
		MinIdle:  q.claimMinIdle,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("claim pending Agent task: %w", err)
	}
	if len(claimed) > 0 {
		return q.delivery(ctx, claimed[0])
	}
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: q.consumer,
		Streams:  []string{q.stream, ">"},
		Count:    1,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) || (err == nil && len(streams) == 0) {
		return nil, ErrNoMessage
	}
	if err != nil {
		return nil, fmt.Errorf("read Agent task: %w", err)
	}
	for _, stream := range streams {
		if len(stream.Messages) > 0 {
			return q.delivery(ctx, stream.Messages[0])
		}
	}
	return nil, ErrNoMessage
}

func (q *RedisQueue) delivery(parent context.Context, message redis.XMessage) (Delivery, error) {
	raw, ok := message.Values["task"]
	if !ok {
		return nil, fmt.Errorf("redis Stream message %s has no task field", message.ID)
	}
	var payload string
	switch value := raw.(type) {
	case string:
		payload = value
	case []byte:
		payload = string(value)
	default:
		return nil, fmt.Errorf("redis Stream task field has type %T", raw)
	}
	var task AgentTask
	if err := json.Unmarshal([]byte(payload), &task); err != nil {
		return nil, fmt.Errorf("decode Agent task: %w", err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, errors.New("redis work queue is closed")
	}
	if _, exists := q.active[message.ID]; exists {
		return nil, ErrNoMessage
	}
	ctx, cancel := context.WithCancelCause(parent)
	d := &redisDelivery{queue: q, messageID: message.ID, task: task, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	q.active[message.ID] = d
	q.workers.Add(1)
	go d.renew()
	return d, nil
}

func (q *RedisQueue) Ready(ctx context.Context) error {
	if q == nil || q.client == nil {
		return fmt.Errorf("redis work queue is closed")
	}
	if err := q.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping queue Redis: %w", err)
	}
	return nil
}

func (q *RedisQueue) Close() error {
	if q == nil {
		return nil
	}
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.stop()
		q.mu.Unlock()
		q.closeErr = q.client.Close()
		q.workers.Wait()
	})
	return q.closeErr
}

type redisDelivery struct {
	queue      *RedisQueue
	messageID  string
	task       AgentTask
	once       sync.Once
	err        error
	ctx        context.Context
	cancel     context.CancelCauseFunc
	done       chan struct{}
	finalizing atomic.Bool
}

func (d *redisDelivery) Task() AgentTask          { return d.task }
func (d *redisDelivery) Context() context.Context { return d.ctx }
func (d *redisDelivery) Close()                   { d.cancel(context.Canceled); <-d.done }

var ErrQueueFull = errors.New("agent queue capacity reached; retain task in durable outbox")
var ErrDeliveryOwnership = errors.New("agent queue delivery ownership lost")

func (d *redisDelivery) renew() {
	defer d.queue.workers.Done()
	defer close(d.done)
	defer func() {
		d.queue.mu.Lock()
		delete(d.queue.active, d.messageID)
		d.queue.mu.Unlock()
	}()
	interval := max(d.queue.claimMinIdle/3, time.Millisecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-d.queue.lifetime.Done():
			d.cancel(context.Canceled)
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(d.ctx, min(interval, 2*time.Second))
			owned, err := renewDelivery.Run(ctx, d.queue.client, []string{d.queue.stream}, d.queue.group, d.queue.consumer, d.messageID).Int()
			cancel()
			if err != nil || owned != 1 {
				if d.finalizing.Load() {
					return
				}
				d.cancel(ErrDeliveryOwnership)
				return
			}
		}
	}
}

func (d *redisDelivery) Ack(ctx context.Context) error {
	d.once.Do(func() {
		d.finalizing.Store(true)
		defer d.Close()
		var owned int
		owned, d.err = acknowledgeDelivery.Run(ctx, d.queue.client, []string{d.queue.stream}, d.queue.group, d.queue.consumer, d.messageID).Int()
		if d.err == nil && owned != 1 {
			d.err = ErrDeliveryOwnership
		}
		if d.err != nil {
			d.err = fmt.Errorf("ack Agent task: %w", d.err)
		}
	})
	return d.err
}

func (d *redisDelivery) Retry(ctx context.Context) error {
	d.once.Do(func() {
		d.finalizing.Store(true)
		defer d.Close()
		task := d.task
		task.Attempt++
		payload, err := json.Marshal(task)
		if err != nil {
			d.err = err
			return
		}
		var owned int
		owned, d.err = retryDelivery.Run(ctx, d.queue.client, []string{d.queue.stream}, d.queue.group, d.queue.consumer, d.messageID, string(payload)).Int()
		if d.err == nil && owned != 1 {
			d.err = ErrDeliveryOwnership
		}
		if d.err != nil {
			d.err = fmt.Errorf("ack retried Agent task: %w", d.err)
		}
	})
	return d.err
}

var _ Queue = (*RedisQueue)(nil)
var _ Delivery = (*redisDelivery)(nil)
