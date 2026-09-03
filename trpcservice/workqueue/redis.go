package workqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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
		consumer = fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), uuid.NewString()[:8])
	}
	queue := &RedisQueue{
		client:       redis.NewClient(redisOptions),
		stream:       prefix + ":stream:" + streamName,
		group:        opts.Group,
		consumer:     consumer,
		blockTimeout: opts.BlockTimeout,
		claimMinIdle: opts.ClaimMinIdle,
		maxLen:       opts.MaxLen,
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := queue.client.Ping(ctx).Err(); err != nil {
		_ = queue.client.Close()
		return nil, fmt.Errorf("ping queue Redis: %w", err)
	}
	if err := queue.client.XGroupCreateMkStream(ctx, queue.stream, queue.group, "0").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
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
	if _, err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		MaxLen: q.maxLen,
		Approx: true,
		Values: map[string]any{"task": string(payload)},
	}).Result(); err != nil {
		return fmt.Errorf("publish Agent task: %w", err)
	}
	return nil
}

func (q *RedisQueue) Receive(ctx context.Context) (Delivery, error) {
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
		return q.delivery(claimed[0])
	}
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: q.consumer,
		Streams:  []string{q.stream, ">"},
		Count:    1,
		Block:    q.blockTimeout,
	}).Result()
	if errors.Is(err, redis.Nil) || (err == nil && len(streams) == 0) {
		return nil, ErrNoMessage
	}
	if err != nil {
		return nil, fmt.Errorf("read Agent task: %w", err)
	}
	for _, stream := range streams {
		if len(stream.Messages) > 0 {
			return q.delivery(stream.Messages[0])
		}
	}
	return nil, ErrNoMessage
}

func (q *RedisQueue) delivery(message redis.XMessage) (Delivery, error) {
	raw, ok := message.Values["task"]
	if !ok {
		return nil, fmt.Errorf("Redis Stream message %s has no task field", message.ID)
	}
	var payload string
	switch value := raw.(type) {
	case string:
		payload = value
	case []byte:
		payload = string(value)
	default:
		return nil, fmt.Errorf("Redis Stream task field has type %T", raw)
	}
	var task AgentTask
	if err := json.Unmarshal([]byte(payload), &task); err != nil {
		return nil, fmt.Errorf("decode Agent task: %w", err)
	}
	return &redisDelivery{queue: q, messageID: message.ID, task: task}, nil
}

func (q *RedisQueue) Ready(ctx context.Context) error {
	if q == nil || q.client == nil {
		return fmt.Errorf("Redis work queue is closed")
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
		q.closeErr = q.client.Close()
	})
	return q.closeErr
}

type redisDelivery struct {
	queue     *RedisQueue
	messageID string
	task      AgentTask
	once      sync.Once
	err       error
}

func (d *redisDelivery) Task() AgentTask { return d.task }

func (d *redisDelivery) Ack(ctx context.Context) error {
	d.once.Do(func() {
		_, d.err = d.queue.client.XAck(
			ctx, d.queue.stream, d.queue.group, d.messageID,
		).Result()
		if d.err != nil {
			d.err = fmt.Errorf("ack Agent task: %w", d.err)
		}
	})
	return d.err
}

func (d *redisDelivery) Retry(ctx context.Context) error {
	d.once.Do(func() {
		task := d.task
		task.Attempt++
		if err := d.queue.Publish(ctx, task); err != nil {
			d.err = err
			return
		}
		_, d.err = d.queue.client.XAck(
			ctx, d.queue.stream, d.queue.group, d.messageID,
		).Result()
		if d.err != nil {
			d.err = fmt.Errorf("ack retried Agent task: %w", d.err)
		}
	})
	return d.err
}

var _ Queue = (*RedisQueue)(nil)
var _ Delivery = (*redisDelivery)(nil)
