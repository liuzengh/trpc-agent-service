package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
	stream string
	group  string
}

func NewRedisQueue(ctx context.Context, addr, stream, group string) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	q := &RedisQueue{client: client, stream: stream, group: group}
	if err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil && !stringsContains(err.Error(), "BUSYGROUP") {
		client.Close()
		return nil, fmt.Errorf("create redis consumer group: %w", err)
	}
	return q, nil
}

func (q *RedisQueue) Publish(ctx context.Context, task store.DispatchTask) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return q.client.XAdd(ctx, &redis.XAddArgs{Stream: q.stream, Values: map[string]any{"payload": payload}}).Err()
}

func (q *RedisQueue) Receive(ctx context.Context, consumer string, block time.Duration) (Delivery, error) {
	pending, _, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: q.stream, Group: q.group, Consumer: consumer,
		MinIdle: 30 * time.Second, Start: "0-0", Count: 1,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Delivery{}, err
	}
	if len(pending) > 0 {
		return decodeDelivery(pending[0])
	}
	result, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: q.group, Consumer: consumer, Streams: []string{q.stream, ">"}, Count: 1, Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return Delivery{}, context.DeadlineExceeded
	}
	if err != nil {
		return Delivery{}, err
	}
	if len(result) == 0 || len(result[0].Messages) == 0 {
		return Delivery{}, context.DeadlineExceeded
	}
	return decodeDelivery(result[0].Messages[0])
}

func decodeDelivery(msg redis.XMessage) (Delivery, error) {
	raw, ok := msg.Values["payload"]
	if !ok {
		return Delivery{}, errors.New("redis delivery has no payload")
	}
	var payload []byte
	switch value := raw.(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
	default:
		return Delivery{}, fmt.Errorf("unsupported redis payload type %T", raw)
	}
	var task store.DispatchTask
	if err := json.Unmarshal(payload, &task); err != nil {
		return Delivery{}, err
	}
	return Delivery{ID: msg.ID, Task: task}, nil
}

func (q *RedisQueue) Ack(ctx context.Context, delivery Delivery) error {
	return q.client.XAck(ctx, q.stream, q.group, delivery.ID).Err()
}

func (q *RedisQueue) Retry(ctx context.Context, delivery Delivery) error {
	delivery.Task.Attempts++
	if err := q.Publish(ctx, delivery.Task); err != nil {
		return err
	}
	return q.Ack(ctx, delivery)
}

func (q *RedisQueue) Dead(ctx context.Context, delivery Delivery, cause error) error {
	payload, err := json.Marshal(delivery.Task)
	if err != nil {
		return err
	}
	if err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream + ":dlq",
		Values: map[string]any{"payload": payload, "cause": errorText(cause), "source_id": delivery.ID},
	}).Err(); err != nil {
		return err
	}
	return q.Ack(ctx, delivery)
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 512 {
		return value[:512]
	}
	return value
}

func (q *RedisQueue) Close() error { return q.client.Close() }

type RedisLocker struct{ client *redis.Client }

func NewRedisLocker(client *redis.Client) *RedisLocker { return &RedisLocker{client: client} }
func (q *RedisQueue) Locker() *RedisLocker             { return NewRedisLocker(q.client) }

func (l *RedisLocker) Acquire(ctx context.Context, key string, ttl time.Duration) (Lease, error) {
	owner := uuid.NewString()
	lockKey := "trpc:lock:" + key
	ok, err := l.client.SetNX(ctx, lockKey, owner, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLeaseBusy
	}
	fence, err := l.client.Incr(ctx, "trpc:fence:"+key).Result()
	if err != nil {
		l.client.Del(ctx, lockKey)
		return nil, err
	}
	return &redisLease{client: l.client, key: lockKey, owner: owner, fence: fence}, nil
}

type redisLease struct {
	client *redis.Client
	key    string
	owner  string
	fence  int64
}

func (l *redisLease) Fence() int64 { return l.fence }

func (l *redisLease) Renew(ctx context.Context, ttl time.Duration) error {
	result, err := renewScript.Run(ctx, l.client, []string{l.key}, l.owner, strconv.FormatInt(ttl.Milliseconds(), 10)).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrLeaseBusy
	}
	return nil
}

func (l *redisLease) Release(ctx context.Context) error {
	_, err := releaseScript.Run(ctx, l.client, []string{l.key}, l.owner).Result()
	return err
}

var releaseScript = redis.NewScript(`if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end`)
var renewScript = redis.NewScript(`if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('pexpire',KEYS[1],ARGV[2]) else return 0 end`)

func stringsContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
