// Package broker defines at-least-once execution delivery.
package broker

import (
	"context"
	"hash/fnv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func ShardForSession(tenantID, agentAppID, sessionID string, shardCount uint32) (Shard, error) {
	if tenantID == "" || agentAppID == "" || sessionID == "" || shardCount == 0 {
		return 0, runtime.ErrInvariantViolation
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(tenantID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(agentAppID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(sessionID))
	return Shard(hash.Sum32() % shardCount), nil
}

type Shard uint32
type ConsumerOptions struct {
	ConsumerID string
	Shards     []Shard
}
type ReclaimOptions struct {
	ConsumerID string
	Limit      int
}
type Delivery struct {
	ID       string
	Shard    Shard
	Envelope runtime.ExecutionEnvelope
}

type Broker interface {
	Publish(context.Context, Shard, runtime.ExecutionEnvelope) error
	Consume(context.Context, ConsumerOptions, func(context.Context, Delivery) error) error
	Ack(context.Context, Delivery) error
	Reclaim(context.Context, ReclaimOptions) ([]Delivery, error)
}
