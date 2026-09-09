// Package queuebench measures the real reliable queue independently of models
// and IM providers. It uses a finite, preloaded backlog, not an arrival-rate
// generator, and makes no production throughput or latency claims.
package queuebench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

type Config struct {
	MessagesPerTenant int           `json:"messages_per_tenant"`
	Consumers         []int         `json:"consumers"`
	MaxConnections    int           `json:"workload_pool_connections"`
	MaxInflight       int           `json:"fair_max_inflight_per_tenant"`
	WorkDuration      time.Duration `json:"synthetic_work_nanoseconds"`
	Timeout           time.Duration `json:"timeout_nanoseconds"`
}

func DefaultConfig() Config {
	return Config{MessagesPerTenant: 64, Consumers: []int{1, 4, 8, 16}, MaxConnections: 16, MaxInflight: 4, WorkDuration: time.Millisecond, Timeout: 5 * time.Minute}
}

func (c Config) Validate() error {
	if c.MessagesPerTenant < 16 || c.MessagesPerTenant > 2000 || c.MaxConnections < 1 || c.MaxConnections > 64 ||
		c.MaxInflight < 1 || c.MaxInflight > 32 || c.WorkDuration < 0 || c.WorkDuration > 100*time.Millisecond ||
		c.Timeout < time.Second || c.Timeout > 30*time.Minute || len(c.Consumers) == 0 || len(c.Consumers) > 8 {
		return errors.New("queue benchmark configuration is outside its bounded range")
	}
	seen := make(map[int]bool)
	for _, n := range c.Consumers {
		if n < 1 || n > 32 || seen[n] {
			return errors.New("consumer counts must be distinct values in 1..32")
		}
		seen[n] = true
	}
	return nil
}

type Queue interface {
	EnqueueInboxWithAdmission(context.Context, *reliable.InboxMessage) (bool, error)
	ClaimInbox(context.Context, string, time.Duration) (*reliable.InboxMessage, error)
	ClaimInboxFair(context.Context, string, time.Duration) (*reliable.InboxMessage, error)
	CompleteInbox(context.Context, int64, reliable.Lease, reliable.OutboxReply) (*reliable.OutboxMessage, error)
	UpsertTenantQueuePolicy(context.Context, reliable.TenantQueuePolicy) error
}

type Percentiles struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
}

type TenantResult struct {
	ID              string      `json:"tenant_id"`
	Weight          int64       `json:"weight"`
	Completed       int         `json:"completed"`
	FirstHalfClaims int         `json:"first_half_claims"`
	Completion      Percentiles `json:"completion_since_drain_start"`
}

type Result struct {
	Scenario           string         `json:"scenario"`
	Mode               string         `json:"mode"`
	Consumers          int            `json:"consumers"`
	Messages           int            `json:"messages"`
	Completed          int            `json:"completed"`
	DuplicateClaims    int            `json:"duplicate_claims"`
	DuplicateEnqueues  int            `json:"duplicate_enqueues_rejected"`
	ElapsedSeconds     float64        `json:"drain_seconds"`
	MessagesPerSecond  float64        `json:"completed_per_second"`
	Claim              Percentiles    `json:"claim_call"`
	Processing         Percentiles    `json:"claim_to_complete"`
	Completion         Percentiles    `json:"completion_since_drain_start"`
	Tenants            []TenantResult `json:"tenants"`
	PoolWaitCount      int64          `json:"pool_wait_count"`
	PoolWaitSeconds    float64        `json:"pool_wait_seconds"`
	PoolInUseAfter     int            `json:"pool_in_use_after"`
	PersistedCompleted int            `json:"persisted_completed"`
	PersistedReplies   int            `json:"persisted_unique_outbox"`
	PersistedInflight  int            `json:"persisted_inflight_after"`
	Verified           bool           `json:"verified"`
}

func distribution(values []time.Duration) Percentiles {
	if len(values) == 0 {
		return Percentiles{}
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	at := func(p float64) float64 {
		return float64(ordered[int(math.Ceil(p*float64(len(ordered))))-1]) / float64(time.Millisecond)
	}
	return Percentiles{P50: at(.5), P95: at(.95), P99: at(.99)}
}

func validTenantIDs(ids []string) bool {
	return len(ids) == 2 && ids[0] != ids[1] &&
		strings.HasPrefix(ids[0], "queuebench-") && strings.HasPrefix(ids[1], "queuebench-") &&
		len(ids[0]) <= 64 && len(ids[1]) <= 64
}

func enqueue(ctx context.Context, q Queue, ids []string, count int, scenario string, maxInflight int) (int, error) {
	duplicates := 0
	for index, id := range ids {
		weight := int64(1)
		if index == 0 && scenario != "uniform" {
			weight = 4
		}
		if err := q.UpsertTenantQueuePolicy(ctx, reliable.TenantQueuePolicy{TenantID: id, Weight: weight, MaxInflight: int64(maxInflight)}); err != nil {
			return duplicates, err
		}
	}
	// Interleave ingress so global FIFO is not biased by fixture tenant order.
	for i := range count {
		for index, id := range ids {
			session := fmt.Sprintf("session-%d", i)
			if scenario == "weighted_hotspot" && index == 0 && i%4 != 0 {
				session = "hot-session"
			}
			payload := []byte(fmt.Sprintf(`{"content":"queuebench-%d"}`, i))
			hash := sha256.Sum256(payload)
			message := &reliable.InboxMessage{
				TenantID: id, ChannelType: "local", ChannelAccountID: "queuebench", AgentApp: "queuebench",
				ExternalMessageID: fmt.Sprintf("message-%d", i), ConversationID: session,
				UserID: "synthetic-user", SessionID: session, SessionOwnerID: "synthetic-user", RoutingVersion: 1,
				Payload: payload, PayloadHash: hex.EncodeToString(hash[:]), MaxAttempts: 2,
			}
			inserted, err := q.EnqueueInboxWithAdmission(ctx, message)
			if err != nil || !inserted {
				return duplicates, fmt.Errorf("enqueue did not insert a fresh benchmark message: %w", err)
			}
			if i == 0 {
				copy := *message
				inserted, err := q.EnqueueInboxWithAdmission(ctx, &copy)
				if err != nil || inserted || copy.ID != message.ID {
					return duplicates, errors.New("duplicate enqueue changed the persisted identity")
				}
				duplicates++
			}
		}
	}
	return duplicates, nil
}

// Execute measures drain time after preload. The first-half distribution is
// descriptive: quotas and hot Session serialization can constrain a 4:1 weight.
func Execute(ctx context.Context, q Queue, c Config, ids []string, scenario, mode string, consumers int) (Result, error) {
	result := Result{Scenario: scenario, Mode: mode, Consumers: consumers, Messages: 2 * c.MessagesPerTenant}
	if err := c.Validate(); err != nil {
		return result, err
	}
	if !validTenantIDs(ids) || consumers < 1 || consumers > 32 ||
		(scenario != "uniform" && scenario != "weighted" && scenario != "weighted_hotspot") || (mode != "ordinary" && mode != "fair") {
		return result, errors.New("invalid benchmark case or tenant scope")
	}
	duplicates, err := enqueue(ctx, q, ids, c.MessagesPerTenant, scenario, c.MaxInflight)
	result.DuplicateEnqueues = duplicates
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	claim := q.ClaimInbox
	if mode == "fair" {
		claim = q.ClaimInboxFair
	}
	start := time.Now()
	var completed atomic.Int64
	var mu sync.Mutex
	var firstErr error
	var claimTimes, processingTimes, completionTimes []time.Duration
	seen := make(map[int64]bool)
	tenantTimes := make(map[string][]time.Duration)
	firstHalf := make(map[string]int)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}
	var wg sync.WaitGroup
	for index := range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner := fmt.Sprintf("%s-consumer-%d", ids[0], index)
			for completed.Load() < int64(result.Messages) {
				if ctx.Err() != nil {
					return
				}
				callStart := time.Now()
				message, err := claim(ctx, owner, time.Minute)
				claimedAt := time.Now()
				if errors.Is(err, reliable.ErrNoWork) {
					if err := pause(ctx, time.Millisecond); err != nil {
						return
					}
					continue
				}
				if err != nil {
					fail(err)
					return
				}
				if message == nil || (message.TenantID != ids[0] && message.TenantID != ids[1]) {
					fail(errors.New("claim escaped benchmark tenant scope"))
					return
				}
				mu.Lock()
				duplicate := seen[message.ID]
				if duplicate {
					result.DuplicateClaims++
				}
				seen[message.ID] = true
				if len(seen) <= result.Messages/2 {
					firstHalf[message.TenantID]++
				}
				mu.Unlock()
				if duplicate || message.AttemptCount != 1 {
					fail(errors.New("benchmark message was claimed more than once"))
					return
				}
				if err := pause(ctx, c.WorkDuration); err != nil {
					return
				}
				reply, err := q.CompleteInbox(ctx, message.ID, message.Lease, reliable.OutboxReply{ContentType: "text", Content: "queuebench-complete"})
				if err != nil {
					fail(err)
					return
				}
				if reply == nil || reply.InboxID != message.ID || reply.TenantID != message.TenantID {
					fail(errors.New("completed reply lost its authoritative Inbox identity"))
					return
				}
				finished := time.Now()
				mu.Lock()
				claimTimes = append(claimTimes, claimedAt.Sub(callStart))
				processingTimes = append(processingTimes, finished.Sub(claimedAt))
				completionTimes = append(completionTimes, finished.Sub(start))
				tenantTimes[message.TenantID] = append(tenantTimes[message.TenantID], finished.Sub(start))
				mu.Unlock()
				completed.Add(1)
			}
		}()
	}
	wg.Wait()
	result.ElapsedSeconds = time.Since(start).Seconds()
	result.Completed = int(completed.Load())
	result.MessagesPerSecond = float64(result.Completed) / result.ElapsedSeconds
	result.Claim, result.Processing, result.Completion = distribution(claimTimes), distribution(processingTimes), distribution(completionTimes)
	for index, id := range ids {
		weight := int64(1)
		if index == 0 && scenario != "uniform" {
			weight = 4
		}
		result.Tenants = append(result.Tenants, TenantResult{ID: id, Weight: weight, Completed: len(tenantTimes[id]), FirstHalfClaims: firstHalf[id], Completion: distribution(tenantTimes[id])})
	}
	if firstErr != nil {
		return result, firstErr
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if result.Completed != result.Messages {
		return result, errors.New("benchmark did not drain its finite backlog")
	}
	return result, nil
}

func pause(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
