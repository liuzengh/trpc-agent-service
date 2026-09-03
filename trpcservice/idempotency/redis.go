package idempotency

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

const beginStartedMarker = "__TRPC_AGENT_IDEMPOTENCY_STARTED__"

var beginScript = redis.NewScript(`
local current = redis.call("get", KEYS[1])
if not current then
  redis.call("psetex", KEYS[1], ARGV[2], ARGV[1])
  return ARGV[3]
end
return current
`)

var replaceIfOwnedScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  redis.call("psetex", KEYS[1], ARGV[3], ARGV[2])
  return 1
end
return 0
`)

var renewIfOwnedScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`)

var deleteIfOwnedScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
`)

// RedisOptions configures distributed idempotency records.
type RedisOptions struct {
	URL           string
	KeyPrefix     string
	ProcessingTTL time.Duration
	CompletedTTL  time.Duration
	RenewInterval time.Duration
	PollInterval  time.Duration
}

type redisRecord struct {
	Status      string `json:"status"`
	Fingerprint string `json:"fingerprint"`
	Owner       string `json:"owner,omitempty"`
	Result      Result `json:"result,omitempty"`
	StartedAt   int64  `json:"started_at,omitempty"`
	CompletedAt int64  `json:"completed_at,omitempty"`
}

// RedisStore deduplicates message IDs across Agent Worker processes.
type RedisStore struct {
	client        *redis.Client
	keyPrefix     string
	processingTTL time.Duration
	completedTTL  time.Duration
	renewInterval time.Duration
	pollInterval  time.Duration
	closeCtx      context.Context
	closeCancel   context.CancelCauseFunc
	closeOnce     sync.Once
	closeErr      error
}

// NewRedisStore creates a Redis idempotency store without probing it.
func NewRedisStore(opts RedisOptions) (*RedisStore, error) {
	redisOpts, err := redis.ParseURL(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("parse idempotency Redis URL: %w", err)
	}
	if opts.ProcessingTTL < time.Millisecond || opts.CompletedTTL < time.Millisecond {
		return nil, fmt.Errorf("idempotency TTL values must be at least 1ms")
	}
	if opts.RenewInterval <= 0 || opts.RenewInterval > opts.ProcessingTTL/2 {
		return nil, fmt.Errorf("idempotency renew interval must be positive and at most half of processing TTL")
	}
	if opts.PollInterval <= 0 {
		return nil, fmt.Errorf("idempotency poll interval must be positive")
	}
	prefix := strings.Trim(strings.TrimSpace(opts.KeyPrefix), ":")
	if prefix == "" {
		return nil, fmt.Errorf("idempotency Redis key prefix is required")
	}
	closeCtx, closeCancel := context.WithCancelCause(context.Background())
	return &RedisStore{
		client:        redis.NewClient(redisOpts),
		keyPrefix:     prefix,
		processingTTL: opts.ProcessingTTL,
		completedTTL:  opts.CompletedTTL,
		renewInterval: opts.RenewInterval,
		pollInterval:  opts.PollInterval,
		closeCtx:      closeCtx,
		closeCancel:   closeCancel,
	}, nil
}

// Begin atomically creates one processing record or returns the current one.
func (s *RedisStore) Begin(
	ctx context.Context,
	key Key,
	fingerprint string,
) (BeginResult, error) {
	if err := validateBeginInput(ctx, key, fingerprint); err != nil {
		return BeginResult{}, err
	}
	if cause := context.Cause(s.closeCtx); cause != nil {
		return BeginResult{}, cause
	}
	owner, err := randomID()
	if err != nil {
		return BeginResult{}, err
	}
	record := redisRecord{
		Status:      "processing",
		Fingerprint: fingerprint,
		Owner:       owner,
		StartedAt:   time.Now().UTC().UnixMilli(),
	}
	processingJSON, err := json.Marshal(record)
	if err != nil {
		return BeginResult{}, fmt.Errorf("marshal idempotency processing record: %w", err)
	}
	redisKey := s.redisKey(key)
	value, err := beginScript.Run(
		ctx,
		s.client,
		[]string{redisKey},
		string(processingJSON),
		s.processingTTL.Milliseconds(),
		beginStartedMarker,
	).Text()
	if err != nil {
		return BeginResult{}, fmt.Errorf("begin Redis idempotency attempt: %w", err)
	}
	if value == beginStartedMarker {
		return BeginResult{
			Status:  BeginStarted,
			Attempt: s.newAttempt(ctx, redisKey, string(processingJSON)),
		}, nil
	}
	return beginResultFromRecord(value, fingerprint)
}

// Wait polls the shared record until it completes, disappears or is cancelled.
func (s *RedisStore) Wait(
	ctx context.Context,
	key Key,
	fingerprint string,
) (Result, error) {
	if err := validateBeginInput(ctx, key, fingerprint); err != nil {
		return Result{}, err
	}
	redisKey := s.redisKey(key)
	for {
		if cause := context.Cause(s.closeCtx); cause != nil {
			return Result{}, cause
		}
		value, err := s.client.Get(ctx, redisKey).Result()
		if err == redis.Nil {
			return Result{}, ErrRetry
		}
		if err != nil {
			return Result{}, fmt.Errorf("read Redis idempotency record: %w", err)
		}
		begin, err := beginResultFromRecord(value, fingerprint)
		if err != nil {
			return Result{}, err
		}
		if begin.Status == BeginCompleted {
			return begin.Result, nil
		}

		timer := time.NewTimer(s.pollInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			stopAndDrainTimer(timer)
			return Result{}, context.Cause(ctx)
		case <-s.closeCtx.Done():
			stopAndDrainTimer(timer)
			return Result{}, ErrStoreClosed
		}
	}
}

func beginResultFromRecord(value string, fingerprint string) (BeginResult, error) {
	var record redisRecord
	if err := json.Unmarshal([]byte(value), &record); err != nil {
		return BeginResult{}, fmt.Errorf("decode Redis idempotency record: %w", err)
	}
	if record.Fingerprint != fingerprint {
		return BeginResult{}, ErrKeyConflict
	}
	switch record.Status {
	case "processing":
		return BeginResult{Status: BeginProcessing}, nil
	case "completed":
		return BeginResult{Status: BeginCompleted, Result: record.Result}, nil
	default:
		return BeginResult{}, fmt.Errorf("unknown Redis idempotency status %q", record.Status)
	}
}

func (s *RedisStore) newAttempt(
	ctx context.Context,
	redisKey string,
	processingJSON string,
) *redisAttempt {
	attemptCtx, attemptCancel := context.WithCancelCause(ctx)
	attempt := &redisAttempt{
		ctx:            attemptCtx,
		cancel:         attemptCancel,
		client:         s.client,
		redisKey:       redisKey,
		processingJSON: processingJSON,
		processingTTL:  s.processingTTL,
		completedTTL:   s.completedTTL,
		renewInterval:  s.renewInterval,
		renewDone:      make(chan struct{}),
	}
	attempt.stopCloseCallback = context.AfterFunc(s.closeCtx, func() {
		attempt.cancel(ErrStoreClosed)
	})
	go attempt.renewLoop()
	return attempt
}

func (s *RedisStore) redisKey(key Key) string {
	digestInput := fmt.Sprintf(
		"%d:%s|%d:%s|%d:%s|%d:%s",
		len(key.AppName), key.AppName,
		len(key.UserID), key.UserID,
		len(key.SessionID), key.SessionID,
		len(key.MessageID), key.MessageID,
	)
	digest := sha256.Sum256([]byte(digestInput))
	return s.keyPrefix + ":idempotency:message:" + hex.EncodeToString(digest[:])
}

// Ready checks Redis connectivity.
func (s *RedisStore) Ready(ctx context.Context) error {
	if s == nil || s.client == nil {
		return ErrStoreClosed
	}
	if cause := context.Cause(s.closeCtx); cause != nil {
		return cause
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping idempotency Redis: %w", err)
	}
	return nil
}

// Close cancels active attempts and closes the owned Redis client.
func (s *RedisStore) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.closeCancel(ErrStoreClosed)
		if s.client != nil {
			s.closeErr = s.client.Close()
		}
	})
	return s.closeErr
}

type redisAttempt struct {
	ctx               context.Context
	cancel            context.CancelCauseFunc
	stopCloseCallback func() bool
	client            *redis.Client
	redisKey          string
	processingJSON    string
	processingTTL     time.Duration
	completedTTL      time.Duration
	renewInterval     time.Duration
	renewDone         chan struct{}
	finishOnce        sync.Once
	finishErr         error
}

func (a *redisAttempt) Context() context.Context {
	return a.ctx
}

func (a *redisAttempt) Complete(ctx context.Context, result Result) error {
	return a.finish(ctx, &result)
}

func (a *redisAttempt) Fail(ctx context.Context) error {
	return a.finish(ctx, nil)
}

func (a *redisAttempt) finish(ctx context.Context, result *Result) error {
	if a == nil {
		return nil
	}
	a.finishOnce.Do(func() {
		if a.stopCloseCallback != nil {
			a.stopCloseCallback()
		}
		a.cancel(nil)
		<-a.renewDone
		if ctx == nil {
			ctx = context.Background()
		}
		if result == nil {
			changed, err := deleteIfOwnedScript.Run(
				ctx,
				a.client,
				[]string{a.redisKey},
				a.processingJSON,
			).Int64()
			a.finishErr = finishRedisAttemptResult("fail", changed, err)
			return
		}
		var processing redisRecord
		if err := json.Unmarshal([]byte(a.processingJSON), &processing); err != nil {
			a.finishErr = fmt.Errorf("decode owned idempotency record: %w", err)
			return
		}
		completedJSON, err := json.Marshal(redisRecord{
			Status:      "completed",
			Fingerprint: processing.Fingerprint,
			Result:      *result,
			CompletedAt: time.Now().UTC().UnixMilli(),
		})
		if err != nil {
			a.finishErr = fmt.Errorf("marshal completed idempotency record: %w", err)
			return
		}
		changed, err := replaceIfOwnedScript.Run(
			ctx,
			a.client,
			[]string{a.redisKey},
			a.processingJSON,
			string(completedJSON),
			a.completedTTL.Milliseconds(),
		).Int64()
		a.finishErr = finishRedisAttemptResult("complete", changed, err)
	})
	return a.finishErr
}

func finishRedisAttemptResult(operation string, changed int64, err error) error {
	if err != nil {
		return fmt.Errorf("%s Redis idempotency attempt: %w", operation, err)
	}
	if changed != 1 {
		return ErrAttemptLost
	}
	return nil
}

func (a *redisAttempt) renewLoop() {
	defer close(a.renewDone)
	ticker := time.NewTicker(a.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			changed, err := renewIfOwnedScript.Run(
				a.ctx,
				a.client,
				[]string{a.redisKey},
				a.processingJSON,
				a.processingTTL.Milliseconds(),
			).Int64()
			if err != nil {
				if a.ctx.Err() == nil {
					a.cancel(fmt.Errorf("%w: renew Redis idempotency attempt: %v", ErrAttemptLost, err))
				}
				return
			}
			if changed != 1 {
				a.cancel(ErrAttemptLost)
				return
			}
		case <-a.ctx.Done():
			return
		}
	}
}

func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate idempotency owner: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func stopAndDrainTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

var _ Store = (*RedisStore)(nil)
var _ Attempt = (*redisAttempt)(nil)
