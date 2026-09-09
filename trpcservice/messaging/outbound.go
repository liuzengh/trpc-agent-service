package messaging

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/keyspace"
)

var ErrOutboundDeferred = errors.New("outbound delivery is not due")

type OutboundState struct {
	Status        string
	Attempts      int
	LastError     string
	NextAttemptAt time.Time
	Terminal      bool
	AckedAt       time.Time
	UpdatedAt     time.Time
}

func (s *Store) BeginOutbound(ctx context.Context, delivery ReplyDelivery) (OutboundState, error) {
	if delivery.StreamID == "" || delivery.Result.TaskID == "" || !delivery.Result.Target.Valid() {
		return OutboundState{}, errors.New("outbound delivery target is invalid")
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return OutboundState{}, err
	}
	value, err := beginOutboundScript.Run(ctx, s.client, []string{s.outboundKey(delivery.Result.TaskID)}, now.UnixMilli(), int64(s.config.InboxRetention/time.Second)).Slice()
	if err != nil {
		return OutboundState{}, err
	}
	code := asInt64(value[0])
	attempts := asInt64(value[1])
	due := asInt64(value[2])
	state := OutboundState{Status: "sending", Attempts: int(attempts), UpdatedAt: now}
	switch code {
	case -9:
		return OutboundState{}, ErrKeyType
	case 2:
		state.Terminal = true
		return state, ErrTerminal
	case 3:
		state.Status = "retry_wait"
		state.NextAttemptAt = time.UnixMilli(due)
		return state, ErrOutboundDeferred
	case 1:
		return state, nil
	default:
		return OutboundState{}, errors.New("unexpected outbound state transition")
	}
}

func (s *Store) RetryOutbound(ctx context.Context, delivery ReplyDelivery, state OutboundState, errorCode string) (bool, error) {
	if state.Attempts >= outboundMaxAttempts(s.config) {
		return true, s.finishOutbound(ctx, delivery, "failed_terminal", errorCode)
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return false, err
	}
	due := now.Add(outboundBackoff(s.config, state.Attempts))
	result, err := retryOutboundScript.Run(ctx, s.client, []string{s.outboundKey(delivery.Result.TaskID)}, errorCode, due.UnixMilli(), now.UnixMilli(), int64(s.config.InboxRetention/time.Second)).Int()
	if result == -9 {
		return false, ErrKeyType
	}
	return false, err
}

func (s *Store) CompleteOutbound(ctx context.Context, delivery ReplyDelivery) error {
	return s.finishOutbound(ctx, delivery, "succeeded", "")
}

func (s *Store) finishOutbound(ctx context.Context, delivery ReplyDelivery, status, errorCode string) error {
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	result, err := finishOutboundScript.Run(ctx, s.client, []string{s.outboundKey(delivery.Result.TaskID), s.replyStream}, status, errorCode, now.UnixMilli(), int64(s.config.InboxRetention/time.Second), gatewayGroup, delivery.StreamID).Int()
	if result == -9 {
		return ErrKeyType
	}
	return err
}

func (s *Store) OutboundSnapshot(ctx context.Context, taskID string) (OutboundState, error) {
	values, err := s.client.HGetAll(ctx, s.outboundKey(taskID)).Result()
	if err != nil {
		return OutboundState{}, err
	}
	if len(values) == 0 {
		return OutboundState{}, ErrInboxMissing
	}
	return OutboundState{
		Status: values["status"], Attempts: parseInt(values["attempts"]), LastError: values["last_error"],
		NextAttemptAt: parseMillis(values["next_attempt_at"]), Terminal: values["terminal"] == "1",
		AckedAt: parseMillis(values["acked_at"]), UpdatedAt: parseMillis(values["updated_at"]),
	}, nil
}

func (s *Store) outboundKey(taskID string) string {
	return keyspace.Outbound(s.config.KeyPrefix, taskID)
}

func outboundMaxAttempts(cfg config.MessagingConfig) int {
	if cfg.OutboundMaxAttempts == 0 {
		return config.DefaultOutboundMaxAttempts
	}
	return cfg.OutboundMaxAttempts
}

func outboundBackoff(cfg config.MessagingConfig, attempt int) time.Duration {
	initial := cfg.OutboundInitialBackoff
	if initial == 0 {
		initial = config.DefaultOutboundInitialBackoff
	}
	maximum := cfg.OutboundMaxBackoff
	if maximum == 0 {
		maximum = config.DefaultOutboundMaxBackoff
	}
	delay := initial
	for i := 1; i < attempt && delay < maximum; i++ {
		delay *= 2
		if delay > maximum {
			delay = maximum
		}
	}
	return delay
}

func parseInt(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func parseMillis(value string) time.Time {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	if parsed == 0 {
		return time.Time{}
	}
	return time.UnixMilli(parsed)
}
