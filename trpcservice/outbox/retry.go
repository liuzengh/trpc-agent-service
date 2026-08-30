package outbox

import (
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const (
	OutcomeDelivered        OutcomeClass = "delivered"
	OutcomeRetryableFailure OutcomeClass = "retryable_failure"
	OutcomePermanentFailure OutcomeClass = "permanent_failure"
	OutcomeUnknown          OutcomeClass = "outcome_unknown"

	Delivered        = OutcomeDelivered
	RetryableFailure = OutcomeRetryableFailure
	PermanentFailure = OutcomePermanentFailure
	Unknown          = OutcomeUnknown

	Retry      RetryAction = "retry"
	DeadLetter RetryAction = "dead-letter"
	Complete   RetryAction = "complete"
)

const (
	SenderUnavailableCode        = "sender_unavailable"
	SenderTimeoutCode            = "sender_timeout"
	SenderRateLimitedCode        = "sender_rate_limited"
	SenderInvalidPayloadCode     = "sender_invalid_payload"
	SenderInvalidDestinationCode = "sender_invalid_destination"
	SenderUnknownChannelCode     = "sender_unknown_channel"
	SenderMalformedResponseCode  = "sender_malformed_response"
	SenderRejectedCode           = "sender_rejected"
	SenderNotConfiguredCode      = "sender_not_configured"
	DeliveryOutcomeUnknownCode   = "delivery_outcome_unknown"
	RepositoryLockLostCode       = "repository_lock_lost"
	RepositoryUnavailableCode    = "repository_unavailable"
	DispatcherShutdownCode       = "dispatcher_shutdown"
)

const (
	DefaultMaxAttempts = 5
	DefaultBaseDelay   = time.Second
	DefaultMaxDelay    = time.Minute
	MaxPolicyAttempts  = 100
	MaxPolicyDelay     = 24 * time.Hour
)

var (
	ErrInvalidPolicy  = errors.New("outbox: invalid retry policy")
	ErrInvalidOutcome = errors.New("outbox: invalid sender outcome")
)

// OutcomeClass is the sender's explicit delivery conclusion.
type OutcomeClass string

// SenderOutcome is the only result a Sender may return. A Delivered outcome
// means the Sender has explicit confirmation; an error is never inferred from
// an arbitrary error string by the Dispatcher.
type SenderOutcome struct {
	Class OutcomeClass
	Code  string
}

type SendOutcome = SenderOutcome

// RetryAction is the durable action selected for the current delivery attempt.
type RetryAction string

type RetryDecision struct {
	Action      RetryAction
	Code        string
	NextAttempt time.Time
}

// RetryPolicy is a bounded, deterministic policy. It has no clock or random
// state; the caller supplies now so tests and production can control time.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxAttempts == 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}
	if p.BaseDelay == 0 {
		p.BaseDelay = DefaultBaseDelay
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = DefaultMaxDelay
	}
	return p
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 || p.MaxAttempts > MaxPolicyAttempts {
		return fmt.Errorf("%w: max attempts must be between one and %d", ErrInvalidPolicy, MaxPolicyAttempts)
	}
	if p.BaseDelay < 0 || p.MaxDelay < 0 || p.MaxDelay < p.BaseDelay {
		return fmt.Errorf("%w: delays must be non-negative and ordered", ErrInvalidPolicy)
	}
	if p.MaxDelay > MaxPolicyDelay {
		return fmt.Errorf("%w: max delay exceeds %s", ErrInvalidPolicy, MaxPolicyDelay)
	}
	return nil
}

// Decide selects a bounded action. Retryable and unknown outcomes retry while
// attempts remain; once the configured boundary is reached, the policy makes
// the terminal DLQ decision. Unknown is therefore never completed and is not
// treated as permanent merely because the Sender returned an error.
func (p RetryPolicy) Decide(attempt int, outcome SenderOutcome, now time.Time) (RetryDecision, error) {
	if err := p.Validate(); err != nil {
		return RetryDecision{}, err
	}
	if attempt < 1 || now.IsZero() {
		return RetryDecision{}, fmt.Errorf("%w: attempt and current time are required", ErrInvalidPolicy)
	}
	if outcome.Class != OutcomeDelivered && outcome.Class != OutcomeRetryableFailure && outcome.Class != OutcomePermanentFailure && outcome.Class != OutcomeUnknown {
		return RetryDecision{}, fmt.Errorf("%w: class %q is not supported", ErrInvalidOutcome, outcome.Class)
	}

	decision := RetryDecision{Code: normalizeOutcome(outcome).Code}
	if outcome.Class == OutcomeDelivered {
		return RetryDecision{Action: Complete}, nil
	}
	if outcome.Class == OutcomePermanentFailure || attempt >= p.MaxAttempts {
		decision.Action = DeadLetter
		return decision, nil
	}

	delay, err := p.delay(attempt)
	if err != nil {
		return RetryDecision{}, err
	}
	now = now.UTC()
	next := now.Add(delay)
	if delay > 0 && !next.After(now) {
		return RetryDecision{}, fmt.Errorf("%w: next attempt time overflow", ErrInvalidPolicy)
	}
	decision.Action = Retry
	decision.NextAttempt = next
	return decision, nil
}

func (p RetryPolicy) delay(attempt int) (time.Duration, error) {
	if attempt < 1 {
		return 0, fmt.Errorf("%w: attempt must be positive", ErrInvalidPolicy)
	}
	if p.BaseDelay == 0 || p.MaxDelay == 0 {
		return 0, nil
	}
	delay := p.BaseDelay
	for step := 1; step < attempt && delay < p.MaxDelay; step++ {
		if delay > p.MaxDelay/2 {
			return p.MaxDelay, nil
		}
		delay *= 2
	}
	if delay > p.MaxDelay {
		return p.MaxDelay, nil
	}
	return delay, nil
}

// ClassifyOutcome normalizes all durable codes to a finite allowlist. The
// original Code is never persisted unless it is one of these stable tokens.
func ClassifyOutcome(outcome SenderOutcome) SenderOutcome {
	return normalizeOutcome(outcome)
}

func normalizeOutcome(outcome SenderOutcome) SenderOutcome {
	switch outcome.Class {
	case OutcomeDelivered:
		return SenderOutcome{Class: OutcomeDelivered}
	case OutcomeUnknown:
		return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
	case OutcomeRetryableFailure:
		switch outcome.Code {
		case SenderUnavailableCode, SenderTimeoutCode, SenderRateLimitedCode, RepositoryUnavailableCode,
			"lark_sender_timeout", "lark_sender_rate_limited", "lark_sender_unavailable",
			"telegram_sender_timeout", "telegram_sender_rate_limited", "telegram_sender_unavailable":
			return SenderOutcome{Class: outcome.Class, Code: outcome.Code}
		default:
			return SenderOutcome{Class: outcome.Class, Code: SenderUnavailableCode}
		}
	case OutcomePermanentFailure:
		switch outcome.Code {
		case SenderInvalidPayloadCode, SenderInvalidDestinationCode, SenderUnknownChannelCode,
			SenderMalformedResponseCode, SenderNotConfiguredCode, SenderRejectedCode,
			"lark_sender_invalid_destination", "lark_sender_auth_failed", "lark_sender_forbidden",
			"lark_sender_rejected", "lark_sender_not_configured", "lark_sender_malformed_response",
			"telegram_sender_invalid_destination", "telegram_sender_auth_failed", "telegram_sender_forbidden",
			"telegram_sender_rejected", "telegram_sender_message_too_long", "telegram_sender_not_configured",
			"telegram_sender_malformed_response":
			return SenderOutcome{Class: outcome.Class, Code: outcome.Code}
		default:
			return SenderOutcome{Class: outcome.Class, Code: SenderRejectedCode}
		}
	default:
		return SenderOutcome{Class: outcome.Class, Code: storage.ErrInvalidArgument.Error()}
	}
}
