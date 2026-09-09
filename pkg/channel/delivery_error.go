//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package channel

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	deliveryCursorKey   = "delivery_cursor"
	deliveryNextKey     = "delivery_next_cursor"
	deliveryCompleteKey = "delivery_complete"
)

var (
	// ErrDeliveryOutcomeUnknown identifies a provider call whose outcome cannot
	// be proven from the client response. A message send may have been accepted
	// before a connection reset, timeout, or malformed response; retrying it
	// without a provider idempotency key can create a duplicate user-visible
	// message. The durable Delivery pipeline must stop and require reconciliation.
	ErrDeliveryOutcomeUnknown = errors.New("channel delivery outcome is unknown")
)

// DeliveryError carries durable retry semantics for a provider failure.
type DeliveryError struct {
	Err        error
	Permanent  bool
	RetryAfter time.Duration
	// OutcomeUnknown means the provider may have accepted the operation even
	// though this process cannot prove the result. It is intentionally separate
	// from Permanent: an unknown operation is not safe to retry, but it is not a
	// deterministic provider rejection either.
	OutcomeUnknown bool
}

func (e *DeliveryError) Error() string {
	if e == nil || e.Err == nil {
		return "channel delivery failure"
	}
	if e.OutcomeUnknown {
		// A provider response or transport error can contain URLs, response
		// bodies, credentials, or user content. Unknown outcomes are persisted
		// and may be rendered by operators, so never expose the wrapped cause
		// through the ordinary error string.
		return ErrDeliveryOutcomeUnknown.Error()
	}
	return e.Err.Error()
}

func (e *DeliveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func PermanentDeliveryError(err error) error {
	if err == nil {
		err = errors.New("permanent delivery failure")
	}
	return &DeliveryError{Err: err, Permanent: true}
}

func RetryableDeliveryError(err error) error {
	if err == nil {
		err = errors.New("retryable delivery failure")
	}
	return &DeliveryError{Err: err}
}

func RateLimitedDeliveryError(err error, retryAfter time.Duration) error {
	if err == nil {
		err = errors.New("provider rate limit")
	}
	return &DeliveryError{Err: err, RetryAfter: retryAfter}
}

// UnknownDeliveryError marks a send whose provider outcome cannot be proven.
// This is the conservative default for transport and response-integrity
// failures after a non-idempotent message request has been issued.
func UnknownDeliveryError(err error) error {
	// Preserve only platform-owned classification sentinels. The original
	// cause is intentionally not put in the unwrap chain: extension adapters
	// may return arbitrary provider text containing credentials or user data,
	// and this error is allowed to cross durable/logging boundaries.
	stable := []error{ErrDeliveryOutcomeUnknown}
	if errors.Is(err, ErrChannelTransport) {
		stable = append(stable, ErrChannelTransport)
	}
	return &DeliveryError{Err: errors.Join(stable...), OutcomeUnknown: true}
}

// DeliveryOutcomeUnknown reports whether retrying the same outbound operation
// could duplicate a provider-side side effect. Callers should persist the
// Outbox row in reconciliation state instead of applying ordinary backoff.
func DeliveryOutcomeUnknown(err error) bool {
	var typed *DeliveryError
	if !errors.As(err, &typed) || typed == nil {
		return errors.Is(err, ErrDeliveryOutcomeUnknown)
	}
	return typed.OutcomeUnknown || errors.Is(typed.Err, ErrDeliveryOutcomeUnknown)
}

// DeliveryFailure returns typed retry semantics. Unknown errors use platform
// exponential backoff.
func DeliveryFailure(err error) (permanent bool, retryAfter time.Duration) {
	var typed *DeliveryError
	if errors.As(err, &typed) {
		// An interface containing a nil *DeliveryError is still non-nil. Treat
		// this malformed classification as permanent instead of panicking or
		// retrying it forever.
		if typed == nil {
			return true, 0
		}
		return typed.Permanent, typed.RetryAfter
	}
	return false, 0
}

// OutboundDeliveryCursor returns the next provider chunk to send.
func OutboundDeliveryCursor(msg *OutboundMessage) (int, error) {
	if msg == nil || msg.Metadata == nil || msg.Metadata[deliveryCursorKey] == "" {
		return 0, nil
	}
	cursor, err := strconv.Atoi(msg.Metadata[deliveryCursorKey])
	if err != nil || cursor < 0 {
		return 0, fmt.Errorf("invalid delivery cursor")
	}
	return cursor, nil
}

// SetOutboundDeliveryProgress records progress after one successful provider
// call. Delivery persists it before another chunk is attempted.
func SetOutboundDeliveryProgress(msg *OutboundMessage, next int, complete bool) {
	if msg.Metadata == nil {
		msg.Metadata = make(map[string]string)
	}
	msg.Metadata[deliveryNextKey] = strconv.Itoa(next)
	msg.Metadata[deliveryCompleteKey] = strconv.FormatBool(complete)
}

// OutboundDeliveryProgress reads progress produced by an Adapter.
func OutboundDeliveryProgress(msg *OutboundMessage) (next int, complete bool, err error) {
	if msg == nil || msg.Metadata == nil {
		return 0, false, fmt.Errorf("adapter did not report delivery progress")
	}
	next, err = strconv.Atoi(msg.Metadata[deliveryNextKey])
	if err != nil || next <= 0 {
		return 0, false, fmt.Errorf("adapter reported invalid delivery progress")
	}
	complete, err = strconv.ParseBool(msg.Metadata[deliveryCompleteKey])
	if err != nil {
		return 0, false, fmt.Errorf("adapter reported invalid completion state")
	}
	return next, complete, nil
}
