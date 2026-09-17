package governance

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type ReservationState string

const (
	ReservationReserved ReservationState = "reserved"
	ReservationSettled  ReservationState = "settled"
	ReservationRefunded ReservationState = "refunded"
)

type ReserveRequest struct {
	TenantID       string
	RequestID      string
	ResourceID     string
	AttemptClass   string
	PolicyVersion  int64
	PricingVersion int64
	MaxCostMicros  int64
	MaxTokens      int64
}

type Reservation struct {
	ReservationID      string
	TenantID           string
	RequestID          string
	ResourceID         string
	AttemptClass       string
	PolicyVersion      int64
	PricingVersion     int64
	ReservedCostMicros int64
	ReservedTokens     int64
	ActualCostMicros   int64
	InputTokens        int64
	OutputTokens       int64
	State              ReservationState
	Version            int64
}

type Usage struct{ InputTokens, OutputTokens, CachedInputTokens int64 }

type SettleRequest struct {
	TenantID         string
	ReservationID    string
	RequestID        string
	Stage            string
	UsageKind        string
	ExpectedVersion  int64
	Usage            Usage
	ActualCostMicros int64
}

type Ledger interface {
	Reserve(context.Context, ReserveRequest) (Reservation, error)
	Settle(context.Context, SettleRequest) (Reservation, error)
	Refund(context.Context, string, string, int64, string) (Reservation, error)
	GetReservation(context.Context, string, string) (Reservation, error)
}

func StableReservationID(in ReserveRequest) (string, error) {
	if in.TenantID == "" || in.RequestID == "" || in.ResourceID == "" || in.AttemptClass == "" {
		return "", runtime.ErrInvariantViolation
	}
	return "bres_" + contentDigest([]byte(in.TenantID + "\x00" + in.RequestID + "\x00" + in.ResourceID + "\x00" + in.AttemptClass))[:32], nil
}

func PriceUsage(snapshot PricingSnapshot, model VersionedRef, usage Usage, at time.Time) (int64, error) {
	if snapshot.TenantID == "" || snapshot.Version < 1 || usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.CachedInputTokens < 0 || usage.CachedInputTokens > usage.InputTokens ||
		at.Before(snapshot.Pricing.ValidFrom) || !at.Before(snapshot.Pricing.ValidUntil) {
		return 0, runtime.ErrCapabilityUnsupported
	}
	for _, price := range snapshot.Pricing.Prices {
		if price.ModelProfileID != model.ID || price.ModelProfileVersion != model.Version {
			continue
		}
		parts := [][2]int64{{usage.InputTokens - usage.CachedInputTokens, price.InputMicrosPerMillion}, {usage.CachedInputTokens, price.CachedInputMicrosPerMillion}, {usage.OutputTokens, price.OutputMicrosPerMillion}}
		var total int64
		for _, part := range parts {
			if part[0] != 0 && part[1] > math.MaxInt64/part[0] {
				return 0, runtime.ErrInvariantViolation
			}
			product := part[0] * part[1]
			charge := product / 1_000_000
			if product%1_000_000 != 0 {
				charge++
			}
			if charge > math.MaxInt64-total {
				return 0, runtime.ErrInvariantViolation
			}
			total += charge
		}
		return total, nil
	}
	return 0, fmt.Errorf("%w: %s", runtime.ErrCapabilityUnsupported, ReasonPricingUnavailable)
}
