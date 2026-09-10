package governance

import (
	"testing"
	"time"
)

func TestNewPostgresUsageGovernorRequiresDatabase(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresUsageGovernor(nil); err == nil {
		t.Fatal("NewPostgresUsageGovernor(nil) error = nil")
	}
}

func TestValidateUsageReservationRequest(t *testing.T) {
	t.Parallel()
	valid := UsageReservationRequest{
		TenantID: "support", AppCode: "assistant", Channel: "web", BindingID: "console",
		MessageID: "message-1", TraceID: "trace-1", MaxConcurrentRuns: 2,
		TokenBudget: 1000, ReservedTokens: 100, LeaseTTL: time.Minute,
	}
	if err := validateUsageReservationRequest(valid); err != nil {
		t.Fatalf("valid request error = %v", err)
	}

	tests := []struct {
		name string
		edit func(*UsageReservationRequest)
	}{
		{"missing tenant", func(r *UsageReservationRequest) { r.TenantID = " " }},
		{"missing app", func(r *UsageReservationRequest) { r.AppCode = "" }},
		{"missing channel", func(r *UsageReservationRequest) { r.Channel = "" }},
		{"missing binding", func(r *UsageReservationRequest) { r.BindingID = "" }},
		{"missing message", func(r *UsageReservationRequest) { r.MessageID = "" }},
		{"missing trace", func(r *UsageReservationRequest) { r.TraceID = "" }},
		{"negative concurrency", func(r *UsageReservationRequest) { r.MaxConcurrentRuns = -1 }},
		{"negative budget", func(r *UsageReservationRequest) { r.TokenBudget = -1 }},
		{"negative reservation", func(r *UsageReservationRequest) { r.ReservedTokens = -1 }},
		{"zero reservation with budget", func(r *UsageReservationRequest) { r.ReservedTokens = 0 }},
		{"reservation exceeds budget", func(r *UsageReservationRequest) { r.ReservedTokens = 1001 }},
		{"zero ttl", func(r *UsageReservationRequest) { r.LeaseTTL = 0 }},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			request := valid
			tt.edit(&request)
			if err := validateUsageReservationRequest(request); err == nil {
				t.Fatal("invalid request error = nil")
			}
		})
	}
}

func TestSettleKnownRejectsInvalidUsageBeforeDatabase(t *testing.T) {
	t.Parallel()
	governor := &PostgresUsageGovernor{}
	invalid := []SettledUsage{
		{PromptTokens: -1},
		{CompletionTokens: -1},
		{TotalTokens: -1},
		{CostMicros: -1},
		{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 3},
	}
	for _, usage := range invalid {
		if err := governor.SettleKnown(nil, UsageReservation{}, usage); err == nil {
			t.Fatalf("SettleKnown(%+v) error = nil", usage)
		}
	}
}

func TestSettlementRequiresReservationIdentityBeforeDatabase(t *testing.T) {
	t.Parallel()
	governor := &PostgresUsageGovernor{}
	if err := governor.SettleUnknown(nil, UsageReservation{}); err == nil {
		t.Fatal("SettleUnknown(empty) error = nil")
	}
	if err := governor.SettleKnown(nil, UsageReservation{}, SettledUsage{}); err == nil {
		t.Fatal("SettleKnown(empty) error = nil")
	}
}
