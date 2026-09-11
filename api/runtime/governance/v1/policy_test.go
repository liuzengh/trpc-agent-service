package governancev1

import (
	"errors"
	"testing"
)

func fixture() Policy {
	return Policy{SchemaVersion: 1, TenantID: "tnt_a", Revision: 2, Enabled: true,
		IM:       IMPolicy{Rules: []IMRule{{AccountID: "cha_a", BindingID: "cbd_a", UserIDs: []string{"100", "200"}, GroupIDs: []string{"group-a"}}}},
		Requests: RequestPolicy{TenantPerMinute: 100, UserPerMinute: 10}, Execution: ExecutionPolicy{MaxConcurrentRuns: 3},
		Tokens: TokenPolicy{PeriodSeconds: 86400, Limit: 1_000_000, ReservationPerRun: 4096, InputMicrosPerMTok: 100, OutputMicrosPerMTok: 400}}
}
func TestPolicyValidationAndAuthorization(t *testing.T) {
	p := fixture()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if !p.Allows("cha_a", "cbd_a", "100", "direct") || !p.Allows("cha_a", "cbd_a", "other", "group-a") || p.Allows("cha_a", "cbd_a", "other", "other") {
		t.Fatal("authorization mismatch")
	}
	p.IM.Rules[0].UserIDs = []string{"200", "100"}
	if !errors.Is(p.Validate(), ErrInvalidPolicy) {
		t.Fatal("unsorted identity list accepted")
	}
}
func TestDisabledPolicyCarriesNoHiddenLimits(t *testing.T) {
	p := Disabled("tnt_a")
	if err := p.Validate(); err != nil || !p.Allows("a", "b", "u", "g") {
		t.Fatal(err)
	}
	p.Requests.TenantPerMinute = 1
	if !errors.Is(p.Validate(), ErrInvalidPolicy) {
		t.Fatal("hidden disabled limit accepted")
	}
}
