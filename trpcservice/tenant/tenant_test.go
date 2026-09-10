package tenant

import (
	"errors"
	"math"
	"testing"
)

func validCreate(key string) CreateInput {
	return CreateInput{TenantKey: key, DisplayName: "Example", AuditRetentionDays: 30, LogMaskingLevel: MaskingBasic, TraceSamplingRate: 1}
}

func TestNewTenantDefaultsAndID(t *testing.T) {
	tenant, err := NewTenant(validCreate(" Acme-1 "))
	if err != nil {
		t.Fatal(err)
	}
	if tenant.TenantKey != "acme-1" || tenant.Status != StatusActive || tenant.Version != 1 {
		t.Fatalf("unexpected tenant: %+v", tenant)
	}
	if err := validateTenantID(tenant.TenantID); err != nil {
		t.Fatal(err)
	}
	if tenant.AuditRetentionDays != 30 {
		t.Fatalf("retention changed: %d", tenant.AuditRetentionDays)
	}
}

func TestNewTenantValidationAndQuotaSemantics(t *testing.T) {
	zero := int64(0)
	input := validCreate("zero")
	input.RateLimitRPM = &zero
	input.MonthlyTokenBudget = &zero
	tenant, err := NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	if tenant.RateLimitRPM == nil || *tenant.RateLimitRPM != 0 || tenant.MonthlyTokenBudget == nil || *tenant.MonthlyTokenBudget != 0 {
		t.Fatal("zero quota must remain distinct from nil")
	}
	bad := validCreate("bad")
	bad.MonthlySpendLimitMinor = &zero
	if _, err := NewTenant(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected currency validation, got %v", err)
	}
	bad = validCreate("bad-sampling")
	bad.TraceSamplingRate = 1.1
	if _, err := NewTenant(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected sampling validation, got %v", err)
	}
}

func TestTenantCloneAndExecutionGate(t *testing.T) {
	rate := int64(10)
	concurrent := int64(2)
	tokens := int64(100)
	spend := int64(25)
	appID := "app-1"
	backendID := "backend-1"
	tenant, err := NewTenant(CreateInput{
		TenantKey: "clone", DisplayName: "Clone", RateLimitRPM: &rate, MaxConcurrentExecutions: &concurrent,
		MonthlyTokenBudget: &tokens, MonthlySpendLimitMinor: &spend, BillingCurrency: "USD",
		AuditRetentionDays: 30, LogMaskingLevel: MaskingStrict, TraceSamplingRate: 0.5,
		DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID,
	})
	if err != nil {
		t.Fatal(err)
	}
	clone := tenant.Clone()
	*clone.RateLimitRPM = 99
	*clone.DefaultAgentAppID = "changed"
	if *tenant.RateLimitRPM != 10 || *tenant.DefaultAgentAppID != "app-1" {
		t.Fatal("clone must not share mutable pointer fields")
	}
	if !tenant.CanAcceptExecution() {
		t.Fatal("active tenant must accept execution")
	}
	tenant.Status = StatusSuspended
	if tenant.CanAcceptExecution() {
		t.Fatal("suspended tenant must reject execution")
	}
	tenant.Status = StatusDisabled
	if tenant.CanAcceptExecution() {
		t.Fatal("disabled tenant must reject execution")
	}
	if cloneInt64(nil) != nil || cloneString(nil) != nil {
		t.Fatal("nil pointers must remain nil when cloned")
	}
}

func TestNewTenantRejectsInvalidIdentityAndStatus(t *testing.T) {
	tests := []CreateInput{
		{TenantKey: "1bad", DisplayName: "Example", AuditRetentionDays: 1, LogMaskingLevel: MaskingBasic},
		{TenantKey: "valid", DisplayName: "Example", Status: StatusDisabled, AuditRetentionDays: 1, LogMaskingLevel: MaskingBasic},
		{TenantKey: "valid", DisplayName: "Example", Status: Status("unknown"), AuditRetentionDays: 1, LogMaskingLevel: MaskingBasic},
	}
	for _, input := range tests {
		if _, err := NewTenant(input); !errors.Is(err, ErrInvalid) {
			t.Fatalf("expected invalid input %+v, got %v", input, err)
		}
	}
}

func TestValidateConfigurationBoundaries(t *testing.T) {
	negative := int64(-1)
	zero := int64(0)
	positive := int64(1)
	if err := ValidateConfiguration("Example", &zero, &positive, &zero, &zero, "USD", 1, MaskingNone, 0); err != nil {
		t.Fatalf("expected valid configuration, got %v", err)
	}
	tests := []struct {
		name string
		err  error
	}{
		{"blank name", ValidateConfiguration(" ", nil, nil, nil, nil, "", 1, MaskingBasic, 0)},
		{"long name", ValidateConfiguration(string(make([]rune, 201)), nil, nil, nil, nil, "", 1, MaskingBasic, 0)},
		{"negative rate", ValidateConfiguration("Example", &negative, nil, nil, nil, "", 1, MaskingBasic, 0)},
		{"zero concurrent", ValidateConfiguration("Example", nil, &zero, nil, nil, "", 1, MaskingBasic, 0)},
		{"negative tokens", ValidateConfiguration("Example", nil, nil, &negative, nil, "", 1, MaskingBasic, 0)},
		{"negative spend", ValidateConfiguration("Example", nil, nil, nil, &negative, "USD", 1, MaskingBasic, 0)},
		{"spend without currency", ValidateConfiguration("Example", nil, nil, nil, &zero, "", 1, MaskingBasic, 0)},
		{"spend with XXX", ValidateConfiguration("Example", nil, nil, nil, &zero, "XXX", 1, MaskingBasic, 0)},
		{"spend with XTS", ValidateConfiguration("Example", nil, nil, nil, &zero, "XTS", 1, MaskingBasic, 0)},
		{"spend with XUA", ValidateConfiguration("Example", nil, nil, nil, &zero, "XUA", 1, MaskingBasic, 0)},
		{"invalid currency", ValidateConfiguration("Example", nil, nil, nil, nil, "usd", 1, MaskingBasic, 0)},
		{"retention", ValidateConfiguration("Example", nil, nil, nil, nil, "", 0, MaskingBasic, 0)},
		{"masking", ValidateConfiguration("Example", nil, nil, nil, nil, "", 1, LogMaskingLevel("unknown"), 0)},
		{"sampling", ValidateConfiguration("Example", nil, nil, nil, nil, "", 1, MaskingBasic, -0.1)},
		{"nan sampling", ValidateConfiguration("Example", nil, nil, nil, nil, "", 1, MaskingBasic, math.NaN())},
		{"infinite sampling", ValidateConfiguration("Example", nil, nil, nil, nil, "", 1, MaskingBasic, math.Inf(1))},
	}
	for _, test := range tests {
		if !errors.Is(test.err, ErrInvalid) {
			t.Fatalf("%s: expected ErrInvalid, got %v", test.name, test.err)
		}
	}
}

func TestTenantIdentityHelpers(t *testing.T) {
	if key, err := normalizeTenantKey(" Acme-1 "); err != nil || key != "acme-1" {
		t.Fatalf("unexpected normalized key %q: %v", key, err)
	}
	for _, key := range []string{"a", "a_", "a*"} {
		if _, err := normalizeTenantKey(key); !errors.Is(err, ErrInvalid) {
			t.Fatalf("expected invalid key %q, got %v", key, err)
		}
	}
	validID := "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	if err := validateTenantID(validID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"bad", "t_81J1K9ZQTVE4PAWF1TSB2WMHNP", "t_01J1K9ZQTVE4PAWF1TSB2WMHN!"} {
		if !errors.Is(validateTenantID(id), ErrInvalid) {
			t.Fatalf("expected invalid ID %q", id)
		}
	}
	for _, currency := range []string{"", "US", "usd", "US1", "ZZZ", "XXX", "XTS", "XUA"} {
		if validCurrency(currency) {
			t.Fatalf("expected invalid currency %q", currency)
		}
	}
	if !validCurrency("USD") {
		t.Fatal("expected valid currency")
	}
}

func TestTenantValidateRejectsMutableIdentityAndInvalidLifecycle(t *testing.T) {
	tenant, err := NewTenant(validCreate("validate"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tenant.Validate(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*Tenant)
	}{
		{name: "malformed key", mutate: func(tenant *Tenant) { tenant.TenantKey = "!" }},
		{name: "unnormalized key", mutate: func(tenant *Tenant) { tenant.TenantKey = "UPPERCASE" }},
		{name: "unknown status", mutate: func(tenant *Tenant) { tenant.Status = Status("unknown") }},
		{name: "invalid configuration", mutate: func(tenant *Tenant) { tenant.DisplayName = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := tenant.Clone()
			test.mutate(&invalid)
			if !errors.Is(invalid.Validate(), ErrInvalid) {
				t.Fatal("expected invalid tenant rejection")
			}
		})
	}
}
