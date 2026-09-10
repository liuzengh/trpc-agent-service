package postgres

import (
	"math"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestQuotaLimitsSelectScopeAndPeriod(t *testing.T) {
	tenantQuota := tenant.QuotaPolicy{DailyTokenQuota: 10, MonthlyTokenQuota: 100, DailyCostQuota: 1, MonthlyCostQuota: 10}
	appQuota := tenant.QuotaPolicy{DailyTokenQuota: 3, MonthlyTokenQuota: 30, DailyCostQuota: .3, MonthlyCostQuota: 3}
	day, err := quotaLimits("TENANT", "DAY", tenantQuota, appQuota)
	if err != nil || day.tokenLimit != 10 || day.costLimit != 1 {
		t.Fatalf("tenant day limit = %#v, error=%v", day, err)
	}
	month, err := quotaLimits("APP", "MONTH", tenantQuota, appQuota)
	if err != nil || month.tokenLimit != 30 || month.costLimit != 3 {
		t.Fatalf("app month limit = %#v, error=%v", month, err)
	}
	if _, err := quotaLimits("OTHER", "DAY", tenantQuota, appQuota); err == nil {
		t.Fatal("invalid quota scope was accepted")
	}
}

func TestUsageWithinQuotaEnforcesActualUsageAtHardLimit(t *testing.T) {
	limits := quotaPeriod{tokenLimit: 10, costLimit: 1}
	if !usageWithinQuotaValues(7, .7, 3, .3, limits) {
		t.Fatal("usage exactly at both limits was rejected")
	}
	if usageWithinQuotaValues(7, .7, 4, .1, limits) {
		t.Fatal("token usage over hard limit was accepted")
	}
	if usageWithinQuotaValues(7, .7, 1, .4, limits) {
		t.Fatal("cost usage over hard limit was accepted")
	}
}

func TestUsageCostFailsClosedForUnknownPricing(t *testing.T) {
	cost, err := usageCost(nil, .25)
	if err != nil || cost != .25 {
		t.Fatalf("fallback cost = %v, error=%v", cost, err)
	}
	if _, err := usageCost(nil, math.NaN()); err == nil {
		t.Fatal("invalid fallback cost was accepted")
	}
	invalid := math.Inf(1)
	if _, err := usageCost(&invalid, .25); err == nil {
		t.Fatal("infinite actual cost was accepted")
	}
}
