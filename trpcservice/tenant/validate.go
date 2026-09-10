package tenant

import (
	"fmt"
	"regexp"
	"strings"
)

var tenantKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,63}$`)

func (t Tenant) Validate() error {
	if t.TenantID == "" || !tenantKeyPattern.MatchString(t.TenantKey) || t.TenantKey != strings.ToLower(t.TenantKey) || strings.TrimSpace(t.DisplayName) == "" {
		return fmt.Errorf("%w: identity", ErrInvalid)
	}
	if t.Status != StatusActive && t.Status != StatusSuspended && t.Status != StatusDisabled {
		return fmt.Errorf("%w: status", ErrInvalid)
	}
	if invalidInt64(t.RequestLimitPerMinute) || invalidInt(t.MaxConcurrentExecutions) || invalidInt64(t.MonthlyTokenBudget) || invalidInt64(t.MonthlyCostBudgetMicros) {
		return fmt.Errorf("%w: negative limit", ErrInvalid)
	}
	if len(t.BillingCurrency) != 3 || t.BillingCurrency != strings.ToUpper(t.BillingCurrency) || t.AuditRetentionDays < 1 || t.TraceSamplingRate < 0 || t.TraceSamplingRate > 1 {
		return fmt.Errorf("%w: policy field", ErrInvalid)
	}
	if t.AuditPayloadMode != AuditMetadataOnly && t.AuditPayloadMode != AuditRedacted && t.AuditPayloadMode != AuditEncryptedReference {
		return fmt.Errorf("%w: audit mode", ErrInvalid)
	}
	if t.LogMaskingLevel != MaskingNone && t.LogMaskingLevel != MaskingBasic && t.LogMaskingLevel != MaskingStrict {
		return fmt.Errorf("%w: masking level", ErrInvalid)
	}
	if _, err := t.RedactionProgram(); err != nil {
		return fmt.Errorf("%w: redaction rules", ErrInvalid)
	}
	return nil
}

func invalidInt64(v *int64) bool { return v != nil && *v < 0 }
func invalidInt(v *int) bool     { return v != nil && *v < 0 }
