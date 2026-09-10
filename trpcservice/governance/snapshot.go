// Package governance owns immutable policy and pricing snapshots, deterministic
// decisions, and the durable budget/usage protocol used by every runtime entry.
package governance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const CurrentPolicySchemaVersion uint16 = 1

type Action string

const (
	ActionAllow    Action = "allow"
	ActionDeny     Action = "deny"
	ActionAsk      Action = "ask"
	ActionRedact   Action = "redact"
	ActionThrottle Action = "throttle"
)

const (
	ReasonAllowed              = "allowed"
	ReasonSubjectDenied        = "subject_denied"
	ReasonInputRejected        = "input_rejected"
	ReasonOutputRejected       = "output_rejected"
	ReasonModelDenied          = "model_denied"
	ReasonToolDenied           = "tool_denied"
	ReasonConfirmationMissing  = "confirmation_unavailable"
	ReasonBudgetExceeded       = "budget_exceeded"
	ReasonPricingUnavailable   = "pricing_unavailable"
	ReasonUsageUnavailable     = "usage_unavailable"
	ReasonReservationClosed    = "reservation_closed"
	ReasonConfirmationRequired = "confirmation_required"
	ReasonConfirmationDenied   = "confirmation_denied"
	ReasonConfirmationExpired  = "confirmation_expired"
	ReasonToolAttemptFailed    = "tool_attempt_failed"
	ReasonToolEffectUnknown    = "tool_effect_unknown"
)

type DLPMode string

const (
	DLPDisabled DLPMode = "disabled"
	DLPRequired DLPMode = "required"
)

type VersionedRef struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

type BudgetPolicy struct {
	MaxInputTokens      int64 `json:"max_input_tokens"`
	MaxOutputTokens     int64 `json:"max_output_tokens"`
	MaxCostMicrosPerRun int64 `json:"max_cost_micros_per_run"`
}

type ToolRule struct {
	ToolID                string `json:"tool_id"`
	Version               int64  `json:"version"`
	Dangerous             bool   `json:"dangerous,omitempty"`
	ConfirmationSupported bool   `json:"confirmation_supported,omitempty"`
}

type PolicyV1 struct {
	SchemaVersion  uint16         `json:"schema_version"`
	DefaultAction  Action         `json:"default_action"`
	AllowedModels  []VersionedRef `json:"allowed_models,omitempty"`
	Tools          []ToolRule     `json:"tools,omitempty"`
	InputDLP       DLPMode        `json:"input_dlp"`
	OutputDLP      DLPMode        `json:"output_dlp"`
	Budget         BudgetPolicy   `json:"budget"`
	PricingVersion int64          `json:"pricing_version,omitempty"`
}

type PolicySnapshot struct {
	TenantID      string
	Version       int64
	SchemaVersion uint16
	Policy        PolicyV1
	ContentDigest string
	PublishedAt   time.Time
}

type Price struct {
	Provider                    string `json:"provider"`
	Model                       string `json:"model"`
	ModelProfileID              string `json:"model_profile_id"`
	ModelProfileVersion         int64  `json:"model_profile_version"`
	InputMicrosPerMillion       int64  `json:"input_micros_per_million"`
	OutputMicrosPerMillion      int64  `json:"output_micros_per_million"`
	CachedInputMicrosPerMillion int64  `json:"cached_input_micros_per_million"`
}

type PricingV1 struct {
	SchemaVersion uint16    `json:"schema_version"`
	Currency      string    `json:"currency"`
	ConversionRef string    `json:"conversion_ref"`
	ValidFrom     time.Time `json:"valid_from"`
	ValidUntil    time.Time `json:"valid_until"`
	Prices        []Price   `json:"prices"`
}

type PricingSnapshot struct {
	TenantID      string
	Version       int64
	SchemaVersion uint16
	Pricing       PricingV1
	ContentDigest string
	PublishedAt   time.Time
}

type Decision struct {
	DecisionID    string
	TenantID      string
	RequestID     string
	Stage         string
	Action        Action
	ReasonCode    string
	PolicyVersion int64
	RuleIDs       []string
	ReservationID string
}

type GovernanceDecision = Decision

type Repository interface {
	GetPolicy(context.Context, string, int64) (PolicySnapshot, error)
	GetPricing(context.Context, string, int64) (PricingSnapshot, error)
}

// VersionedTool is implemented only by service-owned guarded wrappers. It lets
// per-run visibility and permission checks use the same exact tool revision as
// the final call guard instead of trusting a declaration name alone.
type VersionedTool interface {
	GovernanceToolRef() VersionedRef
}

func DecodePolicyV1(data []byte) (PolicyV1, error) {
	var value PolicyV1
	if err := strictDecode(data, &value); err != nil {
		return PolicyV1{}, fmt.Errorf("decode policy: %w", err)
	}
	if err := ValidatePolicy(value); err != nil {
		return PolicyV1{}, err
	}
	return NormalizePolicy(value), nil
}

func ValidatePolicy(value PolicyV1) error {
	if value.SchemaVersion != CurrentPolicySchemaVersion || (value.DefaultAction != ActionAllow && value.DefaultAction != ActionDeny) ||
		(value.InputDLP != DLPDisabled && value.InputDLP != DLPRequired) || (value.OutputDLP != DLPDisabled && value.OutputDLP != DLPRequired) ||
		value.Budget.MaxInputTokens < 0 || value.Budget.MaxOutputTokens < 0 || value.Budget.MaxCostMicrosPerRun < 0 || value.PricingVersion < 0 {
		return runtime.ErrInvariantViolation
	}
	seenModels := make(map[string]bool)
	for _, ref := range value.AllowedModels {
		key := fmt.Sprintf("%s\x00%d", ref.ID, ref.Version)
		if ref.ID == "" || ref.Version < 1 || seenModels[key] {
			return runtime.ErrInvariantViolation
		}
		seenModels[key] = true
	}
	seenTools := make(map[string]bool)
	for _, rule := range value.Tools {
		key := fmt.Sprintf("%s\x00%d", rule.ToolID, rule.Version)
		if rule.ToolID == "" || rule.Version < 1 || seenTools[key] || (rule.ConfirmationSupported && !rule.Dangerous) {
			return runtime.ErrInvariantViolation
		}
		seenTools[key] = true
	}
	if value.Budget.MaxCostMicrosPerRun > 0 && value.PricingVersion < 1 {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func NormalizePolicy(value PolicyV1) PolicyV1 {
	value.AllowedModels = append([]VersionedRef(nil), value.AllowedModels...)
	value.Tools = append([]ToolRule(nil), value.Tools...)
	sort.Slice(value.AllowedModels, func(i, j int) bool {
		if value.AllowedModels[i].ID == value.AllowedModels[j].ID {
			return value.AllowedModels[i].Version < value.AllowedModels[j].Version
		}
		return value.AllowedModels[i].ID < value.AllowedModels[j].ID
	})
	sort.Slice(value.Tools, func(i, j int) bool {
		if value.Tools[i].ToolID == value.Tools[j].ToolID {
			return value.Tools[i].Version < value.Tools[j].Version
		}
		return value.Tools[i].ToolID < value.Tools[j].ToolID
	})
	return value
}

func PolicyDigest(value PolicyV1) (string, []byte, error) {
	if err := ValidatePolicy(value); err != nil {
		return "", nil, err
	}
	data, err := json.Marshal(NormalizePolicy(value))
	if err != nil {
		return "", nil, err
	}
	return contentDigest(data), data, nil
}

func ValidatePricing(value PricingV1) error {
	if value.SchemaVersion != CurrentPolicySchemaVersion || len(value.Currency) != 3 || value.Currency != strings.ToUpper(value.Currency) || value.ConversionRef == "" ||
		value.ValidFrom.IsZero() || value.ValidUntil.IsZero() || !value.ValidUntil.After(value.ValidFrom) || len(value.Prices) == 0 {
		return runtime.ErrInvariantViolation
	}
	seen := make(map[string]bool)
	for _, price := range value.Prices {
		key := fmt.Sprintf("%s\x00%d", price.ModelProfileID, price.ModelProfileVersion)
		if price.Provider == "" || price.Model == "" || price.ModelProfileID == "" || price.ModelProfileVersion < 1 || seen[key] ||
			price.InputMicrosPerMillion < 0 || price.OutputMicrosPerMillion < 0 || price.CachedInputMicrosPerMillion < 0 {
			return runtime.ErrInvariantViolation
		}
		seen[key] = true
	}
	return nil
}

func NormalizePricing(value PricingV1) PricingV1 {
	value.Prices = append([]Price(nil), value.Prices...)
	sort.Slice(value.Prices, func(i, j int) bool {
		if value.Prices[i].ModelProfileID == value.Prices[j].ModelProfileID {
			return value.Prices[i].ModelProfileVersion < value.Prices[j].ModelProfileVersion
		}
		return value.Prices[i].ModelProfileID < value.Prices[j].ModelProfileID
	})
	value.ValidFrom, value.ValidUntil = value.ValidFrom.UTC(), value.ValidUntil.UTC()
	return value
}

func PricingDigest(value PricingV1) (string, []byte, error) {
	if err := ValidatePricing(value); err != nil {
		return "", nil, err
	}
	data, err := json.Marshal(NormalizePricing(value))
	if err != nil {
		return "", nil, err
	}
	return contentDigest(data), data, nil
}

func ModelAllowed(policy PolicySnapshot, model VersionedRef) bool {
	for _, allowed := range policy.Policy.AllowedModels {
		if allowed == model {
			return true
		}
	}
	return false
}

func strictDecode(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func contentDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
