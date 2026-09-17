package governance

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestPolicyIsStrictNormalizedAndPricingFailsClosed(t *testing.T) {
	if _, err := DecodePolicyV1([]byte(`{"schema_version":1,"default_action":"allow","input_dlp":"disabled","output_dlp":"disabled","budget":{"max_input_tokens":1,"max_output_tokens":1,"max_cost_micros_per_run":0},"unknown":true}`)); err == nil {
		t.Fatal("unknown policy field accepted")
	}
	policy := PolicyV1{SchemaVersion: 1, DefaultAction: ActionAllow, AllowedModels: []VersionedRef{{ID: "b", Version: 1}, {ID: "a", Version: 2}}, InputDLP: DLPDisabled, OutputDLP: DLPDisabled}
	first, _, err := PolicyDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	policy.AllowedModels[0], policy.AllowedModels[1] = policy.AllowedModels[1], policy.AllowedModels[0]
	second, _, _ := PolicyDigest(policy)
	if first != second {
		t.Fatalf("digest drift %s %s", first, second)
	}
	now := time.Now().UTC()
	pricingValue := PricingV1{SchemaVersion: 1, Currency: "USD", ConversionRef: "fx-usd-v1", ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), Prices: []Price{{Provider: "deepseek", Model: "chat", ModelProfileID: "model", ModelProfileVersion: 1, InputMicrosPerMillion: 1_000_000, OutputMicrosPerMillion: 2_000_000, CachedInputMicrosPerMillion: 500_000}}}
	digest, _, err := PricingDigest(pricingValue)
	if err != nil {
		t.Fatal(err)
	}
	pricing := PricingSnapshot{TenantID: "tenant", Version: 1, Pricing: NormalizePricing(pricingValue), ContentDigest: digest}
	cost, err := PriceUsage(pricing, VersionedRef{ID: "model", Version: 1}, Usage{InputTokens: 10, CachedInputTokens: 4, OutputTokens: 3}, now)
	if err != nil || cost != 14 {
		t.Fatalf("cost=%d err=%v", cost, err)
	}
	if _, err := PriceUsage(pricing, VersionedRef{ID: "missing", Version: 1}, Usage{InputTokens: 1}, now); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("missing price=%v", err)
	}
}
