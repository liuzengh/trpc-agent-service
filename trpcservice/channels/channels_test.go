package channels

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

const (
	testTenantID = "t_00000000000000000000000000"
	testAppID    = "app_00000000000000000000000000"
)

func TestBindingDomainInvariantsAndDefensiveConfiguration(t *testing.T) {
	routeDigest, err := DigestPublicRouteKey(ChannelWeCom, "shared-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := NewBinding(CreateInput{
		TenantID: testTenantID, BindingKey: " Support-Channel ", Channel: ChannelWeCom,
		ProviderAccountID: " corp-1 ", PublicRouteKeyDigest: routeDigest, AppID: testAppID,
		SecretRef: "secret/wecom", Protocol: ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{
			CorpID: " corp-id ", ReceiveID: " receive-id ",
		}}, Metadata: validChangeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if binding.BindingKey != "support-channel" || binding.ProviderAccountID != "corp-1" || binding.Protocol.WeCom.CorpID != "corp-id" {
		t.Fatalf("input was not normalized: %+v", binding)
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(binding.ConfigDigest, "secret-value") || strings.Contains(fmt.Sprintf("%+v", binding), "secret-value") {
		t.Fatal("secret value appeared in the binding or config digest")
	}

	clone := binding.Clone()
	clone.Protocol.WeCom.CorpID = "changed"
	if binding.Protocol.WeCom.CorpID == "changed" {
		t.Fatal("binding clone leaked protocol pointer state")
	}

	encoded, err := json.Marshal(CandidateBindingContext{
		Channel: ChannelWeCom, PublicRouteKeyDigest: routeDigest, BindingVersion: binding.Version,
		ConfigDigest: binding.ConfigDigest, Purpose: PurposeWebhookVerification, CandidateToken: "opaque",
		IssuedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedText := string(encoded)
	for _, forbidden := range []string{testTenantID, testAppID, binding.SecretRef, "secret-value"} {
		if strings.Contains(encodedText, forbidden) {
			t.Fatalf("candidate context leaked %q: %s", forbidden, encodedText)
		}
	}
	if _, exists := reflectField(CandidateBindingContext{}, "TenantID"); exists {
		t.Fatal("candidate context contains a tenant identity field")
	}
}

func TestWeComAIBotBindingSeparatesCredentialsAndNormalizesEndpoint(t *testing.T) {
	routeDigest, err := DigestPublicRouteKey(ChannelWeComAIBot, "bot-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := NewBinding(CreateInput{TenantID: testTenantID, BindingKey: "aibot", Channel: ChannelWeComAIBot, ProviderAccountID: "bot-account", PublicRouteKeyDigest: routeDigest, AppID: testAppID, SecretRef: "secret/aibot", Protocol: ProtocolConfiguration{WeComAIBot: &WeComAIBotProtocolConfiguration{BotID: "bot-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if binding.Protocol.WeComAIBot == nil || binding.Protocol.WeComAIBot.WSURL != "wss://openws.work.weixin.qq.com" {
		t.Fatalf("AI Bot endpoint = %+v", binding.Protocol.WeComAIBot)
	}
	invalid := binding.Clone()
	invalid.Protocol.WeComAIBot.WSURL = "ws://insecure.example.com"
	if err := invalid.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("insecure AI Bot URL accepted: %v", err)
	}
	missingBot := binding.Clone()
	missingBot.Protocol.WeComAIBot.BotID = ""
	if err := missingBot.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing AI Bot ID accepted: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"wecom_aibot":{"bot_id":"bot-1","secret":"must-not-be-stored"}}`), &ProtocolConfiguration{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("credential-shaped protocol JSON accepted: %v", err)
	}
}

func TestBindingRejectsUnknownChannelAndProtocolCrossing(t *testing.T) {
	routeDigest, err := DigestPublicRouteKey(ChannelTelegram, "telegram-route")
	if err != nil {
		t.Fatal(err)
	}
	base := CreateInput{
		TenantID: testTenantID, BindingKey: "telegram", Channel: ChannelTelegram,
		ProviderAccountID: "bot-1", PublicRouteKeyDigest: routeDigest, AppID: testAppID,
		SecretRef: "secret/telegram", Protocol: ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{}},
	}
	if _, err := NewBinding(base); err != nil {
		t.Fatal(err)
	}

	unknown := base
	unknown.Channel = Channel("line")
	if _, err := NewBinding(unknown); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown channel was accepted: %v", err)
	}
	crossed := base
	crossed.Protocol = ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{CorpID: "wrong"}}
	if _, err := NewBinding(crossed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cross-channel protocol was accepted: %v", err)
	}
	badURL := base
	badURL.Protocol = ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{APIBaseURL: "http://localhost"}}
	if _, err := NewBinding(badURL); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-HTTPS Telegram API URL was accepted: %v", err)
	}
	badURL.Protocol = ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{APIBaseURL: "https://api.example.com/path"}}
	if _, err := NewBinding(badURL); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-origin Telegram API URL was accepted: %v", err)
	}
	badURL.Protocol = ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{APIBaseURL: "https://" + strings.Repeat("a", 260) + ".com"}}
	if _, err := NewBinding(badURL); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized Telegram API URL was accepted: %v", err)
	}
	badProtocol := base
	badProtocol.Protocol = ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{CorpID: "bad\nvalue"}}
	if _, err := NewBinding(badProtocol); !errors.Is(err, ErrInvalid) {
		t.Fatalf("control protocol value was accepted: %v", err)
	}
	var configuration ProtocolConfiguration
	if err := json.Unmarshal([]byte(`{"wecom":{"token":"must-not-be-stored"}}`), &configuration); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown or credential-shaped protocol field was accepted: %v", err)
	}
}

func TestNewBindingAndBindingValidationRejectMalformedState(t *testing.T) {
	routeDigest, _ := DigestPublicRouteKey(ChannelWeCom, "invalid-state-route")
	base := CreateInput{TenantID: testTenantID, BindingKey: "invalid-state", Channel: ChannelWeCom, ProviderAccountID: "corp-invalid", PublicRouteKeyDigest: routeDigest, AppID: testAppID, SecretRef: "secret/invalid", Protocol: ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{CorpID: "corp-invalid"}}}
	createCases := []struct {
		name   string
		mutate func(*CreateInput)
	}{
		{name: "tenant", mutate: func(input *CreateInput) { input.TenantID = "" }},
		{name: "key", mutate: func(input *CreateInput) { input.BindingKey = "!" }},
		{name: "channel", mutate: func(input *CreateInput) { input.Channel = Channel("unknown") }},
		{name: "provider", mutate: func(input *CreateInput) { input.ProviderAccountID = "" }},
		{name: "route", mutate: func(input *CreateInput) { input.PublicRouteKeyDigest = "bad" }},
		{name: "app", mutate: func(input *CreateInput) { input.AppID = "bad" }},
		{name: "secret", mutate: func(input *CreateInput) { input.SecretRef = "" }},
		{name: "protocol", mutate: func(input *CreateInput) {
			input.Protocol = ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{}}
		}},
		{name: "disabled", mutate: func(input *CreateInput) { input.Status = StatusDisabled }},
		{name: "unknown status", mutate: func(input *CreateInput) { input.Status = Status("unknown") }},
	}
	for _, test := range createCases {
		t.Run("create-"+test.name, func(t *testing.T) {
			input := base
			test.mutate(&input)
			if _, err := NewBinding(input); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected invalid input, got %v", err)
			}
		})
	}

	binding, err := NewBinding(base)
	if err != nil {
		t.Fatal(err)
	}
	validationCases := []struct {
		name   string
		mutate func(*Binding)
	}{
		{name: "tenant", mutate: func(value *Binding) { value.TenantID = "" }},
		{name: "binding id", mutate: func(value *Binding) { value.BindingID = "bad" }},
		{name: "key normalization", mutate: func(value *Binding) { value.BindingKey = "Invalid-State" }},
		{name: "channel", mutate: func(value *Binding) { value.Channel = Channel("unknown") }},
		{name: "provider normalization", mutate: func(value *Binding) { value.ProviderAccountID = " corp-invalid" }},
		{name: "route normalization", mutate: func(value *Binding) { value.PublicRouteKeyDigest = strings.ToUpper(value.PublicRouteKeyDigest) }},
		{name: "app", mutate: func(value *Binding) { value.AppID = "bad" }},
		{name: "secret normalization", mutate: func(value *Binding) { value.SecretRef = " secret/invalid" }},
		{name: "protocol", mutate: func(value *Binding) {
			value.Protocol = ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{}}
		}},
		{name: "status", mutate: func(value *Binding) { value.Status = Status("unknown") }},
		{name: "version", mutate: func(value *Binding) { value.Version = 0 }},
		{name: "created time", mutate: func(value *Binding) { value.CreatedAt = time.Time{} }},
		{name: "updated order", mutate: func(value *Binding) { value.UpdatedAt = value.CreatedAt.Add(-time.Second) }},
		{name: "timestamp location", mutate: func(value *Binding) {
			value.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("test", 3600))
			value.UpdatedAt = value.CreatedAt
		}},
		{name: "config digest", mutate: func(value *Binding) { value.ConfigDigest = "bad" }},
	}
	for _, test := range validationCases {
		t.Run("validate-"+test.name, func(t *testing.T) {
			value := binding.Clone()
			test.mutate(&value)
			if err := value.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected invalid Binding, got %v", err)
			}
		})
	}

	digestCases := []string{"", "bad", strings.Repeat("a", 63), strings.Repeat("A", 64), strings.Repeat("g", 64)}
	for _, digest := range digestCases {
		if ValidatePublicRouteKeyDigest(digest) == nil {
			t.Fatalf("invalid route digest was accepted: %q", digest)
		}
	}
	if _, err := DigestPublicRouteKey(ChannelWeCom, "\n"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("control route key was accepted: %v", err)
	}
	if _, err := DigestPublicRouteKey(ChannelWeCom, strings.Repeat("x", 1025)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unbounded route key was accepted: %v", err)
	}
	if (Binding{Status: StatusDisabled}).CanTransitionTo(StatusActive) {
		t.Fatal("disabled Binding reported a resumable transition")
	}
	if ValidatePublicRouteKeyDigest(strings.Repeat("g", 64)) == nil {
		t.Fatal("non-hex route digest was accepted")
	}
	if err := validateTenantID("t_80000000000000000000000000"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid ULID padding was accepted: %v", err)
	}
	if err := validateTenantID("t_0000000000000000000000000I"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid Crockford character was accepted: %v", err)
	}
}

func TestRoutingAndScopeValidationBoundaries(t *testing.T) {
	binding := newPreparedTestBinding(t)
	binding.Status = StatusActive
	verified, err := newVerifiedBinding(*binding)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, verified.Clone()) {
		t.Fatal("VerifiedBinding clone changed value state")
	}
	verifiedCases := []struct {
		name   string
		mutate func(*VerifiedBinding)
	}{
		{name: "tenant", mutate: func(value *VerifiedBinding) { value.TenantID = "" }},
		{name: "binding", mutate: func(value *VerifiedBinding) { value.BindingID = "bad" }},
		{name: "version", mutate: func(value *VerifiedBinding) { value.BindingVersion = 0 }},
		{name: "app", mutate: func(value *VerifiedBinding) { value.AppID = "bad" }},
		{name: "channel", mutate: func(value *VerifiedBinding) { value.Channel = Channel("bad") }},
		{name: "provider", mutate: func(value *VerifiedBinding) { value.ProviderAccountID = "" }},
		{name: "digest", mutate: func(value *VerifiedBinding) { value.ConfigDigest = "bad" }},
	}
	for _, test := range verifiedCases {
		t.Run("verified-"+test.name, func(t *testing.T) {
			value := verified
			test.mutate(&value)
			if err := value.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected invalid verified binding, got %v", err)
			}
		})
	}
	if _, err := newVerifiedBinding(Binding{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid Binding was sealed as verified: %v", err)
	}
	suspended := binding.Clone()
	suspended.Status = StatusSuspended
	if _, err := newVerifiedBinding(suspended); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("suspended Binding was sealed as verified: %v", err)
	}

	validSnapshot := testConfigurationSnapshot(t, binding.TenantID)
	validApp := testActiveApp(t, binding.TenantID, binding.AppID)
	for _, test := range []struct {
		name     string
		binding  *Binding
		app      *appmodel.App
		verified VerifiedBinding
		snapshot tenant.ConfigurationSnapshot
	}{
		{name: "nil binding", binding: nil, app: validApp, verified: verified, snapshot: validSnapshot},
		{name: "nil app", binding: binding, app: nil, verified: verified, snapshot: validSnapshot},
		{name: "verified tenant mismatch", binding: binding, app: validApp, verified: func() VerifiedBinding {
			value := verified
			value.TenantID = "t_00000000000000000000000001"
			return value
		}(), snapshot: validSnapshot},
		{name: "verified binding mismatch", binding: binding, app: validApp, verified: func() VerifiedBinding {
			value := verified
			value.BindingID = "cb_00000000000000000000000000"
			return value
		}(), snapshot: validSnapshot},
		{name: "app tenant mismatch", binding: binding, app: func() *appmodel.App {
			value := validApp.Clone()
			value.TenantID = "t_00000000000000000000000001"
			return &value
		}(), verified: verified, snapshot: validSnapshot},
		{name: "zero snapshot", binding: binding, app: validApp, verified: verified, snapshot: tenant.ConfigurationSnapshot{}},
	} {
		t.Run("target-"+test.name, func(t *testing.T) {
			if _, err := NewRoutingTarget(test.snapshot, test.binding, test.app, test.verified); !errors.Is(err, ErrVerificationFailed) {
				t.Fatalf("invalid routing target input was accepted: %v", err)
			}
		})
	}

	identityCases := []IdentityInput{
		{Kind: ConversationDirect, ExternalPeerID: "peer"},
		{ExternalUserID: "user", Kind: ConversationDirect},
		{ExternalUserID: "user", Kind: ConversationGroup},
		{ExternalUserID: "user", Kind: ConversationKind("unknown"), ExternalPeerID: "peer"},
		{ExternalUserID: "user", Kind: ConversationDirect, ExternalPeerID: "peer", ExternalThreadID: "bad\n"},
	}
	target, err := NewRoutingTarget(validSnapshot, binding, validApp, verified)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range identityCases {
		if _, err := target.RunnerIdentity(input); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid identity input was accepted: %+v -> %v", input, err)
		}
	}
	secretScopeCases := []SecretScope{{}, {TenantID: testTenantID}, {TenantID: testTenantID, SecretRef: " secret"}, {TenantID: "bad", SecretRef: "secret"}}
	for _, scope := range secretScopeCases {
		if err := scope.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid secret scope was accepted: %+v -> %v", scope, err)
		}
	}
}

func TestProtocolJSONAndCandidateValidationErrors(t *testing.T) {
	var nilConfiguration *ProtocolConfiguration
	if err := nilConfiguration.UnmarshalJSON([]byte(`{}`)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil protocol receiver returned %v", err)
	}
	var configuration ProtocolConfiguration
	for _, raw := range []string{`{`, `{"wecom":{}} {}`, `{"wecom":{"unknown":"x"}}`} {
		if err := json.Unmarshal([]byte(raw), &configuration); err == nil {
			t.Fatalf("invalid protocol JSON was accepted: %s -> %v", raw, err)
		}
	}
	for _, raw := range []string{`{"wecom":{}} {}`, `{"wecom":{}} trailing`} {
		if err := configuration.UnmarshalJSON([]byte(raw)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("direct invalid protocol JSON was accepted: %s -> %v", raw, err)
		}
	}
	valid := time.Now().UTC()
	digest := strings.Repeat("a", 64)
	candidate, err := NewCandidateBindingContextFromInput(CandidateBindingInput{
		Channel: ChannelWeCom, PublicRouteKeyDigest: digest, BindingVersion: 1, ConfigDigest: digest,
		Purpose: PurposeWebhookVerification, CandidateToken: "token", IssuedAt: valid, ExpiresAt: valid.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	candidateCases := []struct {
		name   string
		mutate func(*CandidateBindingContext)
	}{
		{name: "channel", mutate: func(value *CandidateBindingContext) { value.Channel = Channel("bad") }},
		{name: "route", mutate: func(value *CandidateBindingContext) { value.PublicRouteKeyDigest = "bad" }},
		{name: "version", mutate: func(value *CandidateBindingContext) { value.BindingVersion = 0 }},
		{name: "config", mutate: func(value *CandidateBindingContext) { value.ConfigDigest = "bad" }},
		{name: "config characters", mutate: func(value *CandidateBindingContext) { value.ConfigDigest = strings.Repeat("g", 64) }},
		{name: "purpose", mutate: func(value *CandidateBindingContext) { value.Purpose = "" }},
		{name: "token", mutate: func(value *CandidateBindingContext) { value.CandidateToken = "" }},
		{name: "issued", mutate: func(value *CandidateBindingContext) { value.IssuedAt = time.Time{} }},
		{name: "location", mutate: func(value *CandidateBindingContext) { value.IssuedAt = time.Now().In(time.FixedZone("test", 3600)) }},
		{name: "order", mutate: func(value *CandidateBindingContext) { value.ExpiresAt = value.IssuedAt }},
	}
	for _, test := range candidateCases {
		t.Run("candidate-"+test.name, func(t *testing.T) {
			value := candidate.Clone()
			test.mutate(&value)
			if err := value.Validate(valid); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected invalid candidate, got %v", err)
			}
		})
	}
	future := candidate
	if err := future.Validate(valid.Add(-time.Second)); !errors.Is(err, ErrCandidateUnavailable) {
		t.Fatalf("future candidate was accepted: %v", err)
	}
}

func TestRouteDigestSeparatesChannelAndNeverReturnsRouteKey(t *testing.T) {
	wecomDigest, err := DigestPublicRouteKey(ChannelWeCom, "same-public-key")
	if err != nil {
		t.Fatal(err)
	}
	telegramDigest, err := DigestPublicRouteKey(ChannelTelegram, "same-public-key")
	if err != nil {
		t.Fatal(err)
	}
	if wecomDigest == telegramDigest || len(wecomDigest) != 64 || ValidatePublicRouteKeyDigest(wecomDigest) != nil {
		t.Fatalf("channel route namespaces are not separated: %q %q", wecomDigest, telegramDigest)
	}
	if strings.Contains(wecomDigest, "same-public-key") || strings.Contains(telegramDigest, "same-public-key") {
		t.Fatal("route key was returned in its digest")
	}
}

func TestCandidateLifetimePurposeAndOpaqueHandleBoundaries(t *testing.T) {
	routeDigest, _ := DigestPublicRouteKey(ChannelWeCom, "route")
	configDigest := strings.Repeat("a", 64)
	now := time.Now().UTC()
	candidate, err := NewCandidateBindingContextFromInput(CandidateBindingInput{
		Channel: ChannelWeCom, PublicRouteKeyDigest: routeDigest, BindingVersion: 1, ConfigDigest: configDigest,
		Purpose: PurposeWebhookVerification, CandidateToken: "opaque-token", IssuedAt: now, ExpiresAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := candidate.Validate(now); err != nil {
		t.Fatal(err)
	}
	if err := candidate.Validate(now.Add(time.Second)); !errors.Is(err, ErrCandidateUnavailable) {
		t.Fatalf("expired candidate was accepted: %v", err)
	}
	if _, err := NewCandidateBindingContextFromInput(CandidateBindingInput{
		Channel: ChannelWeCom, PublicRouteKeyDigest: routeDigest, BindingVersion: 1, ConfigDigest: configDigest,
		Purpose: VerificationPurpose(""), CandidateToken: "opaque-token", IssuedAt: now, ExpiresAt: now.Add(time.Second),
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty candidate purpose was accepted: %v", err)
	}
	if _, err := NewCandidateBindingContextFromInput(CandidateBindingInput{
		Channel: ChannelWeCom, PublicRouteKeyDigest: routeDigest, BindingVersion: 1, ConfigDigest: configDigest,
		Purpose: PurposeWebhookVerification, CandidateToken: "opaque-token", IssuedAt: now, ExpiresAt: now.Add(MaxCandidateLifetime + time.Nanosecond),
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unbounded candidate lifetime was accepted: %v", err)
	}

	handle, err := NewScopedVerifierHandle("private-token", PurposeWebhookVerification, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if handle.Token() != "private-token" {
		t.Fatal("resolver could not read its own opaque handle")
	}
	if _, err := NewScopedVerifierHandle("private-token", VerificationPurpose(""), now.Add(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty handle purpose was accepted: %v", err)
	}
}

func TestRepositoryPrepareContractsRejectInvalidChanges(t *testing.T) {
	binding := newPreparedTestBinding(t)
	validUpdate := UpdateConfigurationInput{
		TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version,
		ProviderAccountID: "corp-updated", PublicRouteKeyDigest: binding.PublicRouteKeyDigest,
		AppID: binding.AppID, SecretRef: binding.SecretRef, Protocol: binding.Protocol,
		Metadata: validChangeMetadata(),
	}
	updated, event, err := PrepareConfigurationChange(*binding, validUpdate, binding.UpdatedAt.Add(time.Second).UTC())
	if err != nil || event.EventType != EventConfigurationUpdated || updated.Version != binding.Version+1 || updated.ConfigDigest == binding.ConfigDigest {
		t.Fatalf("valid configuration change failed: binding=%+v event=%+v err=%v", updated, event, err)
	}
	configurationCases := []struct {
		name     string
		current  Binding
		input    UpdateConfigurationInput
		occurred time.Time
		expected error
	}{
		{name: "identity", current: *binding, input: UpdateConfigurationInput{TenantID: "t_00000000000000000000000001", BindingID: binding.BindingID}, occurred: binding.UpdatedAt, expected: ErrNotFound},
		{name: "invalid current", current: func() Binding { value := binding.Clone(); value.Version = 0; return value }(), input: validUpdate, occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "disabled", current: disabledBinding(*binding), input: validUpdate, occurred: binding.UpdatedAt.Add(time.Second), expected: ErrDisabled},
		{name: "version", current: *binding, input: func() UpdateConfigurationInput { value := validUpdate; value.ExpectedVersion++; return value }(), occurred: binding.UpdatedAt, expected: ErrConflict},
		{name: "metadata", current: *binding, input: func() UpdateConfigurationInput { value := validUpdate; value.Metadata = ChangeMetadata{}; return value }(), occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "provider", current: *binding, input: func() UpdateConfigurationInput { value := validUpdate; value.ProviderAccountID = ""; return value }(), occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "route", current: *binding, input: func() UpdateConfigurationInput {
			value := validUpdate
			value.PublicRouteKeyDigest = "bad"
			return value
		}(), occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "app", current: *binding, input: func() UpdateConfigurationInput { value := validUpdate; value.AppID = "bad"; return value }(), occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "secret", current: *binding, input: func() UpdateConfigurationInput { value := validUpdate; value.SecretRef = ""; return value }(), occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "protocol", current: *binding, input: func() UpdateConfigurationInput {
			value := validUpdate
			value.Protocol = ProtocolConfiguration{Telegram: &TelegramProtocolConfiguration{}}
			return value
		}(), occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "zero time", current: *binding, input: validUpdate, occurred: time.Time{}, expected: ErrInvalid},
		{name: "non UTC", current: *binding, input: validUpdate, occurred: time.Now().In(time.FixedZone("test", 3600)), expected: ErrInvalid},
		{name: "before current", current: *binding, input: validUpdate, occurred: binding.UpdatedAt.Add(-time.Second), expected: ErrInvalid},
	}
	for _, test := range configurationCases {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := PrepareConfigurationChange(test.current, test.input, test.occurred); !errors.Is(err, test.expected) {
				t.Fatalf("expected %v, got %v", test.expected, err)
			}
		})
	}

	createdEvent, err := PrepareCreatedChange(*binding, validChangeMetadata())
	if err != nil || createdEvent.EventType != EventCreated {
		t.Fatalf("valid created event failed: %+v %v", createdEvent, err)
	}
	createdCases := []struct {
		name     string
		value    Binding
		metadata ChangeMetadata
		expected error
	}{
		{name: "invalid binding", value: Binding{}, metadata: validChangeMetadata(), expected: ErrInvalid},
		{name: "disabled", value: disabledBinding(*binding), metadata: validChangeMetadata(), expected: ErrInvalid},
		{name: "version", value: func() Binding { value := binding.Clone(); value.Version = 2; return value }(), metadata: validChangeMetadata(), expected: ErrInvalid},
		{name: "updated timestamp", value: func() Binding {
			value := binding.Clone()
			value.UpdatedAt = value.CreatedAt.Add(time.Second)
			return value
		}(), metadata: validChangeMetadata(), expected: ErrInvalid},
		{name: "metadata", value: binding.Clone(), metadata: ChangeMetadata{}, expected: ErrInvalid},
	}
	for _, test := range createdCases {
		t.Run("created-"+test.name, func(t *testing.T) {
			if _, err := PrepareCreatedChange(test.value, test.metadata); !errors.Is(err, test.expected) {
				t.Fatalf("expected %v, got %v", test.expected, err)
			}
		})
	}
}

func TestRepositoryPrepareStatusTransitionsAndFailures(t *testing.T) {
	binding := newPreparedTestBinding(t)
	active, event, err := PrepareStatusChange(*binding, TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, NextStatus: StatusActive, Metadata: validChangeMetadata()}, binding.UpdatedAt.Add(time.Second).UTC())
	if err != nil || event.EventType != EventActivated {
		t.Fatalf("draft activation failed: %+v %+v %v", active, event, err)
	}
	suspended, event, err := PrepareStatusChange(active, TransitionStatusInput{TenantID: active.TenantID, BindingID: active.BindingID, ExpectedVersion: active.Version, NextStatus: StatusSuspended, Metadata: validChangeMetadata()}, active.UpdatedAt.Add(time.Second).UTC())
	if err != nil || event.EventType != EventSuspended {
		t.Fatalf("suspension failed: %+v %+v %v", suspended, event, err)
	}
	resumed, event, err := PrepareStatusChange(suspended, TransitionStatusInput{TenantID: suspended.TenantID, BindingID: suspended.BindingID, ExpectedVersion: suspended.Version, NextStatus: StatusActive, Metadata: validChangeMetadata()}, suspended.UpdatedAt.Add(time.Second).UTC())
	if err != nil || event.EventType != EventResumed {
		t.Fatalf("resume failed: %+v %+v %v", resumed, event, err)
	}
	disabled, event, err := PrepareStatusChange(resumed, TransitionStatusInput{TenantID: resumed.TenantID, BindingID: resumed.BindingID, ExpectedVersion: resumed.Version, NextStatus: StatusDisabled, Metadata: validChangeMetadata()}, resumed.UpdatedAt.Add(time.Second).UTC())
	if err != nil || event.EventType != EventDisabled {
		t.Fatalf("disable failed: %+v %+v %v", disabled, event, err)
	}
	statusCases := []struct {
		name     string
		current  Binding
		input    TransitionStatusInput
		occurred time.Time
		expected error
	}{
		{name: "identity", current: *binding, input: TransitionStatusInput{TenantID: "t_00000000000000000000000001", BindingID: binding.BindingID}, occurred: binding.UpdatedAt, expected: ErrNotFound},
		{name: "invalid current", current: Binding{}, input: TransitionStatusInput{}, occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "disabled", current: disabled, input: TransitionStatusInput{TenantID: disabled.TenantID, BindingID: disabled.BindingID, ExpectedVersion: disabled.Version, NextStatus: StatusActive, Metadata: validChangeMetadata()}, occurred: disabled.UpdatedAt.Add(time.Second), expected: ErrDisabled},
		{name: "version", current: *binding, input: TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: 99, NextStatus: StatusActive, Metadata: validChangeMetadata()}, occurred: binding.UpdatedAt, expected: ErrConflict},
		{name: "transition", current: *binding, input: TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, NextStatus: StatusSuspended, Metadata: validChangeMetadata()}, occurred: binding.UpdatedAt, expected: ErrInvalidTransition},
		{name: "metadata", current: *binding, input: TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, NextStatus: StatusActive}, occurred: binding.UpdatedAt, expected: ErrInvalid},
		{name: "zero time", current: *binding, input: TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, NextStatus: StatusActive, Metadata: validChangeMetadata()}, occurred: time.Time{}, expected: ErrInvalid},
		{name: "before current", current: *binding, input: TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, NextStatus: StatusActive, Metadata: validChangeMetadata()}, occurred: binding.UpdatedAt.Add(-time.Second), expected: ErrInvalid},
	}
	for _, test := range statusCases {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := PrepareStatusChange(test.current, test.input, test.occurred); !errors.Is(err, test.expected) {
				t.Fatalf("expected %v, got %v", test.expected, err)
			}
		})
	}
}

func newPreparedTestBinding(t *testing.T) *Binding {
	t.Helper()
	routeDigest, err := DigestPublicRouteKey(ChannelWeCom, "prepared-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := NewBinding(CreateInput{TenantID: testTenantID, BindingKey: "prepared", Channel: ChannelWeCom, ProviderAccountID: "corp-prepared", PublicRouteKeyDigest: routeDigest, AppID: testAppID, SecretRef: "secret/prepared", Protocol: ProtocolConfiguration{WeCom: &WeComProtocolConfiguration{CorpID: "corp-prepared"}}})
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func disabledBinding(binding Binding) Binding {
	binding.Status = StatusDisabled
	return binding
}

func testConfigurationSnapshot(t *testing.T, tenantID string) tenant.ConfigurationSnapshot {
	t.Helper()
	root, err := tenant.NewTenant(tenant.CreateInput{TenantKey: "snapshot-test", DisplayName: "Snapshot Test", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	root.TenantID = tenantID
	if err := root.Validate(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func testActiveApp(t *testing.T, tenantID, appID string) *appmodel.App {
	t.Helper()
	appRoot, err := appmodel.NewApp(appmodel.CreateInput{TenantID: tenantID, AppKey: "snapshot-app", DisplayName: "Snapshot App", Description: "test"})
	if err != nil {
		t.Fatal(err)
	}
	appRoot.AppID = appID
	revision := int64(1)
	appRoot.Status = appmodel.StatusActive
	appRoot.CurrentRevision = &revision
	appRoot.Version = 2
	appRoot.UpdatedAt = appRoot.CreatedAt.Add(time.Second)
	if err := appRoot.Validate(); err != nil {
		t.Fatal(err)
	}
	return appRoot
}

func validChangeMetadata() ChangeMetadata {
	return ChangeMetadata{ActorType: "test", ActorID: "test-actor", Reason: "test change", CorrelationID: "corr-1"}
}

func reflectField(value any, name string) (any, bool) {
	field, ok := reflect.TypeOf(value).FieldByName(name)
	return field, ok
}
