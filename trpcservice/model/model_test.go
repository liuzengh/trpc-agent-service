package model

import (
	"errors"
	"math"
	"net/url"
	"strings"
	"testing"
	"time"
)

const (
	modelTestTenantOne = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestNewProfileNormalizesAndDefensivelyCopies(t *testing.T) {
	catalog := modelTestCatalog(t)
	temperature := 0.2
	maxTokens := 128
	options := map[string]string{"mode": " SAFE "}
	profile, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: " Primary ", DisplayName: " Primary model ",
		Description: " Shared deterministic model ", Configuration: Configuration{
			Provider: "FAKE", Model: "DETERMINISTIC", Options: options,
			Generation: GenerationConfig{Temperature: &temperature, MaxOutputTokens: &maxTokens},
		},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if profile.ProfileKey != "primary" || profile.Configuration.Provider != "fake" || profile.Configuration.Model != "deterministic" || profile.Configuration.Options["mode"] != "safe" {
		t.Fatalf("profile was not normalized: %+v", profile)
	}
	if profile.Status != StatusActive || profile.SchemaVersion != SchemaVersionV1 || profile.Version != 1 {
		t.Fatalf("profile defaults = status %q schema %d version %d", profile.Status, profile.SchemaVersion, profile.Version)
	}
	if profile.CreatedAt.IsZero() || !profile.CreatedAt.Equal(profile.UpdatedAt) || profile.CreatedAt.Location() != time.UTC {
		t.Fatalf("timestamps are not initialized in UTC: created=%v updated=%v", profile.CreatedAt, profile.UpdatedAt)
	}
	if err := profile.Validate(catalog); err != nil {
		t.Fatal(err)
	}

	options["mode"] = "fast"
	temperature = 1.5
	maxTokens = 999
	if profile.Configuration.Options["mode"] != "safe" || *profile.Configuration.Generation.Temperature != 0.2 || *profile.Configuration.Generation.MaxOutputTokens != 128 {
		t.Fatal("NewProfile retained mutable caller configuration")
	}
	clone := profile.Clone()
	clone.Configuration.Options["mode"] = "fast"
	*clone.Configuration.Generation.Temperature = 1.1
	if profile.Configuration.Options["mode"] != "safe" || *profile.Configuration.Generation.Temperature != 0.2 {
		t.Fatal("Profile.Clone leaked nested configuration mutation")
	}
}

func TestProfileSchemaRejectsUnknownAndCredentialBearingConfiguration(t *testing.T) {
	catalog := modelTestCatalog(t)
	tests := []struct {
		name  string
		input Configuration
	}{
		{name: "unknown provider", input: Configuration{Provider: "unknown", Model: "chat"}},
		{name: "unknown model", input: Configuration{Provider: "public", Model: "unknown"}},
		{name: "unknown option", input: Configuration{Provider: "fake", Model: "deterministic", Options: map[string]string{"unknown": "value"}}},
		{name: "userinfo endpoint", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://user:password@example.test/v1"}},
		{name: "query endpoint", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://example.test/v1?api_key=password"}},
		{name: "fragment endpoint", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://example.test/v1#fragment"}},
		{name: "wrong endpoint scheme", input: Configuration{Provider: "public", Model: "chat", Endpoint: "http://example.test/v1"}},
		{name: "malformed endpoint host", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://example..test/v1"}},
		{name: "invalid endpoint port", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://example.test:0/v1"}},
		{name: "invalid generation", input: Configuration{Provider: "fake", Model: "deterministic", Generation: GenerationConfig{Temperature: float64Pointer(math.NaN())}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewProfile(CreateInput{TenantID: modelTestTenantOne, ProfileKey: test.name, DisplayName: "Model", Configuration: test.input}, catalog); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}

func TestProviderCatalogRejectsSensitiveOptionSchemas(t *testing.T) {
	for _, key := range []string{"api_key", "access_key", "private_key", "connection_string", "client_password", "db_pwd", "signing_secret", "provider_dsn"} {
		_, err := NewProviderCatalog(ProviderSpec{
			Provider: "unsafe", Models: []string{"chat"}, EndpointPolicy: FieldForbidden,
			SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{key: {Kind: OptionString}},
		})
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("sensitive option schema %q error = %v", key, err)
		}
	}
}

func TestProfileStateTransitionsAndSchemaPolicies(t *testing.T) {
	catalog := modelTestCatalog(t)
	if _, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: "required", DisplayName: "Required",
		Configuration: Configuration{Provider: "secured", Model: "chat"},
	}, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing required secret error = %v", err)
	}
	if _, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: "forbidden", DisplayName: "Forbidden",
		Configuration: Configuration{Provider: "fake", Model: "deterministic", SecretRef: "secret://tenant/model"},
	}, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forbidden secret error = %v", err)
	}
	profile, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: "lifecycle", DisplayName: "Lifecycle",
		Configuration: Configuration{Provider: "fake", Model: "deterministic"},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !profile.CanTransitionTo(StatusSuspended) || profile.CanTransitionTo(StatusActive) || profile.CanTransitionTo(StatusDisabled) == false {
		t.Fatal("unexpected active lifecycle transitions")
	}
	profile.Status = StatusSuspended
	if !profile.CanTransitionTo(StatusActive) || !profile.CanTransitionTo(StatusDisabled) {
		t.Fatal("unexpected suspended lifecycle transitions")
	}
	profile.Status = StatusDisabled
	if profile.CanAcceptExecution() || profile.CanTransitionTo(StatusActive) {
		t.Fatal("disabled Profile remained executable or resumable")
	}
}

func modelTestCatalog(t *testing.T) *ProviderCatalog {
	t.Helper()
	defaultMode := "safe"
	catalog, err := NewProviderCatalog(
		ProviderSpec{
			Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: FieldForbidden,
			SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{
				"mode": {Kind: OptionEnum, DefaultValue: &defaultMode, AllowedValues: []string{"fast", "safe"}},
			},
		},
		ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test", "127.0.0.1", "2001:db8::1"}, SecretRefPolicy: FieldOptional},
		ProviderSpec{Provider: "secured", Models: []string{"chat"}, EndpointPolicy: FieldRequired, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test", "127.0.0.1", "2001:db8::1"}, SecretRefPolicy: FieldRequired},
	)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func float64Pointer(value float64) *float64 { return &value }

func TestNewProfileAndValidateRejectInvalidState(t *testing.T) {
	catalog := modelTestCatalog(t)
	base := CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: "valid-profile", DisplayName: "Valid Profile",
		Configuration: Configuration{Provider: "fake", Model: "deterministic"},
	}
	tests := []struct {
		name   string
		mutate func(*CreateInput)
	}{
		{name: "invalid tenant", mutate: func(input *CreateInput) { input.TenantID = "tenant" }},
		{name: "invalid profile key", mutate: func(input *CreateInput) { input.ProfileKey = "x" }},
		{name: "empty display name", mutate: func(input *CreateInput) { input.DisplayName = " " }},
		{name: "disabled initial status", mutate: func(input *CreateInput) { input.Status = StatusDisabled }},
		{name: "unknown initial status", mutate: func(input *CreateInput) { input.Status = Status("retired") }},
		{name: "unsupported schema", mutate: func(input *CreateInput) { input.SchemaVersion = 99 }},
		{name: "missing catalog", mutate: func(input *CreateInput) { input.Configuration.Provider = "fake" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := base
			test.mutate(&input)
			catalogForTest := catalog
			if test.name == "missing catalog" {
				catalogForTest = nil
			}
			if _, err := NewProfile(input, catalogForTest); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}

	profile, err := NewProfile(base, catalog)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name   string
		mutate func(*Profile)
	}{
		{name: "unnormalized key", mutate: func(profile *Profile) { profile.ProfileKey = "Valid-Profile" }},
		{name: "unnormalized metadata", mutate: func(profile *Profile) { profile.DisplayName = " Valid Profile " }},
		{name: "unknown status", mutate: func(profile *Profile) { profile.Status = Status("retired") }},
		{name: "unnormalized configuration", mutate: func(profile *Profile) { profile.Configuration.Options["mode"] = "fast" }},
		{name: "wrong digest", mutate: func(profile *Profile) { profile.ContentDigest = "wrong" }},
		{name: "invalid version", mutate: func(profile *Profile) { profile.Version = 0 }},
		{name: "zero created time", mutate: func(profile *Profile) { profile.CreatedAt = time.Time{} }},
		{name: "non UTC timestamps", mutate: func(profile *Profile) {
			zone := time.FixedZone("test", 3600)
			profile.CreatedAt = profile.CreatedAt.In(zone)
			profile.UpdatedAt = profile.UpdatedAt.In(zone)
		}},
		{name: "updated before created", mutate: func(profile *Profile) { profile.UpdatedAt = profile.CreatedAt.Add(-time.Second) }},
	}
	for _, test := range mutations {
		t.Run("validate/"+test.name, func(t *testing.T) {
			candidate := profile.Clone()
			test.mutate(&candidate)
			if err := candidate.Validate(catalog); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}

func TestProviderCatalogRejectsMalformedSchemasAndCompilesOptionDefaults(t *testing.T) {
	if _, err := NewProviderCatalog(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty catalog error = %v", err)
	}
	duplicate := ProviderSpec{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}
	if _, err := NewProviderCatalog(duplicate, duplicate); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate provider error = %v", err)
	}

	invalidProviders := []ProviderSpec{
		{Provider: "Fake", Models: []string{"chat"}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"Chat"}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat", "chat"}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldPolicy("bad"), SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"HTTPS"}, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"https", "https"}, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldForbidden, EndpointSchemes: []string{"https"}, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldOptional, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"https"}, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldForbidden, EndpointHosts: []string{"example.test"}, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"Example.test"}, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test", "example.test"}, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example..test"}, SecretRefPolicy: FieldForbidden},
		{Provider: "fake", Models: []string{"chat"}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"bad key": {Kind: OptionString}}},
	}
	for index, spec := range invalidProviders {
		if _, err := NewProviderCatalog(spec); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid provider schema %d error = %v", index, err)
		}
	}

	min, max := int64(1), int64(10)
	defaultInt := " 5 "
	defaultBool := " TRUE "
	compiled, err := compileOptionSpec(OptionSpec{Kind: OptionInteger, DefaultValue: &defaultInt, MinInteger: &min, MaxInteger: &max})
	if err != nil || compiled.DefaultValue == nil || *compiled.DefaultValue != "5" {
		t.Fatalf("integer option compilation = %+v, %v", compiled, err)
	}
	compiled, err = compileOptionSpec(OptionSpec{Kind: OptionBoolean, DefaultValue: &defaultBool})
	if err != nil || compiled.DefaultValue == nil || *compiled.DefaultValue != "true" {
		t.Fatalf("boolean option compilation = %+v, %v", compiled, err)
	}
	if compiled.DefaultValue == &defaultBool {
		t.Fatal("option compilation retained default pointer")
	}

	invalidOptions := []OptionSpec{
		{Kind: OptionKind("unknown")},
		{Kind: OptionString, Required: true, DefaultValue: stringPointer("value")},
		{Kind: OptionString, MinInteger: &min},
		{Kind: OptionInteger, MinInteger: &max, MaxInteger: &min},
		{Kind: OptionEnum},
		{Kind: OptionEnum, AllowedValues: []string{"fast", "fast"}},
		{Kind: OptionEnum, AllowedValues: []string{"fast\nvalue"}},
		{Kind: OptionString, AllowedValues: []string{"value"}},
		{Kind: OptionBoolean, DefaultValue: stringPointer("not-bool")},
		{Kind: OptionInteger, DefaultValue: stringPointer("99"), MaxInteger: &min},
	}
	for index, spec := range invalidOptions {
		if _, err := compileOptionSpec(spec); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid option schema %d error = %v", index, err)
		}
	}
}

func TestProviderConfigurationBoundaryNormalization(t *testing.T) {
	public := ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test", "127.0.0.1", "2001:db8::1"}, SecretRefPolicy: FieldOptional}
	secured := ProviderSpec{Provider: "secured", Models: []string{"chat"}, EndpointPolicy: FieldRequired, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test", "127.0.0.1", "2001:db8::1"}, SecretRefPolicy: FieldRequired}
	catalog, err := NewProviderCatalog(public, secured)
	if err != nil {
		t.Fatal(err)
	}
	assertProviderConfigurationNormalization(t, catalog)
	assertProviderEndpointBoundaries(t, catalog)
	assertProviderEndpointPrimitiveHelpers(t)
}

func assertProviderConfigurationNormalization(t *testing.T, catalog *ProviderCatalog) {
	t.Helper()
	normalized, err := catalog.NormalizeConfiguration(Configuration{Provider: " PUBLIC ", Model: " CHAT ", Endpoint: " HTTPS://Example.TEST/v1 "})
	if err != nil || normalized.Provider != "public" || normalized.Model != "chat" || normalized.Endpoint != "https://example.test/v1" {
		t.Fatalf("endpoint normalization = %+v, %v", normalized, err)
	}
	if optional, err := catalog.NormalizeConfiguration(Configuration{Provider: "public", Model: "chat"}); err != nil || optional.Endpoint != "" {
		t.Fatalf("optional endpoint normalization = %+v, %v", optional, err)
	}
	if _, err := catalog.NormalizeConfiguration(Configuration{Provider: "secured", Model: "chat"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("required endpoint error = %v", err)
	}
}

func assertProviderEndpointBoundaries(t *testing.T, catalog *ProviderCatalog) {
	t.Helper()

	invalidEndpoints := []string{
		"://bad", "https://", "http://example.test", "https://user:password@example.test/v1",
		"https://example.test/v1?token=value", "https://example.test/v1#fragment", "https://example.test:",
		"https://example.test:0", "https://example.test:65536", "https://example..test", "https://-example.test",
		"https://example-.test", "https://example_test", "https://example.test,other",
		"https://attacker.example", "https://example.test/%00", "https://example.test/%0A",
	}
	for _, endpoint := range invalidEndpoints {
		if _, err := catalog.NormalizeConfiguration(Configuration{Provider: "public", Model: "chat", Endpoint: endpoint}); !errors.Is(err, ErrInvalid) {
			t.Errorf("endpoint %q error = %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://127.0.0.1:443", "https://[2001:db8::1]:443", "https://[2001:db8::1]", "https://example.test:443"} {
		if _, err := catalog.NormalizeConfiguration(Configuration{Provider: "public", Model: "chat", Endpoint: endpoint}); err != nil {
			t.Errorf("valid endpoint %q error = %v", endpoint, err)
		}
	}
}

func assertProviderEndpointPrimitiveHelpers(t *testing.T) {
	t.Helper()
	if _, err := normalizeEndpoint("https://example.test", FieldForbidden, nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forbidden endpoint error = %v", err)
	}
	if _, err := normalizeEndpoint("", FieldRequired, nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty required endpoint error = %v", err)
	}
	if _, err := normalizeEndpoint("https://example.test", FieldOptional, map[string]struct{}{"https": {}}, map[string]struct{}{"example.test": {}}); err != nil {
		t.Fatal(err)
	}
	parsed := &url.URL{Scheme: "https", Host: "example.test:443"}
	if err := normalizeEndpointAuthority(parsed); err != nil || parsed.Host != "example.test:443" {
		t.Fatalf("authority normalization = %v, %q", err, parsed.Host)
	}

	for _, test := range []struct {
		host string
		want bool
	}{
		{host: "example.test", want: true}, {host: "example.test.", want: true}, {host: "", want: false},
		{host: ".", want: false}, {host: "-example.test", want: false}, {host: "example-.test", want: false},
		{host: "example_test", want: false}, {host: strings.Repeat("a", 64) + ".test", want: false},
	} {
		if got := validDNSHostname(test.host); got != test.want {
			t.Errorf("validDNSHostname(%q) = %v, want %v", test.host, got, test.want)
		}
	}
	if validDNSHostname(strings.Repeat("a", 254)) {
		t.Fatal("overlong hostname was accepted")
	}
	if asciiLetterOrDigit('a') == false || asciiLetterOrDigit('9') == false || asciiLetterOrDigit('-') {
		t.Fatal("asciiLetterOrDigit classification is wrong")
	}
}

func TestOptionSecretGenerationAndPrimitiveValidation(t *testing.T) {
	specs := map[string]OptionSpec{
		"text":         {Kind: OptionString},
		"flag":         {Kind: OptionBoolean},
		"count":        {Kind: OptionInteger, MinInteger: int64Pointer(1), MaxInteger: int64Pointer(3)},
		"mode":         {Kind: OptionEnum, AllowedValues: []string{"safe", "fast"}},
		"with-default": {Kind: OptionString, DefaultValue: stringPointer("fallback")},
		"required":     {Kind: OptionString, Required: true},
	}
	assertOptionNormalization(t, specs)
	assertSecretAndGenerationNormalization(t)
	assertModelPrimitiveValidation(t)
}

func assertOptionNormalization(t *testing.T, specs map[string]OptionSpec) {
	t.Helper()
	options, err := normalizeOptions(map[string]string{"text": " value ", "flag": " TRUE ", "count": "2", "mode": " FAST ", "required": "present"}, specs)
	if err != nil || options["text"] != "value" || options["flag"] != "true" || options["count"] != "2" || options["mode"] != "fast" || options["with-default"] != "fallback" {
		t.Fatalf("option normalization = %+v, %v", options, err)
	}
	invalidOptionInputs := []map[string]string{
		{"TEXT": "value"}, {"api-key": "value"}, {"unknown": "value"}, {"flag": "not-bool"},
		{"count": "0"}, {"mode": "other"}, {"text": "bad\nvalue"},
	}
	for _, input := range invalidOptionInputs {
		if _, err := normalizeOptions(input, specs); !errors.Is(err, ErrInvalid) {
			t.Errorf("options %v error = %v", input, err)
		}
	}
	if _, err := normalizeOptionValue("value", OptionSpec{Kind: OptionKind("bad")}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown option kind value error = %v", err)
	}
	if _, err := normalizeOptions(nil, map[string]OptionSpec{"required": {Kind: OptionString, Required: true}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing required option error = %v", err)
	}
}

func assertSecretAndGenerationNormalization(t *testing.T) {
	t.Helper()
	if _, err := normalizeSecretRef(" ", FieldRequired); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing required secret error = %v", err)
	}
	for _, value := range []string{"secret://ref", "secret://ref with-space", "secret://ref\nvalue", strings.Repeat("r", maxSecretRefLen+1)} {
		if _, err := normalizeSecretRef(value, FieldOptional); value == "secret://ref" {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, ErrInvalid) {
			t.Errorf("secret ref %q error = %v", value, err)
		}
	}
	if _, err := normalizeSecretRef("secret://ref", FieldForbidden); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forbidden secret error = %v", err)
	}
}

func assertModelPrimitiveValidation(t *testing.T) {
	t.Helper()
	assertGenerationValidation(t)
	assertMetadataAndProfileKeyValidation(t)
	assertModelKeyClassification(t)
	assertModelCloneAndIDValidation(t)
}

func assertGenerationValidation(t *testing.T) {
	t.Helper()

	validTemperature, validTopP, validTokens := 0.5, 0.8, 32
	generation, err := normalizeGeneration(GenerationConfig{Temperature: &validTemperature, TopP: &validTopP, MaxOutputTokens: &validTokens})
	if err != nil || generation.Temperature == &validTemperature || *generation.Temperature != validTemperature {
		t.Fatalf("generation normalization = %+v, %v", generation, err)
	}
	invalidGenerations := []GenerationConfig{
		{Temperature: float64Pointer(-1)}, {Temperature: float64Pointer(3)}, {Temperature: float64Pointer(math.Inf(1))},
		{TopP: float64Pointer(-1)}, {TopP: float64Pointer(2)}, {TopP: float64Pointer(math.NaN())},
		{MaxOutputTokens: intPointer(0)}, {MaxOutputTokens: intPointer(1_000_001)},
	}
	for _, generation := range invalidGenerations {
		if _, err := normalizeGeneration(generation); !errors.Is(err, ErrInvalid) {
			t.Errorf("generation %+v error = %v", generation, err)
		}
	}
}

func assertMetadataAndProfileKeyValidation(t *testing.T) {
	t.Helper()
	if _, _, err := normalizeMetadata("", ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty metadata error = %v", err)
	}
	if _, _, err := normalizeMetadata("valid", strings.Repeat("x", 2001)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("long description error = %v", err)
	}
	for _, key := range []string{"", "1bad", "bad key", "bad/"} {
		if _, err := normalizeProfileKey(key); !errors.Is(err, ErrInvalid) {
			t.Errorf("profile key %q error = %v", key, err)
		}
	}
	if normalized, err := normalizeProfileKey(" Primary-1 "); err != nil || normalized != "primary-1" {
		t.Fatalf("profile key normalization = %q, %v", normalized, err)
	}
}

func assertModelKeyClassification(t *testing.T) {
	t.Helper()
	if !validName("provider.v1") || validName("Provider") || validName("bad/name") {
		t.Fatal("validName classification is wrong")
	}
	if !validOptionKey("option_1") || validOptionKey("bad key") {
		t.Fatal("validOptionKey classification is wrong")
	}
	for _, key := range []string{"api-key", "api_key", "access_key", "private-key", "connection_string", "custom-token-value", "client_password", "db_pwd", "signing_secret", "provider_dsn"} {
		if !sensitiveOptionKey(key) {
			t.Errorf("sensitiveOptionKey(%q) = false", key)
		}
	}
	if sensitiveOptionKey("mode") || sensitiveOptionKey("format") {
		t.Fatal("sensitiveOptionKey classification is wrong")
	}
	if !hasControl("line\n") || hasControl("plain") || !hasSpace("has space") || hasSpace("compact") {
		t.Fatal("control/space classification is wrong")
	}

}

func assertModelCloneAndIDValidation(t *testing.T) {
	t.Helper()
	if cloneStringMap(nil) != nil || cloneString(nil) != nil || cloneInt64(nil) != nil {
		t.Fatal("nil clone helpers changed nil semantics")
	}
	value := "value"
	integer := int64(3)
	if cloneString(&value) == &value || cloneInt64(&integer) == &integer {
		t.Fatal("clone helpers retained pointer identity")
	}
	if err := validateCrockfordID(modelTestTenantOne, "t_", "tenant"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"tenant", "t_short", "t_81ARZ3NDEKTSV4RRFFQ69G5FAV", "t_01ARZ3NDEKTSV4RRFFQ69G5FAV0", "t_01ARZ3NDEKTSV4RRFFQ69G5FAL"} {
		if err := validateCrockfordID(id, "t_", "tenant"); !errors.Is(err, ErrInvalid) {
			t.Errorf("invalid Crockford id %q error = %v", id, err)
		}
	}
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }
func intPointer(value int) *int          { return &value }
