package backend

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
)

const testTenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestNewProfileNormalizesConfiguration(t *testing.T) {
	catalog := newTestCatalog(t)
	inputOptions := map[string]string{
		"database":  " agent ",
		"pool_size": "010",
		"read_only": "TRUE",
		"ssl_mode":  " REQUIRE ",
	}
	profile, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: " Primary-Data ",
		DisplayName: " Primary data ", Description: " Shared stores ",
		Bindings: []CapabilityBinding{
			{Capability: CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": " durable "}},
			{
				Capability: CapabilitySession, Provider: "POSTGRES",
				Endpoint: " POSTGRES://DB.EXAMPLE.COM:5432 ", Options: inputOptions,
				SecretRef: " secret://tenant/database ",
			},
		},
	}, catalog)
	if err != nil {
		t.Fatalf("NewProfile() error = %v", err)
	}
	assertNormalizedProfile(t, profile, inputOptions, catalog)
}

func assertNormalizedProfile(t *testing.T, profile *Profile, inputOptions map[string]string, catalog *ProviderCatalog) {
	t.Helper()
	assertProfileMetadata(t, profile)
	assertProfileBindings(t, profile, inputOptions, catalog)
}

func assertProfileMetadata(t *testing.T, profile *Profile) {
	t.Helper()
	if profile.ProfileKey != "primary-data" || profile.DisplayName != "Primary data" || profile.Description != "Shared stores" {
		t.Fatalf("Profile metadata was not normalized: %#v", profile)
	}
	if profile.Status != StatusActive || profile.SchemaVersion != 1 || profile.Version != 1 {
		t.Fatalf("Profile defaults = status %q schema %d version %d", profile.Status, profile.SchemaVersion, profile.Version)
	}
	if matched := regexp.MustCompile(`^bp_[0-7][0-9A-HJKMNP-TV-Z]{25}$`).MatchString(profile.ProfileID); !matched {
		t.Fatalf("ProfileID = %q", profile.ProfileID)
	}
	if profile.CreatedAt.IsZero() || !profile.CreatedAt.Equal(profile.UpdatedAt) || profile.CreatedAt.Location() != time.UTC {
		t.Fatalf("timestamps are not initialized in UTC: created=%v updated=%v", profile.CreatedAt, profile.UpdatedAt)
	}
	if len(profile.Bindings) != 2 || profile.Bindings[0].Capability != CapabilitySession || profile.Bindings[1].Capability != CapabilityMemory {
		t.Fatalf("Bindings were not canonically sorted: %#v", profile.Bindings)
	}
}

func assertProfileBindings(t *testing.T, profile *Profile, inputOptions map[string]string, catalog *ProviderCatalog) {
	t.Helper()
	session := profile.Bindings[0]
	if session.Provider != "postgres" || session.Endpoint != "postgres://db.example.com:5432" || session.SecretRef != "secret://tenant/database" {
		t.Fatalf("Session binding was not normalized: %#v", session)
	}
	wantOptions := map[string]string{
		"database": "agent", "pool_size": "10", "read_only": "true", "ssl_mode": "require",
	}
	if !stringMapsEqual(session.Options, wantOptions) {
		t.Fatalf("Session options = %#v, want %#v", session.Options, wantOptions)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(profile.ContentDigest) {
		t.Fatalf("ContentDigest = %q", profile.ContentDigest)
	}
	if err := profile.Validate(catalog); err != nil {
		t.Fatalf("Profile.Validate() error = %v", err)
	}

	inputOptions["database"] = "mutated"
	if profile.Bindings[0].Options["database"] != "agent" {
		t.Fatal("NewProfile retained the caller's option map")
	}
}

func TestNewProfileNormalizesUnicodeWhitespace(t *testing.T) {
	catalog := newTestCatalog(t)
	profile, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "unicode", DisplayName: "\u00a0Primary\u2003",
		Description: "\u202fDescription\u3000", Bindings: sessionBinding(),
	}, catalog)
	if err != nil {
		t.Fatalf("NewProfile() error = %v", err)
	}
	if profile.DisplayName != "Primary" || profile.Description != "Description" {
		t.Fatalf("Unicode metadata was not normalized: display=%q description=%q", profile.DisplayName, profile.Description)
	}
	if err := profile.Validate(catalog); err != nil {
		t.Fatalf("Profile.Validate() error = %v", err)
	}
}

func TestProfileLifecycleAndSessionInvariant(t *testing.T) {
	catalog := newTestCatalog(t)
	memoryOnly := []CapabilityBinding{{Capability: CapabilityMemory, Provider: "inmemory"}}

	if _, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "active", DisplayName: "Active", Bindings: memoryOnly,
	}, catalog); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "session") {
		t.Fatalf("active Profile without Session error = %v", err)
	}
	assertProfileLifecycleStates(t, catalog, memoryOnly)
}

func assertProfileLifecycleStates(t *testing.T, catalog *ProviderCatalog, memoryOnly []CapabilityBinding) {
	t.Helper()
	suspended, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "suspended", DisplayName: "Suspended",
		Status: StatusSuspended, Bindings: memoryOnly,
	}, catalog)
	if err != nil {
		t.Fatalf("suspended Profile without Session error = %v", err)
	}
	if suspended.CanAcceptExecution() {
		t.Fatal("suspended Profile accepted execution")
	}
	if !suspended.CanTransitionTo(StatusActive) || !suspended.CanTransitionTo(StatusDisabled) || suspended.CanTransitionTo(StatusSuspended) {
		t.Fatalf("unexpected suspended transitions")
	}
	active := newTestProfile(t, catalog)
	if !active.CanAcceptExecution() || !active.CanTransitionTo(StatusSuspended) || !active.CanTransitionTo(StatusDisabled) || active.CanTransitionTo(StatusActive) {
		t.Fatalf("unexpected active lifecycle behavior")
	}
	active.Status = StatusDisabled
	if active.CanAcceptExecution() || active.CanTransitionTo(StatusActive) || active.CanTransitionTo(StatusSuspended) {
		t.Fatalf("disabled Profile is not terminal")
	}
	if err := active.Validate(catalog); err != nil {
		t.Fatalf("disabled retained Profile should validate: %v", err)
	}
	disabledWithoutBindings := active.Clone()
	disabledWithoutBindings.Bindings = nil
	disabledWithoutBindings.ContentDigest = contentDigest(disabledWithoutBindings.SchemaVersion, disabledWithoutBindings.Bindings)
	if err := disabledWithoutBindings.Validate(catalog); err != nil {
		t.Fatalf("disabled Profile without bindings should validate: %v", err)
	}
	if disabledWithoutBindings.CanAcceptExecution() || disabledWithoutBindings.CanTransitionTo(StatusActive) || disabledWithoutBindings.CanTransitionTo(StatusSuspended) {
		t.Fatalf("disabled Profile without bindings is not terminal")
	}
	if _, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "disabled", DisplayName: "Disabled",
		Status: StatusDisabled, Bindings: sessionBinding(),
	}, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("creating disabled Profile error = %v", err)
	}
}

func TestProfileDigestIsCanonicalAndSemantic(t *testing.T) {
	catalog := newTestCatalog(t)
	first, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "first", DisplayName: "First",
		Bindings: append([]CapabilityBinding{
			{Capability: CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": "memory"}},
		}, sessionBinding()...),
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "second", DisplayName: "Different display",
		Bindings: []CapabilityBinding{
			sessionBinding()[0],
			{Capability: CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": "memory"}},
		},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if first.ContentDigest != second.ContentDigest {
		t.Fatalf("equivalent configurations have different digests: %s != %s", first.ContentDigest, second.ContentDigest)
	}
	changed := sessionBinding()
	changed[0].SecretRef = "secret://tenant/database-next"
	third, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "third", DisplayName: "First", Bindings: changed,
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if first.ContentDigest == third.ContentDigest {
		t.Fatal("changing SecretRef did not change the content digest")
	}
}

func TestProfileCloneAndValidateDetectMutation(t *testing.T) {
	catalog := newTestCatalog(t)
	profile := newTestProfile(t, catalog)
	clone := profile.Clone()
	clone.Bindings[0].Options["database"] = "other"
	if profile.Bindings[0].Options["database"] != "agent" {
		t.Fatal("Profile.Clone leaked the options map")
	}

	mutated := profile.Clone()
	mutated.Bindings[0].Options["database"] = "other"
	if err := mutated.Validate(catalog); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("mutated Profile validation error = %v", err)
	}
	mutated = profile.Clone()
	mutated.Bindings[0], mutated.Bindings[1] = mutated.Bindings[1], mutated.Bindings[0]
	if err := mutated.Validate(catalog); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "normalized") {
		t.Fatalf("unsorted Profile validation error = %v", err)
	}
}

func TestNewProfileBoundaryValidation(t *testing.T) {
	catalog := newTestCatalog(t)
	valid := CreateInput{
		TenantID: testTenantID, ProfileKey: "primary", DisplayName: "Primary", Bindings: sessionBinding(),
	}
	tests := []struct {
		name   string
		mutate func(*CreateInput)
	}{
		{name: "tenant ID", mutate: func(input *CreateInput) { input.TenantID = "t_bad" }},
		{name: "profile key", mutate: func(input *CreateInput) { input.ProfileKey = "1bad" }},
		{name: "display name", mutate: func(input *CreateInput) { input.DisplayName = " " }},
		{name: "description", mutate: func(input *CreateInput) { input.Description = strings.Repeat("x", 2001) }},
		{name: "schema", mutate: func(input *CreateInput) { input.SchemaVersion = 2 }},
		{name: "empty bindings", mutate: func(input *CreateInput) { input.Status = StatusSuspended; input.Bindings = nil }},
		{name: "unknown status", mutate: func(input *CreateInput) { input.Status = "unknown" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			input.Bindings = cloneBindings(valid.Bindings)
			test.mutate(&input)
			if _, err := NewProfile(input, catalog); !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewProfile() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestProfileValidateRejectsCorruptRoot(t *testing.T) {
	catalog := newTestCatalog(t)
	profile := newTestProfile(t, catalog)
	tests := []struct {
		name   string
		mutate func(*Profile)
	}{
		{name: "tenant ID", mutate: func(profile *Profile) { profile.TenantID = "t_bad" }},
		{name: "profile ID", mutate: func(profile *Profile) { profile.ProfileID = "bp_bad" }},
		{name: "profile key grammar", mutate: func(profile *Profile) { profile.ProfileKey = "bad_key" }},
		{name: "key normalization", mutate: func(profile *Profile) { profile.ProfileKey = "Primary" }},
		{name: "metadata grammar", mutate: func(profile *Profile) { profile.DisplayName = "" }},
		{name: "metadata normalization", mutate: func(profile *Profile) { profile.DisplayName += " " }},
		{name: "status", mutate: func(profile *Profile) { profile.Status = "unknown" }},
		{name: "catalog", mutate: func(profile *Profile) { profile.SchemaVersion = 2 }},
		{name: "active session", mutate: func(profile *Profile) {
			profile.Bindings = profile.Bindings[1:]
			profile.ContentDigest = contentDigest(profile.SchemaVersion, profile.Bindings)
		}},
		{name: "version", mutate: func(profile *Profile) { profile.Version = 0 }},
		{name: "created time", mutate: func(profile *Profile) { profile.CreatedAt = time.Time{} }},
		{name: "time order", mutate: func(profile *Profile) { profile.UpdatedAt = profile.CreatedAt.Add(-time.Second) }},
		{name: "created time zone", mutate: func(profile *Profile) { profile.CreatedAt = profile.CreatedAt.In(time.FixedZone("UTC+8", 8*60*60)) }},
		{name: "updated time zone", mutate: func(profile *Profile) { profile.UpdatedAt = profile.UpdatedAt.In(time.FixedZone("UTC-5", -5*60*60)) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := profile.Clone()
			test.mutate(&corrupt)
			if err := corrupt.Validate(catalog); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestCapabilityCanonicalOrderCoversClosedSet(t *testing.T) {
	catalog := newTestCatalog(t)
	bindings := []CapabilityBinding{
		{Capability: CapabilityAudit, Provider: "inmemory"},
		{Capability: CapabilityArtifact, Provider: "inmemory"},
		{Capability: CapabilityKnowledge, Provider: "inmemory"},
		{Capability: CapabilitySummary, Provider: "inmemory"},
		{Capability: CapabilityMemory, Provider: "inmemory"},
		{Capability: CapabilitySession, Provider: "inmemory"},
	}
	normalized, err := catalog.NormalizeBindings(bindings)
	if err != nil {
		t.Fatal(err)
	}
	want := []Capability{CapabilitySession, CapabilityMemory, CapabilitySummary, CapabilityKnowledge, CapabilityArtifact, CapabilityAudit}
	for i, capability := range want {
		if normalized[i].Capability != capability {
			t.Fatalf("normalized capability %d = %q, want %q", i, normalized[i].Capability, capability)
		}
	}
	if rank := capabilityRank("unknown"); rank != len(want) {
		t.Fatalf("unknown capability rank = %d", rank)
	}
}

func TestProviderCatalogRejectsInvalidSchemas(t *testing.T) {
	defaultValue := "10"
	minimum, maximum := int64(1), int64(5)
	valid := ProviderSpec{
		Provider: "postgres", Capabilities: []Capability{CapabilitySession},
		EndpointPolicy: FieldRequired, EndpointSchemes: []string{"postgres"},
		SecretRefPolicy: FieldRequired,
	}
	tests := []struct {
		name  string
		specs []ProviderSpec
	}{
		{name: "empty catalog"},
		{name: "provider name", specs: []ProviderSpec{{Provider: "Postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}}},
		{name: "empty capabilities", specs: []ProviderSpec{{Provider: "postgres", EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}}},
		{name: "unknown capability", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{"unknown"}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}}},
		{name: "duplicate capability", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession, CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}}},
		{name: "field policy", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, SecretRefPolicy: FieldForbidden}}},
		{name: "unknown option kind", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"mode": {Kind: "unknown"}}}}},
		{name: "bounds on string", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"mode": {Kind: OptionString, MinInteger: &minimum}}}}},
		{name: "forbidden schemes", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, EndpointSchemes: []string{"postgres"}, SecretRefPolicy: FieldForbidden}}},
		{name: "missing schemes", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldRequired, SecretRefPolicy: FieldForbidden}}},
		{name: "unnormalized scheme", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldRequired, EndpointSchemes: []string{"Postgres"}, SecretRefPolicy: FieldForbidden}}},
		{name: "duplicate scheme", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldRequired, EndpointSchemes: []string{"postgres", "postgres"}, SecretRefPolicy: FieldForbidden}}},
		{name: "sensitive option", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"password": {Kind: OptionString}}}}},
		{name: "sensitive option suffix", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"api_key_v2": {Kind: OptionString}}}}},
		{name: "sensitive connection suffix", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"connection_string_primary": {Kind: OptionString}}}}},
		{name: "compact password", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"dbpassword": {Kind: OptionString}}}}},
		{name: "passphrase", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"passphrase": {Kind: OptionString}}}}},
		{name: "prefixed passphrase", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"ssh_passphrase": {Kind: OptionString}}}}},
		{name: "password abbreviation", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"pwd": {Kind: OptionString}}}}},
		{name: "compact secret", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"clientsecret": {Kind: OptionString}}}}},
		{name: "compact token", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"bearertoken": {Kind: OptionString}}}}},
		{name: "compact DSN", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"jdbcdsn": {Kind: OptionString}}}}},
		{name: "required default", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"pool": {Kind: OptionInteger, Required: true, DefaultValue: &defaultValue}}}}},
		{name: "enum values", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"mode": {Kind: OptionEnum}}}}},
		{name: "invalid enum value", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"mode": {Kind: OptionEnum, AllowedValues: []string{"\n"}}}}}},
		{name: "oversized enum value", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"mode": {Kind: OptionEnum, Required: true, AllowedValues: []string{strings.Repeat("x", maxOptionValueLength+1)}}}}}},
		{name: "duplicate enum value", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"mode": {Kind: OptionEnum, AllowedValues: []string{"safe", "SAFE"}}}}}},
		{name: "values on string", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"mode": {Kind: OptionString, AllowedValues: []string{"safe"}}}}}},
		{name: "invalid default", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"pool": {Kind: OptionInteger, DefaultValue: stringPointer("many")}}}}},
		{name: "integer bounds", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"pool": {Kind: OptionInteger, MinInteger: &maximum, MaxInteger: &minimum}}}}},
		{name: "duplicate registration", specs: []ProviderSpec{valid, valid}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewProviderCatalog(test.specs...); !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewProviderCatalog() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestProviderCatalogRejectsInvalidBindingsWithoutLeakingValues(t *testing.T) {
	catalog := newTestCatalog(t)
	secretValue := "do-not-leak-password"
	valid := sessionBinding()[0]
	tests := []struct {
		name   string
		mutate func(*CapabilityBinding)
	}{
		{name: "unknown capability", mutate: func(binding *CapabilityBinding) { binding.Capability = "unknown" }},
		{name: "unknown provider", mutate: func(binding *CapabilityBinding) { binding.Provider = "mysql" }},
		{name: "invalid provider", mutate: func(binding *CapabilityBinding) { binding.Provider = "1bad" }},
		{name: "endpoint required", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "" }},
		{name: "endpoint scheme", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "https://db.example.com" }},
		{name: "endpoint user info", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://user:password@db.example.com" }},
		{name: "endpoint query", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com?token=" + secretValue }},
		{name: "endpoint fragment", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com#secret" }},
		{name: "endpoint empty fragment", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com#" }},
		{name: "endpoint control", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com/bad\npath" }},
		{name: "endpoint encoded null", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com/%00" }},
		{name: "endpoint encoded newline", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com/%0A" }},
		{name: "endpoint canonical length", mutate: func(binding *CapabilityBinding) {
			binding.Endpoint = "postgres://db.example.com/" + strings.Repeat("é", 1000)
		}},
		{name: "endpoint absolute", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "://not-a-uri" }},
		{name: "endpoint hostname", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://:5432" }},
		{name: "endpoint multi host", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://HOST1:5432,HOST2:5432" }},
		{name: "endpoint multi colon", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://host1:1234:host2:5432" }},
		{name: "endpoint invalid DNS", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://bad_host:5432" }},
		{name: "endpoint invalid port", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com:70000" }},
		{name: "secret required", mutate: func(binding *CapabilityBinding) { binding.SecretRef = "" }},
		{name: "secret grammar", mutate: func(binding *CapabilityBinding) { binding.SecretRef = "secret ref with spaces" }},
		{name: "unknown option", mutate: func(binding *CapabilityBinding) { binding.Options["unknown"] = secretValue }},
		{name: "sensitive option", mutate: func(binding *CapabilityBinding) { binding.Options["api_key"] = secretValue }},
		{name: "sensitive option suffix", mutate: func(binding *CapabilityBinding) { binding.Options["private_key_pem"] = secretValue }},
		{name: "sensitive compact option", mutate: func(binding *CapabilityBinding) { binding.Options["dbpassword"] = secretValue }},
		{name: "sensitive passphrase option", mutate: func(binding *CapabilityBinding) { binding.Options["ssh_passphrase"] = secretValue }},
		{name: "sensitive password abbreviation", mutate: func(binding *CapabilityBinding) { binding.Options["pwd"] = secretValue }},
		{name: "required option", mutate: func(binding *CapabilityBinding) { delete(binding.Options, "database") }},
		{name: "integer", mutate: func(binding *CapabilityBinding) { binding.Options["pool_size"] = "many" }},
		{name: "integer maximum", mutate: func(binding *CapabilityBinding) { binding.Options["pool_size"] = "101" }},
		{name: "integer minimum", mutate: func(binding *CapabilityBinding) { binding.Options["pool_size"] = "0" }},
		{name: "empty value", mutate: func(binding *CapabilityBinding) { binding.Options["database"] = " " }},
		{name: "boolean", mutate: func(binding *CapabilityBinding) { binding.Options["read_only"] = "sometimes" }},
		{name: "enum", mutate: func(binding *CapabilityBinding) { binding.Options["ssl_mode"] = "unsafe" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding := valid.Clone()
			test.mutate(&binding)
			_, err := catalog.NormalizeBindings([]CapabilityBinding{binding})
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("NormalizeBindings() error = %v, want ErrInvalid", err)
			}
			if strings.Contains(err.Error(), secretValue) || strings.Contains(err.Error(), "user:password") {
				t.Fatalf("error leaked configuration value: %v", err)
			}
		})
	}

	duplicate := []CapabilityBinding{valid.Clone(), valid.Clone()}
	if _, err := catalog.NormalizeBindings(duplicate); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate capability error = %v", err)
	}
	forbidden := CapabilityBinding{Capability: CapabilityMemory, Provider: "inmemory", Endpoint: "https://example.com"}
	if _, err := catalog.NormalizeBindings([]CapabilityBinding{forbidden}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forbidden endpoint error = %v", err)
	}
	forbidden = CapabilityBinding{Capability: CapabilityMemory, Provider: "inmemory", SecretRef: "secret://memory"}
	if _, err := catalog.NormalizeBindings([]CapabilityBinding{forbidden}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forbidden SecretRef error = %v", err)
	}
}

func TestEndpointNormalizationPreservesIPv6ZoneCase(t *testing.T) {
	catalog := newTestCatalog(t)
	bindings := []CapabilityBinding{{
		Capability: CapabilityKnowledge, Provider: "qdrant",
		Endpoint: "https://[FE80:0:0:0:0:0:0:1%25ETH0]:6333",
		Options:  map[string]string{"collection": "documents"},
	}, {
		Capability: CapabilityKnowledge, Provider: "qdrant",
		Endpoint: "https://[fe80::1%25ETH0]:6333",
		Options:  map[string]string{"collection": "documents"},
	}}
	var profiles []*Profile
	for i, binding := range bindings {
		profile, err := NewProfile(CreateInput{
			TenantID: testTenantID, ProfileKey: []string{"ipv6-expanded", "ipv6-compact"}[i], DisplayName: "IPv6",
			Status: StatusSuspended, Bindings: []CapabilityBinding{binding},
		}, catalog)
		if err != nil {
			t.Fatalf("NewProfile(%d) error = %v", i, err)
		}
		const canonical = "https://[fe80::1%25ETH0]:6333"
		if got := profile.Bindings[0].Endpoint; got != canonical {
			t.Fatalf("normalized IPv6 endpoint = %q, want %q", got, canonical)
		}
		if err := profile.Validate(catalog); err != nil {
			t.Fatalf("Profile.Validate(%d) after normalization error = %v", i, err)
		}
		profiles = append(profiles, profile)
	}
	if profiles[0].ContentDigest != profiles[1].ContentDigest {
		t.Fatalf("equivalent IPv6 endpoints have different digests: %s != %s", profiles[0].ContentDigest, profiles[1].ContentDigest)
	}
}

func TestEndpointCanonicalLengthRemainsValid(t *testing.T) {
	catalog := newTestCatalog(t)
	base := "https://qdrant.example.com/"
	profile, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "max-endpoint", DisplayName: "Maximum Endpoint",
		Status: StatusSuspended,
		Bindings: []CapabilityBinding{{
			Capability: CapabilityKnowledge, Provider: "qdrant",
			Endpoint: base + strings.Repeat("a", maxEndpointLength-len(base)),
			Options:  map[string]string{"collection": "documents"},
		}},
	}, catalog)
	if err != nil {
		t.Fatalf("NewProfile() error = %v", err)
	}
	if got := len(profile.Bindings[0].Endpoint); got != maxEndpointLength {
		t.Fatalf("normalized endpoint length = %d, want %d", got, maxEndpointLength)
	}
	if err := profile.Validate(catalog); err != nil {
		t.Fatalf("Profile.Validate() at endpoint limit error = %v", err)
	}
}

func TestIPv4MappedIPv6NormalizesIdempotently(t *testing.T) {
	catalog := newTestCatalog(t)
	endpoints := []string{"https://[::ffff:192.0.2.1]", "https://192.0.2.1"}
	var profiles []*Profile
	for i, endpoint := range endpoints {
		profile, err := NewProfile(CreateInput{
			TenantID: testTenantID, ProfileKey: []string{"mapped-ipv6", "plain-ipv4"}[i], DisplayName: "Mapped IPv6",
			Status: StatusSuspended,
			Bindings: []CapabilityBinding{{
				Capability: CapabilityKnowledge, Provider: "qdrant", Endpoint: endpoint,
				Options: map[string]string{"collection": "documents"},
			}},
		}, catalog)
		if err != nil {
			t.Fatalf("NewProfile(%q) error = %v", endpoint, err)
		}
		if got := profile.Bindings[0].Endpoint; got != "https://192.0.2.1" {
			t.Fatalf("normalized mapped endpoint = %q", got)
		}
		if err := profile.Validate(catalog); err != nil {
			t.Fatalf("Profile.Validate(%q) error = %v", endpoint, err)
		}
		profiles = append(profiles, profile)
	}
	if profiles[0].ContentDigest != profiles[1].ContentDigest {
		t.Fatalf("mapped and plain IPv4 endpoints have different digests: %s != %s", profiles[0].ContentDigest, profiles[1].ContentDigest)
	}
}

func TestEndpointPortNormalizationIsSemantic(t *testing.T) {
	catalog := newTestCatalog(t)
	endpoints := []string{"https://qdrant.example.com:06333", "https://qdrant.example.com:6333"}
	var profiles []*Profile
	for i, endpoint := range endpoints {
		profile, err := NewProfile(CreateInput{
			TenantID: testTenantID, ProfileKey: []string{"leading-zero-port", "canonical-port"}[i], DisplayName: "Port",
			Status: StatusSuspended,
			Bindings: []CapabilityBinding{{
				Capability: CapabilityKnowledge, Provider: "qdrant", Endpoint: endpoint,
				Options: map[string]string{"collection": "documents"},
			}},
		}, catalog)
		if err != nil {
			t.Fatalf("NewProfile(%q) error = %v", endpoint, err)
		}
		if got := profile.Bindings[0].Endpoint; got != endpoints[1] {
			t.Fatalf("normalized endpoint = %q, want %q", got, endpoints[1])
		}
		if err := profile.Validate(catalog); err != nil {
			t.Fatalf("Profile.Validate(%q) error = %v", endpoint, err)
		}
		profiles = append(profiles, profile)
	}
	if profiles[0].ContentDigest != profiles[1].ContentDigest {
		t.Fatalf("equivalent ports have different digests: %s != %s", profiles[0].ContentDigest, profiles[1].ContentDigest)
	}
}

func TestEndpointAuthorityValidation(t *testing.T) {
	catalog := newTestCatalog(t)
	valid := map[string]string{
		"DNS case":       "https://QDRANT.EXAMPLE.COM:6333",
		"DNS root":       "https://qdrant.example.com.",
		"encoded path":   "https://qdrant.example.com/%23documents",
		"IPv4":           "https://127.0.0.1:6333",
		"IPv6":           "https://[2001:db8::1]:6333",
		"IPv6 with zone": "https://[fe80::1%25ETH0]:6333",
	}
	for name, endpoint := range valid {
		t.Run(name, func(t *testing.T) {
			bindings, err := catalog.NormalizeBindings([]CapabilityBinding{{
				Capability: CapabilityKnowledge, Provider: "qdrant", Endpoint: endpoint,
				Options: map[string]string{"collection": "documents"},
			}})
			if err != nil {
				t.Fatalf("NormalizeBindings(%q) error = %v", endpoint, err)
			}
			if len(bindings) != 1 {
				t.Fatalf("NormalizeBindings(%q) returned %d bindings", endpoint, len(bindings))
			}
		})
	}

	invalid := []string{
		"https://-qdrant.example.com:6333",
		"https://qdrant-.example.com:6333",
		"https://qdrant..example.com:6333",
		"https://qdrant.example.com:0",
		"https://[127.0.0.1]:6333",
		"https://[fe80::1%25]:6333",
		"https://[fe80::1%25bad%21zone]:6333",
		"https://[::ffff:192.0.2.1%25ETH0]:6333",
	}
	for _, endpoint := range invalid {
		t.Run(endpoint, func(t *testing.T) {
			_, err := catalog.NormalizeBindings([]CapabilityBinding{{
				Capability: CapabilityKnowledge, Provider: "qdrant", Endpoint: endpoint,
				Options: map[string]string{"collection": "documents"},
			}})
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("NormalizeBindings(%q) error = %v, want ErrInvalid", endpoint, err)
			}
		})
	}
}

func TestProviderCatalogNilAndHelperBoundaries(t *testing.T) {
	assertProviderCatalogNilBoundaries(t)
	assertProviderCatalogGrammarBoundaries(t)
	assertProviderCatalogCloneBoundaries(t)
}

func assertProviderCatalogNilBoundaries(t *testing.T) {
	t.Helper()
	var catalog *ProviderCatalog
	if _, err := catalog.NormalizeBindings(sessionBinding()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil catalog error = %v", err)
	}
	if _, _, err := normalizeConfiguration(1, sessionBinding(), nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil configuration catalog error = %v", err)
	}

}

func assertProviderCatalogGrammarBoundaries(t *testing.T) {
	t.Helper()
	if validProviderName("") || validProviderName("1provider") || validProviderName("provider!") || validProviderName(strings.Repeat("p", 65)) {
		t.Fatal("invalid provider name passed validation")
	}
	if validOptionKey("") || validOptionKey("1option") || validOptionKey("option-") || validOptionKey(strings.Repeat("o", 65)) {
		t.Fatal("invalid option key passed validation")
	}
	if validScheme("") || validScheme("1http") || validScheme("http_") {
		t.Fatal("invalid endpoint scheme passed validation")
	}
	if !validProviderName("provider_1") || !validOptionKey("option_1") || !validScheme("https+v1") {
		t.Fatal("valid provider grammar was rejected")
	}
	if _, err := normalizeOptionValue("value", OptionSpec{Kind: "unknown"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown option kind error = %v", err)
	}
}

func assertProviderCatalogCloneBoundaries(t *testing.T) {
	t.Helper()
	if bindingsEqual(nil, sessionBinding()) || stringMapsEqual(nil, map[string]string{"a": ""}) {
		t.Fatal("different collection lengths compared equal")
	}
	if stringMapsEqual(map[string]string{"a": ""}, map[string]string{"b": ""}) {
		t.Fatal("different map keys with empty values compared equal")
	}
	if cloneBindings(nil) != nil || cloneStringMap(nil) != nil {
		t.Fatal("nil clone did not preserve nil")
	}

	if err := validateProfileID("bp_81ARZ3NDEKTSV4RRFFQ69G5FAV"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid ULID padding error = %v", err)
	}
	if err := validateProfileID("bp_01ARZ3NDEKTSV4RRFFQ69G5FAI"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid Crockford character error = %v", err)
	}
	if _, err := normalizeProfileKey("bad_key"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid interior profile key error = %v", err)
	}
}

func TestProviderCatalogDefensivelyCopiesSpecs(t *testing.T) {
	defaultPool := "10"
	allowed := []string{"disable", "require"}
	options := map[string]OptionSpec{
		"database":  {Kind: OptionString, Required: true},
		"pool_size": {Kind: OptionInteger, DefaultValue: &defaultPool},
		"ssl_mode":  {Kind: OptionEnum, DefaultValue: stringPointer("require"), AllowedValues: allowed},
	}
	spec := ProviderSpec{
		Provider: "postgres", Capabilities: []Capability{CapabilitySession},
		EndpointPolicy: FieldRequired, EndpointSchemes: []string{"postgres"},
		SecretRefPolicy: FieldRequired, Options: options,
	}
	catalog, err := NewProviderCatalog(spec)
	if err != nil {
		t.Fatal(err)
	}
	defaultPool = "999"
	allowed[1] = "mutated"
	delete(options, "database")
	spec.EndpointSchemes[0] = "https"

	binding := CapabilityBinding{
		Capability: CapabilitySession, Provider: "postgres", Endpoint: "postgres://db.example.com",
		Options: map[string]string{"database": "agent"}, SecretRef: "secret://db",
	}
	normalized, err := catalog.NormalizeBindings([]CapabilityBinding{binding})
	if err != nil {
		t.Fatalf("NormalizeBindings() after caller mutation error = %v", err)
	}
	if normalized[0].Options["pool_size"] != "10" || normalized[0].Options["ssl_mode"] != "require" {
		t.Fatalf("catalog retained caller-owned schema data: %#v", normalized[0].Options)
	}
}

func newTestCatalog(t *testing.T) *ProviderCatalog {
	t.Helper()
	minimumPool, maximumPool := int64(1), int64(100)
	catalog, err := NewProviderCatalog(
		ProviderSpec{
			Provider: "inmemory",
			Capabilities: []Capability{
				CapabilitySession, CapabilityMemory, CapabilitySummary, CapabilityKnowledge, CapabilityArtifact, CapabilityAudit,
			},
			EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden,
			Options: map[string]OptionSpec{"namespace": {Kind: OptionString}},
		},
		ProviderSpec{
			Provider: "postgres", Capabilities: []Capability{CapabilitySession, CapabilityMemory, CapabilityAudit},
			EndpointPolicy: FieldRequired, EndpointSchemes: []string{"postgres"}, SecretRefPolicy: FieldRequired,
			Options: map[string]OptionSpec{
				"database":  {Kind: OptionString, Required: true},
				"pool_size": {Kind: OptionInteger, DefaultValue: stringPointer("10"), MinInteger: &minimumPool, MaxInteger: &maximumPool},
				"read_only": {Kind: OptionBoolean, DefaultValue: stringPointer("false")},
				"ssl_mode":  {Kind: OptionEnum, DefaultValue: stringPointer("require"), AllowedValues: []string{"disable", "require", "verify-full"}},
			},
		},
		ProviderSpec{
			Provider: "qdrant", Capabilities: []Capability{CapabilityKnowledge},
			EndpointPolicy: FieldRequired, EndpointSchemes: []string{"https"}, SecretRefPolicy: FieldOptional,
			Options: map[string]OptionSpec{"collection": {Kind: OptionString, Required: true}},
		},
	)
	if err != nil {
		t.Fatalf("NewProviderCatalog() error = %v", err)
	}
	return catalog
}

func sessionBinding() []CapabilityBinding {
	return []CapabilityBinding{{
		Capability: CapabilitySession, Provider: "postgres", Endpoint: "postgres://db.example.com:5432",
		Options: map[string]string{"database": "agent"}, SecretRef: "secret://tenant/database",
	}}
}

func newTestProfile(t *testing.T, catalog *ProviderCatalog) *Profile {
	t.Helper()
	profile, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "primary", DisplayName: "Primary",
		Bindings: append(sessionBinding(), CapabilityBinding{
			Capability: CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": "memory"},
		}),
	}, catalog)
	if err != nil {
		t.Fatalf("NewProfile() error = %v", err)
	}
	return profile
}

func stringPointer(value string) *string { return &value }
