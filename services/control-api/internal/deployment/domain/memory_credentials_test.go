package domain

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

func pgMemoryCredentialInput() CompileInput {
	in := managedExecutableInput()
	in.Platform.RuntimeDataCapabilities = []string{"memory"}
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	n := in.Agent.Spec.Nodes["researcher"]
	n.Memory = &agentdomain.Memory{Tools: []string{"memory_add", "memory_load"}}
	in.Agent.Spec.Nodes["researcher"] = n
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: in.TenantID, BackendID: "memory-pg", BackendRevision: 1, Kind: datav1.PostgreSQL, Adapter: "managed-postgres-v1", Isolation: datav1.MemoryIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, PostgreSQL: &datav1.PostgresTarget{Host: "pg.internal", Port: 5432, Database: "memory", Username: "memory_runtime", SSLMode: "verify-full"}}
	digest, _ := b.Digest()
	in.ManagedBackends["storage/memory"] = b
	in.Profile.Spec.Storage["memory"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedMemory, BackendID: b.BackendID, BackendRevision: b.BackendRevision, DSNCredentialID: credentialMemory, CredentialAudienceDigest: digest}
	return in
}
func TestPGMemoryCompileRequiresBoundPasswordUse(t *testing.T) {
	in := pgMemoryCredentialInput()
	m, r := Compile(in)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	storage := m.Content.Resources.Storage["memory"]
	digest, _ := storage.Backend.Digest()
	want := CredentialUse{CredentialID: credentialMemory, Purpose: CredentialPurposeDSNPassword, AudienceDigest: digest}
	if storage.Credential != want {
		t.Fatal("wrong compiled credential")
	}
	found := false
	for _, use := range m.CredentialUses {
		if use == want {
			found = true
		}
	}
	if !found {
		t.Fatal("required use absent")
	}
	if _, err := deploymentv1.DecodeManifestContent(m.CanonicalContent); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateManifestContent(m.CanonicalContent, m.ContentDigest); err != nil {
		t.Fatal(err)
	}
	view := NewPublicManifestView(m.Content)
	if !view.Resources.Storage["memory"].CredentialPresent {
		t.Fatal("public credential status absent")
	}
	for _, change := range []func(*ManifestContent){
		func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Credential = CredentialUse{}
			c.Resources.Storage["memory"] = r
		},
		func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Credential.Purpose = "dsn"
			c.Resources.Storage["memory"] = r
		},
		func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Credential.AudienceDigest = "sha256:" + strings.Repeat("a", 64)
			c.Resources.Storage["memory"] = r
		},
		func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.PostgreSQL.Database = "other" },
	} {
		c := normalizeManifestContent(m.Content)
		change(&c)
		_, raw, digest, err := CanonicalizeManifest(c)
		if err != nil {
			continue
		}
		if _, err = ValidateManifestContent(raw, digest); err == nil {
			t.Fatal("rehashed credential tampering accepted")
		}
		if _, err = deploymentv1.DecodeManifestContent(raw); err == nil {
			t.Fatal("shared codec accepted tampering")
		}
	}
}
func TestPGMemoryMissingOrStaleProfileBindingFails(t *testing.T) {
	for _, mode := range []string{"missing", "stale"} {
		in := pgMemoryCredentialInput()
		r := in.Profile.Spec.Storage["memory"]
		if mode == "missing" {
			r.DSNCredentialID = ""
		} else {
			r.CredentialAudienceDigest = "sha256:" + strings.Repeat("f", 64)
		}
		in.Profile.Spec.Storage["memory"] = r
		m, report := Compile(in)
		if report.Valid || len(m.CanonicalContent) > 0 {
			t.Fatal("bad binding compiled")
		}
	}
}

func TestRedisMemoryCredentialNullIsNotAbsence(t *testing.T) {
	compiled, report := Compile(fullDataInput())
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	var content map[string]any
	_ = json.Unmarshal(compiled.CanonicalContent, &content)
	content["resources"].(map[string]any)["storage"].(map[string]any)["memory"].(map[string]any)["credential"] = nil
	raw, _ := json.Marshal(content)
	if _, err := DecodeManifestContent(raw); err == nil {
		t.Fatal("Redis credential null was silently omitted")
	}
	if _, err := deploymentv1.DecodeManifestContent(raw); err == nil {
		t.Fatal("shared codec accepted Redis credential")
	}
}

func redisMemoryCredentialInput() CompileInput {
	in := pgMemoryCredentialInput()
	b := in.ManagedBackends["storage/memory"]
	b.Kind = datav1.Redis
	b.Adapter = "managed-redis-v1"
	b.PostgreSQL = nil
	b.Redis = &datav1.RedisTarget{Host: "redis.internal", Port: 6379, Database: 3, Username: "memory_runtime", TLS: true}
	in.ManagedBackends["storage/memory"] = b
	r := in.Profile.Spec.Storage["memory"]
	r.CredentialAudienceDigest, _ = b.Digest()
	in.Profile.Spec.Storage["memory"] = r
	return in
}
func TestRedisMemoryCompileRequiresBoundPasswordUse(t *testing.T) {
	in := redisMemoryCredentialInput()
	m, r := Compile(in)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	storage := m.Content.Resources.Storage["memory"]
	digest, _ := storage.Backend.Digest()
	want := CredentialUse{CredentialID: credentialMemory, Purpose: CredentialPurposeDSNPassword, AudienceDigest: digest}
	if storage.Credential != want {
		t.Fatal("wrong compiled credential")
	}
	found := false
	for _, use := range m.CredentialUses {
		if use == want {
			found = true
		}
	}
	if !found {
		t.Fatal("required use absent")
	}
	if _, err := deploymentv1.DecodeManifestContent(m.CanonicalContent); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateManifestContent(m.CanonicalContent, m.ContentDigest); err != nil {
		t.Fatal(err)
	}
	view := NewPublicManifestView(m.Content)
	if !view.Resources.Storage["memory"].CredentialPresent {
		t.Fatal("public credential status absent")
	}
	for _, change := range []func(*ManifestContent){
		func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Credential = CredentialUse{}
			c.Resources.Storage["memory"] = r
		},
		func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Credential.Purpose = "dsn"
			c.Resources.Storage["memory"] = r
		},
		func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Credential.AudienceDigest = "sha256:" + strings.Repeat("a", 64)
			c.Resources.Storage["memory"] = r
		},
		func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Redis.Database = 4 },
		func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Redis.Host = "other.internal" },
		func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Redis.Port = 6380 },
		func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Redis.Username = "other" },
		func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Redis.TLS = false },
	} {
		c := normalizeManifestContent(m.Content)
		change(&c)
		_, raw, digest, err := CanonicalizeManifest(c)
		if err != nil {
			continue
		}
		_, readErr := ValidateManifestContent(raw, digest)
		if c.Resources.Storage["memory"].Credential == (CredentialUse{}) {
			if readErr != nil {
				t.Fatal("historical descriptor unreadable", readErr)
			}
		} else if readErr == nil {
			t.Fatal("rehashed credential tampering accepted")
		}

		decoded, decodeErr := deploymentv1.DecodeManifestContent(raw)
		if c.Resources.Storage["memory"].Credential == (CredentialUse{}) {
			// The shared reader retains historical descriptors, but they are not executable.
			if decodeErr == nil && deploymentv1.ValidateWorkerV1(decoded, "") == nil {
				t.Fatal("missing credential executable")
			}
		} else if decodeErr == nil {
			t.Fatal("shared codec accepted tampering")
		}

	}
}

func TestMemoryRuntimePrincipalBothBackends(t *testing.T) {
	for _, factory := range []func() CompileInput{pgMemoryCredentialInput, redisMemoryCredentialInput} {
		for _, user := range []string{"memory_runtime", "session_runtime", "runtime"} {
			in := factory()
			b := in.ManagedBackends["storage/memory"]
			if b.PostgreSQL != nil {
				b.PostgreSQL.Username = user
			} else {
				b.Redis.Username = user
			}
			if memoryRuntimePrincipal(b) != (user == "memory_runtime") {
				t.Fatal("incorrect principal", b.Kind, user)
			}
		}
	}
}

func TestRedisMemoryMissingOrStaleProfileBindingFails(t *testing.T) {
	for _, mode := range []string{"missing", "stale"} {
		in := redisMemoryCredentialInput()
		r := in.Profile.Spec.Storage["memory"]
		if mode == "missing" {
			r.DSNCredentialID = ""
		} else {
			r.CredentialAudienceDigest = "sha256:" + strings.Repeat("f", 64)
		}
		in.Profile.Spec.Storage["memory"] = r
		m, report := Compile(in)
		if report.Valid || len(m.CanonicalContent) > 0 {
			t.Fatal("bad Redis binding compiled")
		}
	}
}

func TestHistoricalRedisMemoryReadDoesNotEnableNewCompile(t *testing.T) {
	in := redisMemoryCredentialInput()
	m, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	r := m.Content.Resources.Storage["memory"]
	r.Credential = CredentialUse{}
	m.Content.Resources.Storage["memory"] = r
	_, raw, digest, err := CanonicalizeManifest(m.Content)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ValidateManifestContent(raw, digest)
	if err != nil {
		t.Fatal("historical read", err)
	}
	view, err := json.Marshal(NewPublicManifestView(c))
	if err != nil {
		t.Fatal(err)
	}
	var tree map[string]any
	_ = json.Unmarshal(view, &tree)
	memory := tree["resources"].(map[string]any)["storage"].(map[string]any)["memory"].(map[string]any)
	if _, exists := memory["credential_present"]; exists {
		t.Fatal("historical public projection changed")
	}
	p := in.Profile.Spec.Storage["memory"]
	p.DSNCredentialID = ""
	p.CredentialAudienceDigest = ""
	in.Profile.Spec.Storage["memory"] = p
	if _, r := Compile(in); r.Valid {
		t.Fatal("new compile omitted password")
	}
}
