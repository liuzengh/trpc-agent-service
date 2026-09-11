package domain

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

func redisSessionCredentialInput() CompileInput {
	in := workerSummaryInput()
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: in.TenantID, BackendID: "session-redis", BackendRevision: 1, Kind: datav1.Redis, Adapter: "managed-redis-v1", Isolation: datav1.SessionIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, Redis: &datav1.RedisTarget{Host: "redis.internal", Port: 6379, Database: 2, Username: "session_runtime", TLS: true}}
	digest, _ := b.Digest()
	in.ManagedBackends = map[string]datav1.Snapshot{"storage/session": b}
	in.Profile.Spec.Storage["session"] = profiledomain.StorageResource{Kind: profiledomain.StorageKindManagedSession, BackendID: b.BackendID, BackendRevision: 1, DSNCredentialID: credentialMemory, CredentialAudienceDigest: digest}
	in.Platform.Execution.AllowedEndpointHosts = append(in.Platform.Execution.AllowedEndpointHosts, "redis.internal")
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	return in
}
func TestRedisSessionSummaryCredentialClosure(t *testing.T) {
	in := redisSessionCredentialInput()
	m, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	s := m.Content.Resources.Storage["session"]
	digest, _ := s.Backend.Digest()
	if s.Credential != (CredentialUse{CredentialID: credentialMemory, Purpose: CredentialPurposeDSNPassword, AudienceDigest: digest}) || len(m.CredentialUses) != 3 {
		t.Fatal("session credential closure")
	}
	if m.Content.Runtime.Summary.ModelResource != "summarizer" || m.Content.StorageRoles["session"] != "session" {
		t.Fatal("summary changed session")
	}
	if _, err := ValidateManifestContent(m.CanonicalContent, m.ContentDigest); err != nil {
		t.Fatal(err)
	}
	wire, err := deploymentv1.DecodeManifestContent(m.CanonicalContent)
	if err != nil {
		t.Fatal(err)
	}
	if err = deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest); err != nil {
		t.Fatal(err)
	}
	if !NewPublicManifestView(m.Content).Resources.Storage["session"].CredentialPresent {
		t.Fatal("session status absent")
	}
	for _, mutate := range []func(*datav1.RedisTarget){func(r *datav1.RedisTarget) { r.Host = "other.internal" }, func(r *datav1.RedisTarget) { r.Port++ }, func(r *datav1.RedisTarget) { r.Database++ }, func(r *datav1.RedisTarget) { r.Username = "memory_runtime" }, func(r *datav1.RedisTarget) { r.TLS = false }} {
		c := normalizeManifestContent(m.Content)
		mutate(c.Resources.Storage["session"].Backend.Redis)
		_, raw, digest, err := CanonicalizeManifest(c)
		if err != nil {
			continue
		}
		if _, err = ValidateManifestContent(raw, digest); err == nil {
			t.Fatal("rehashed target changed")
		}
	}
	// Historical descriptor stays readable and retains the old public shape, not executable.
	s.Credential = CredentialUse{}
	m.Content.Resources.Storage["session"] = s
	_, raw, digest, err := CanonicalizeManifest(m.Content)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ValidateManifestContent(raw, digest)
	if err != nil {
		t.Fatal("historical read", err)
	}
	v, _ := json.Marshal(NewPublicManifestView(c))
	var tree map[string]any
	_ = json.Unmarshal(v, &tree)
	session := tree["resources"].(map[string]any)["storage"].(map[string]any)["session"].(map[string]any)
	if _, ok := session["credential_present"]; ok {
		t.Fatal("historical session view changed")
	}
	wire, err = deploymentv1.DecodeManifestContent(raw)
	if err != nil {
		t.Fatal(err)
	}
	if deploymentv1.ValidateWorkerV1(wire, "") == nil {
		t.Fatal("historical session executable")
	}
}
func TestRedisSessionNewPublicationRequiresBoundPasswordAndPrincipal(t *testing.T) {
	for _, mode := range []string{"missing", "stale", "wrong_user", "postgres"} {
		t.Run(mode, func(t *testing.T) {
			in := redisSessionCredentialInput()
			r := in.Profile.Spec.Storage["session"]
			b := in.ManagedBackends["storage/session"]
			switch mode {
			case "missing":
				r.DSNCredentialID = ""
				r.CredentialAudienceDigest = ""
			case "stale":
				r.CredentialAudienceDigest = "sha256:" + strings.Repeat("f", 64)
			case "wrong_user":
				b.Redis.Username = "memory_runtime"
				r.CredentialAudienceDigest, _ = b.Digest()
			case "postgres":
				b.Redis = nil
				b.Kind = datav1.PostgreSQL
				b.Adapter = "managed-postgres-v1"
				b.PostgreSQL = &datav1.PostgresTarget{Host: "pg.internal", Port: 5432, Database: "session", Username: "session_runtime", SSLMode: "verify-full"}
				r.CredentialAudienceDigest, _ = b.Digest()
			}
			in.Profile.Spec.Storage["session"] = r
			in.ManagedBackends["storage/session"] = b
			if m, report := Compile(in); report.Valid || len(m.CanonicalContent) > 0 {
				t.Fatal("invalid session published", mode)
			}
		})
	}
}
