package deploymentv1

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"testing"
)

func redisWorkerMemory(t *testing.T) ManifestContent {
	c := workerMemoryFixture(t)
	r := c.Resources.Storage["memory"]
	r.Backend.Kind = datav1.Redis
	r.Backend.Adapter = "managed-redis-v1"
	r.Backend.PostgreSQL = nil
	r.Backend.Redis = &datav1.RedisTarget{Host: "memory.internal", Port: 6379, Database: 2, Username: "memory_runtime", TLS: true}
	r.Credential.CredentialID = "crd_00000000000000000000000000000003"
	r.Credential.AudienceDigest, _ = r.Backend.Digest()
	c.Resources.Storage["memory"] = r
	return c
}
func TestWorkerRedisMemoryFixedTarget(t *testing.T) {
	c := redisWorkerMemory(t)
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	if err := validateManagedResourceRoles(c); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*ManifestContent){
		"host": func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Redis.Host = "other.invalid" },
		"db":   func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Redis.Database = 3 },
		"tls":  func(c *ManifestContent) { c.Resources.Storage["memory"].Backend.Redis.TLS = false },
		"user": func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Backend.Redis.Username = "default"
			r.Credential.AudienceDigest, _ = r.Backend.Digest()
			c.Resources.Storage["memory"] = r
		},
		"credential": func(c *ManifestContent) {
			r := c.Resources.Storage["memory"]
			r.Credential = CredentialUse{}
			c.Resources.Storage["memory"] = r
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := redisWorkerMemory(t)
			change(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("invalid backend accepted")
			}
		})
	}
}
func TestHistoricalRedisMemoryWithoutCredentialRemainsReadableNotExecutable(t *testing.T) {
	c := redisWorkerMemory(t)
	r := c.Resources.Storage["memory"]
	r.Credential = CredentialUse{}
	c.Resources.Storage["memory"] = r
	if _, err := r.MarshalJSON(); err != nil {
		t.Fatal(err)
	}
	if err := validateManagedResourceRoles(c); err != nil {
		t.Fatal(err)
	}
	if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
		t.Fatal("historical unbound backend executable")
	}
}
