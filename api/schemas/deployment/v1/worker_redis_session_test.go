package deploymentv1

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"testing"
)

func redisWorkerSession(t *testing.T) ManifestContent {
	c := workerSummaryFixture(t)
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: c.TenantID, BackendID: "redis-session", BackendRevision: 1, Kind: datav1.Redis, Adapter: "managed-redis-v1", Isolation: datav1.SessionIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 4, MaxBytes: 1 << 20}, Redis: &datav1.RedisTarget{Host: c.Resources.Storage[c.StorageRoles["session"]].Destination.Host, Port: 6379, Database: 2, Username: "session_runtime", TLS: true}}
	d, _ := b.Digest()
	c.Resources.Storage[c.StorageRoles["session"]] = ManifestStorageResource{Kind: "managed_session", AdapterVersion: "managed-session-v1", Backend: &b, Credential: CredentialUse{CredentialID: "crd_00000000000000000000000000000002", Purpose: "dsn_password", AudienceDigest: d}}
	return c
}
func TestWorkerRedisSessionSummaryFixedClosure(t *testing.T) {
	c := redisWorkerSession(t)
	if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	if err := validateManagedResourceRoles(c); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ManifestContent){
		"host": func(c *ManifestContent) {
			c.Resources.Storage[c.StorageRoles["session"]].Backend.Redis.Host = "changed.invalid"
		},
		"db":  func(c *ManifestContent) { c.Resources.Storage[c.StorageRoles["session"]].Backend.Redis.Database = 3 },
		"tls": func(c *ManifestContent) { c.Resources.Storage[c.StorageRoles["session"]].Backend.Redis.TLS = false },
		"user": func(c *ManifestContent) {
			r := c.Resources.Storage[c.StorageRoles["session"]]
			r.Backend.Redis.Username = "memory_runtime"
			r.Credential.AudienceDigest, _ = r.Backend.Digest()
			c.Resources.Storage[c.StorageRoles["session"]] = r
		},
		"scope": func(c *ManifestContent) {
			c.Resources.Storage[c.StorageRoles["session"]].Backend.Isolation = datav1.MemoryIsolation
		},
		"missing": func(c *ManifestContent) {
			r := c.Resources.Storage[c.StorageRoles["session"]]
			r.Credential = CredentialUse{}
			c.Resources.Storage[c.StorageRoles["session"]] = r
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := redisWorkerSession(t)
			mutate(&c)
			if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
				t.Fatal("invalid session accepted")
			}
		})
	}
}
func TestHistoricalRedisSessionWithoutCredentialReadableOnly(t *testing.T) {
	c := redisWorkerSession(t)
	key := c.StorageRoles["session"]
	r := c.Resources.Storage[key]
	r.Credential = CredentialUse{}
	c.Resources.Storage[key] = r
	if _, err := r.MarshalJSON(); err != nil {
		t.Fatal(err)
	}
	if err := validateManagedResourceRoles(c); err != nil {
		t.Fatal(err)
	}
	if ValidateWorkerV1(c, c.PlatformContract.Digest) == nil {
		t.Fatal("historical unbound session executable")
	}
}
