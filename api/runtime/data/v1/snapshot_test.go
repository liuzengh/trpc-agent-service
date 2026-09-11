package datav1

import (
	"encoding/json"
	"strings"
	"testing"
)

func fixtures() []Snapshot {
	base := Snapshot{SchemaVersion: "v1", TenantID: "tenant-a", BackendID: "backend", BackendRevision: 1, Limits: Limits{TimeoutMS: 10000, MaxConcurrency: 8, MaxBytes: 1024}}
	pg := base
	pg.Kind = PostgreSQL
	pg.Adapter = "managed-postgres-v1"
	pg.Isolation = "tenant-session-v1"
	pg.PostgreSQL = &PostgresTarget{Host: "postgres.internal", Port: 5432, Database: "runtime", Username: "runtime_user", SSLMode: "verify-full"}
	redis := base
	redis.Kind = Redis
	redis.Adapter = "managed-redis-v1"
	redis.Isolation = "tenant-session-v1"
	redis.Redis = &RedisTarget{Host: "redis.internal", Port: 6379, Username: "runtime_user", TLS: true}
	q := base
	q.Kind = Qdrant
	q.Adapter = "managed-qdrant-v1"
	q.Isolation = "tenant-profile-resource-v1"
	q.Qdrant = &QdrantTarget{Endpoint: "https://qdrant.internal:6333", Collection: "knowledge", VectorName: "content", Dimensions: 1536, Distance: "cosine"}
	s3 := base
	s3.Kind = S3
	s3.Adapter = "managed-s3-v1"
	s3.Isolation = "tenant-artifact-v1"
	s3.S3 = &S3Target{Endpoint: "https://s3.internal", Bucket: "runtime-artifacts", Region: "us-east-1", PathStyle: true, Versioning: "disabled"}
	return []Snapshot{pg, redis, q, s3}
}
func TestFourBackendSnapshotRoundTrip(t *testing.T) {
	for _, s := range fixtures() {
		t.Run(string(s.Kind), func(t *testing.T) {
			b, err := s.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			got, err := Decode(b)
			if err != nil || !s.Matches(got) {
				t.Fatal(got, err)
			}
			if host, err := s.EndpointHost(); err != nil || host == "" {
				t.Fatal(host, err)
			}
			for _, field := range []string{`"password"`, `"secret"`, `"token"`, `"dsn"`} {
				if strings.Contains(string(b), field) {
					t.Fatal(field)
				}
			}
			changed := s.Clone()
			changed.TenantID = "tenant-b"
			if s.Matches(changed) {
				t.Fatal("tenant omitted from digest")
			}
			changed = s.Clone()
			changed.BackendRevision++
			if s.Matches(changed) {
				t.Fatal("revision omitted from digest")
			}
			changed = s.Clone()
			changed.Limits.MaxBytes++
			if s.Matches(changed) {
				t.Fatal("limits omitted from digest")
			}
			switch s.Kind {
			case PostgreSQL:
				changed.PostgreSQL.Database = "another"
			case Redis:
				changed.Redis.Database = 1
			case Qdrant:
				changed.Qdrant.Collection = "another"
			case S3:
				changed.S3.Bucket = "another-bucket"
			}
			if s.Matches(changed) {
				t.Fatal("physical target omitted from digest")
			}
			again, _ := s.Canonical()
			if string(b) != string(again) {
				t.Fatal("clone aliases target")
			}
		})
	}
}
func TestClosedSnapshotDecode(t *testing.T) {
	b, _ := fixtures()[1].Canonical()
	good := string(b)
	for _, bad := range []string{
		strings.Replace(good, `"tenant_id":"tenant-a"`, `"Tenant_ID":"tenant-a"`, 1),
		strings.Replace(good, `"tenant_id":"tenant-a"`, `"tenant_id":"tenant-a","tenant_id":"tenant-b"`, 1),
		strings.Replace(good, `"tls":true`, `"tls":null`, 1),
		strings.Replace(good, `"tls":true,`, "", 1),
		strings.Replace(good, `"tls":true`, `"tls":true,"password":"private"`, 1),
		strings.Replace(good, `"redis":{`, `"redis":null,"unknown":{`, 1),
		good + `{}`, `null`, strings.Repeat(" ", MaxSnapshotBytes+1),
	} {
		if _, err := Decode([]byte(bad)); err != ErrSnapshot {
			t.Fatalf("accepted invalid shape: %s (%v)", bad, err)
		}
	}
}
func TestRejectsInvalidTargets(t *testing.T) {
	cases := []func(*Snapshot){func(s *Snapshot) { s.Kind = "other" }, func(s *Snapshot) { s.Adapter = "any-adapter" }, func(s *Snapshot) { s.Isolation = "none" }, func(s *Snapshot) { s.BackendRevision = 0 }, func(s *Snapshot) { s.Limits.MaxConcurrency = 257 }, func(s *Snapshot) { s.Limits.TimeoutMS = 0 }, func(s *Snapshot) { s.Limits.MaxBytes = 0 }, func(s *Snapshot) { s.Redis = &RedisTarget{} }, func(s *Snapshot) { s.PostgreSQL.Host = "db?password=value" }}
	for i, mutate := range cases {
		s := fixtures()[0]
		mutate(&s)
		if s.Validate() != ErrSnapshot {
			t.Fatal(i)
		}
	}
	for _, raw := range []string{"https://user:pass@qdrant.internal", "https://qdrant.internal/path", "https://qdrant.internal?token=x", "https://qdrant.internal#", "https://qdrant.internal:", "file:///tmp/db", "https://qdrant.internal:99999"} {
		s := fixtures()[2]
		s.Qdrant.Endpoint = raw
		if s.Validate() != ErrSnapshot {
			t.Fatal(raw)
		}
	}
	for _, b := range []string{"../bucket", "192.168.1.1", "bucket--x-s3", "a..bucket", "BUCKET", "bucket.mrap"} {
		s := fixtures()[3]
		s.S3.Bucket = b
		if s.Validate() != ErrSnapshot {
			t.Fatal(b)
		}
	}
	for _, mode := range []string{"enabled", "suspended", ""} {
		s := fixtures()[3]
		s.S3.Versioning = mode
		if s.Validate() != ErrSnapshot {
			t.Fatal(mode)
		}
	}
}
func TestMissingTargetAndInvalidValueAreNotEqual(t *testing.T) {
	var a, b Snapshot
	if a.Matches(b) {
		t.Fatal("invalid snapshots compare equal")
	}
	if _, err := a.Digest(); err != ErrSnapshot {
		t.Fatal(err)
	}
	if _, err := a.EndpointHost(); err != ErrSnapshot {
		t.Fatal(err)
	}
}
func TestDecodePreservesIPv6Endpoint(t *testing.T) {
	s := fixtures()[2]
	s.Qdrant.Endpoint = "https://[::1]:6333"
	b, _ := json.Marshal(s)
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if h, _ := got.EndpointHost(); h != "::1" {
		t.Fatal(h)
	}
}
