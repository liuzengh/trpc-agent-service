// Package datav1 defines the closed, non-secret managed backend snapshot wire
// contract. It has no repository, authorization service or network client.
package datav1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

const Version = "v1"

type Kind string

const (
	PostgreSQL Kind = "postgresql"
	Redis      Kind = "redis"
	Qdrant     Kind = "qdrant"
	S3         Kind = "s3"
)

var ErrSnapshot = errors.New("INVALID_MANAGED_BACKEND_SNAPSHOT")
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)
var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)
var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// Snapshot binds an exact backend revision and physical target to one tenant.
// TenantID is an assertion to verify against authenticated execution context;
// possession of a snapshot is not authorization to read that tenant's data.
type Snapshot struct {
	SchemaVersion   string          `json:"schema_version"`
	TenantID        string          `json:"tenant_id"`
	BackendID       string          `json:"backend_id"`
	BackendRevision uint64          `json:"backend_revision"`
	Kind            Kind            `json:"kind"`
	Adapter         string          `json:"adapter"`
	Isolation       string          `json:"isolation"`
	Limits          Limits          `json:"limits"`
	PostgreSQL      *PostgresTarget `json:"postgresql,omitempty"`
	Redis           *RedisTarget    `json:"redis,omitempty"`
	Qdrant          *QdrantTarget   `json:"qdrant,omitempty"`
	S3              *S3Target       `json:"s3,omitempty"`
}
type Limits struct {
	TimeoutMS      int64 `json:"timeout_ms"`
	MaxConcurrency int64 `json:"max_concurrency"`
	MaxBytes       int64 `json:"max_bytes"`
}
type PostgresTarget struct {
	Host     string `json:"host"`
	Port     int64  `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	SSLMode  string `json:"sslmode"`
}
type RedisTarget struct {
	Host     string `json:"host"`
	Port     int64  `json:"port"`
	Database int64  `json:"database"`
	Username string `json:"username"`
	TLS      bool   `json:"tls"`
}
type QdrantTarget struct {
	Endpoint   string `json:"endpoint"`
	Collection string `json:"collection"`
	VectorName string `json:"vector_name"`
	Dimensions int64  `json:"dimensions"`
	Distance   string `json:"distance"`
}
type S3Target struct {
	Endpoint   string `json:"endpoint"`
	Bucket     string `json:"bucket"`
	Region     string `json:"region"`
	PathStyle  bool   `json:"path_style"`
	Versioning string `json:"versioning"`
}

func (s Snapshot) Validate() error {
	if s.SchemaVersion != Version || !idPattern.MatchString(s.TenantID) || !idPattern.MatchString(s.BackendID) || s.BackendRevision == 0 || s.BackendRevision > 9007199254740991 {
		return ErrSnapshot
	}
	if s.Limits.TimeoutMS < 1 || s.Limits.TimeoutMS > 60000 || s.Limits.MaxConcurrency < 1 || s.Limits.MaxConcurrency > 256 || s.Limits.MaxBytes < 1 || s.Limits.MaxBytes > 1<<40 {
		return ErrSnapshot
	}
	n := 0
	for _, yes := range []bool{s.PostgreSQL != nil, s.Redis != nil, s.Qdrant != nil, s.S3 != nil} {
		if yes {
			n++
		}
	}
	if n != 1 {
		return ErrSnapshot
	}
	switch s.Kind {
	case PostgreSQL:
		p := s.PostgreSQL
		if p == nil || s.Adapter != "managed-postgres-v1" || (s.Isolation != SessionIsolation && s.Isolation != MemoryIsolation) || !host(p.Host) || !port(p.Port) || !namePattern.MatchString(p.Database) || !namePattern.MatchString(p.Username) {
			return ErrSnapshot
		}
		if p.SSLMode != "disable" && p.SSLMode != "require" && p.SSLMode != "verify-full" {
			return ErrSnapshot
		}
	case Redis:
		p := s.Redis
		if p == nil || s.Adapter != "managed-redis-v1" || (s.Isolation != SessionIsolation && s.Isolation != MemoryIsolation) || !host(p.Host) || !port(p.Port) || p.Database < 0 || p.Database > 15 || !namePattern.MatchString(p.Username) {
			return ErrSnapshot
		}
	case Qdrant:
		p := s.Qdrant
		if p == nil || s.Adapter != "managed-qdrant-v1" || s.Isolation != "tenant-profile-resource-v1" || !endpoint(p.Endpoint) || !namePattern.MatchString(p.Collection) || !namePattern.MatchString(p.VectorName) || p.Dimensions < 1 || p.Dimensions > 65536 {
			return ErrSnapshot
		}
		if p.Distance != "cosine" && p.Distance != "dot" && p.Distance != "euclid" {
			return ErrSnapshot
		}
	case S3:
		p := s.S3
		if p == nil || s.Adapter != "managed-s3-v1" || s.Isolation != "tenant-artifact-v1" || !endpoint(p.Endpoint) || !bucket(p.Bucket) || !namePattern.MatchString(p.Region) || p.Versioning != "disabled" {
			return ErrSnapshot
		}
	default:
		return ErrSnapshot
	}
	return nil
}
func port(p int64) bool { return p > 0 && p <= 65535 }
func host(s string) bool {
	if s == "" || len(s) > 253 || strings.ContainsAny(s, "\x00\r\n\t /\\%@?#[]") {
		return false
	}
	if net.ParseIP(s) != nil {
		return true
	}
	for _, label := range strings.Split(s, ".") {
		if !dnsLabel.MatchString(label) {
			return false
		}
	}
	return true
}
func endpoint(raw string) bool {
	if !utf8.ValidString(raw) || len(raw) > 2048 || strings.TrimSpace(raw) != raw {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") || u.Path != "" || u.RawPath != "" || !host(u.Hostname()) {
		return false
	}
	if u.Port() != "" {
		p, err := net.LookupPort("tcp", u.Port())
		if err != nil || !port(int64(p)) {
			return false
		}
	}
	return !strings.HasSuffix(u.Host, ":")
}
func bucket(s string) bool {
	if !bucketPattern.MatchString(s) || net.ParseIP(s) != nil || strings.Contains(s, "..") || strings.Contains(s, ".-") || strings.Contains(s, "-.") {
		return false
	}
	for _, prefix := range []string{"xn--", "sthree-", "amzn-s3-demo-"} {
		if strings.HasPrefix(s, prefix) {
			return false
		}
	}
	for _, suffix := range []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"} {
		if strings.HasSuffix(s, suffix) {
			return false
		}
	}
	return true
}

// Clone detaches every pointer branch; no map or arbitrary option object exists.
func (s Snapshot) Clone() Snapshot {
	if s.PostgreSQL != nil {
		p := *s.PostgreSQL
		s.PostgreSQL = &p
	}
	if s.Redis != nil {
		p := *s.Redis
		s.Redis = &p
	}
	if s.Qdrant != nil {
		p := *s.Qdrant
		s.Qdrant = &p
	}
	if s.S3 != nil {
		p := *s.S3
		s.S3 = &p
	}
	return s
}
func (s Snapshot) Canonical() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, ErrSnapshot
	}
	b, err = jcs.Transform(b)
	if err != nil {
		return nil, ErrSnapshot
	}
	return b, nil
}

// Digest includes tenant, adapter, target, revision and limits, not live secrets.
func (s Snapshot) Digest() (string, error) {
	b, err := s.Canonical()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
func (s Snapshot) Matches(other Snapshot) bool {
	a, e := s.Digest()
	if e != nil {
		return false
	}
	b, e := other.Digest()
	return e == nil && a == b
}

// EndpointHost is used by the compiler's network allowlist checks, never to
// dynamically discover or choose a different target.
func (s Snapshot) EndpointHost() (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	switch s.Kind {
	case PostgreSQL:
		return s.PostgreSQL.Host, nil
	case Redis:
		return s.Redis.Host, nil
	case Qdrant:
		u, _ := url.Parse(s.Qdrant.Endpoint)
		return u.Hostname(), nil
	default:
		u, _ := url.Parse(s.S3.Endpoint)
		return u.Hostname(), nil
	}
}
