// Package bootstrap is Worker V1's process composition root.
package bootstrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(raw []byte) error {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return errors.New("duration must be a string")
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return errors.New("invalid duration")
	}
	*d = Duration(duration)
	return nil
}
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d Duration) Value() time.Duration { return time.Duration(d) }

type Config struct {
	Tracing                *telemetrytrace.Config `json:"tracing,omitempty"`
	WorkerID               string                 `json:"worker_id"`
	PlatformContractDigest string                 `json:"platform_contract_digest"`
	HealthAddress          string                 `json:"health_address"`
	InternalAddress        string                 `json:"internal_address"`
	ControlURL             string                 `json:"control_url"`
	ControlTLS             ClientTLS              `json:"control_tls"`
	ProofTLS               ServerTLS              `json:"proof_tls"`
	ControlPrincipals      []string               `json:"control_principals"`
	GatewayPrincipals      []string               `json:"gateway_principals"`
	NATSFile               string                 `json:"nats_file"`
	Policy                 PolicyConfig           `json:"policy"`
	Limits                 Limits                 `json:"limits"`
	Timing                 Timing                 `json:"timing"`
	Telemetry              *TelemetryConfig       `json:"telemetry,omitempty"`
	DatabaseURL            string                 `json:"-"`
	MigrationDatabaseURL   string                 `json:"-"`
}
type ClientTLS struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file"`
}
type ServerTLS struct {
	CertFile     string `json:"cert_file"`
	KeyFile      string `json:"key_file"`
	ClientCAFile string `json:"client_ca_file"`
}
type PolicyConfig struct {
	Version         string   `json:"version"`
	MaxRunAge       Duration `json:"max_run_age"`
	MaxReplyAge     Duration `json:"max_reply_age"`
	MaxFutureSkew   Duration `json:"max_future_skew"`
	LeaseTTL        Duration `json:"lease_ttl"`
	RenewalInterval Duration `json:"renewal_interval"`
	RetryBackoff    Duration `json:"retry_backoff"`
	MaxAttempts     int      `json:"max_attempts"`
}

func (p PolicyConfig) Domain() domain.Policy {
	return domain.Policy{Version: p.Version, MaxRunAge: p.MaxRunAge.Value(), MaxReplyAge: p.MaxReplyAge.Value(), MaxFutureSkew: p.MaxFutureSkew.Value(), LeaseTTL: p.LeaseTTL.Value(), RenewalInterval: p.RenewalInterval.Value(), RetryBackoff: p.RetryBackoff.Value(), MaxAttempts: p.MaxAttempts}
}

type Limits struct {
	MaxActiveAttempts          int   `json:"max_active_attempts"`
	MaxQueuedRuns              int   `json:"max_queued_runs"`
	MaxRetainedRuns            int   `json:"max_retained_runs"`
	MaxManifests               int   `json:"max_manifests"`
	MaxSnapshotBytes           int   `json:"max_snapshot_bytes"`
	MaxCredentialResponseBytes int64 `json:"max_credential_response_bytes"`
	MaxTrackedAttempts         int   `json:"max_tracked_attempts"`
	ScanBatch                  int   `json:"scan_batch"`
	ReplyBatch                 int   `json:"reply_batch"`
	MaxProofQueries            int   `json:"max_proof_queries"`
	MaxDatabaseConnections     int32 `json:"max_database_connections"`
}
type Timing struct {
	StartupTimeout        Duration `json:"startup_timeout"`
	OperationTimeout      Duration `json:"operation_timeout"`
	PollInterval          Duration `json:"poll_interval"`
	HealthInterval        Duration `json:"health_interval"`
	RequestTimeout        Duration `json:"request_timeout"`
	SDKDrainTimeout       Duration `json:"sdk_drain_timeout"`
	ShutdownDrainTimeout  Duration `json:"shutdown_drain_timeout"`
	ShutdownCancelTimeout Duration `json:"shutdown_cancel_timeout"`
	ProofTimeout          Duration `json:"proof_timeout"`
	HTTPShutdownTimeout   Duration `json:"http_shutdown_timeout"`
}

func LoadConfig() (Config, error) {
	var c Config
	if err := readJSONFile(os.Getenv("WORKER_CONFIG_FILE"), false, &c); err != nil {
		return c, err
	}
	c.DatabaseURL = strings.TrimSpace(os.Getenv("WORKER_DATABASE_URL"))
	c.MigrationDatabaseURL = strings.TrimSpace(os.Getenv("WORKER_MIGRATION_DATABASE_URL"))
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.Tracing != nil {
		if err := c.Tracing.Validate(); err != nil {
			return err
		}
	}
	if c.Telemetry != nil {
		if c.Telemetry.MetricsEndpoint == "" {
			return errors.New("explicit telemetry metrics endpoint is required")
		}
		if err := c.Telemetry.Export().Validate(); err != nil {
			return err
		}
	}
	if c.WorkerID == "" || len(c.WorkerID) > 256 || !domain.DigestValid(c.PlatformContractDigest) || c.DatabaseURL == "" || c.MigrationDatabaseURL == "" {
		return errors.New("worker identity, release contract digest and separate database URLs are required")
	}
	for _, address := range []string{c.HealthAddress, c.InternalAddress} {
		if _, _, err := net.SplitHostPort(address); err != nil {
			return errors.New("worker listener address is invalid")
		}
	}
	if c.HealthAddress == c.InternalAddress && !strings.HasSuffix(c.HealthAddress, ":0") {
		return errors.New("worker health and proof listeners must differ")
	}
	u, err := url.Parse(c.ControlURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("Control runtime URL must be a fixed HTTPS origin")
	}
	for _, path := range []string{c.NATSFile, c.ControlTLS.CertFile, c.ControlTLS.KeyFile, c.ControlTLS.CAFile, c.ProofTLS.CertFile, c.ProofTLS.KeyFile, c.ProofTLS.ClientCAFile} {
		if !filepath.IsAbs(path) {
			return errors.New("worker configuration paths must be absolute")
		}
	}
	if c.Policy.Domain().Validate() != nil {
		return errors.New("worker recovery policy is invalid")
	}
	l := c.Limits
	if l.MaxActiveAttempts < 1 || l.MaxQueuedRuns < 1 || l.MaxRetainedRuns < 1 || l.MaxManifests < 1 || l.MaxSnapshotBytes < 1 || l.MaxCredentialResponseBytes < 1 || l.MaxTrackedAttempts < l.MaxActiveAttempts || l.ScanBatch < 1 || l.ScanBatch > 1000 || l.ReplyBatch < 1 || l.ReplyBatch > 1000 || l.MaxProofQueries < 1 || l.MaxProofQueries > 1024 || l.MaxDatabaseConnections < 2 {
		return errors.New("worker capacities must be explicit positive values")
	}
	for _, d := range []Duration{c.Timing.StartupTimeout, c.Timing.OperationTimeout, c.Timing.PollInterval, c.Timing.HealthInterval, c.Timing.RequestTimeout, c.Timing.SDKDrainTimeout, c.Timing.ShutdownDrainTimeout, c.Timing.ShutdownCancelTimeout, c.Timing.ProofTimeout, c.Timing.HTTPShutdownTimeout} {
		if d.Value() < time.Millisecond {
			return errors.New("worker operation durations must be explicit positive values")
		}
	}
	if c.Timing.ProofTimeout.Value() > time.Minute {
		return errors.New("proof timeout exceeds protocol bound")
	}
	seen := map[string]bool{}
	for _, group := range [][]string{c.ControlPrincipals, c.GatewayPrincipals} {
		if len(group) == 0 || len(group) > 64 {
			return errors.New("explicit proof caller allowlists are required")
		}
		for _, principal := range group {
			u, err := url.Parse(principal)
			if err != nil || u.Scheme != "spiffe" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || seen[principal] {
				return errors.New("proof caller URI mappings must be valid and disjoint")
			}
			seen[principal] = true
		}
	}
	return nil
}
func readJSONFile(path string, private bool, target any) error {
	if !filepath.IsAbs(path) {
		return errors.New("worker configuration requires absolute file path")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64*1024 {
		return errors.New("worker configuration file unavailable or oversized")
	}
	if private && info.Mode().Perm()&0077 != 0 {
		return errors.New("worker private configuration must be owner-readable only")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return errors.New("worker configuration file unavailable")
	}
	defer clear(raw)
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return errors.New("worker configuration JSON is invalid")
	}
	defer clear(canonical)
	if !exactFields(canonical, reflect.TypeOf(target).Elem()) {
		return errors.New("worker configuration fields are invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return errors.New("worker configuration shape is invalid")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("worker configuration has trailing data")
	}
	return nil
}
func exactFields(raw []byte, t reflect.Type) bool {
	if bytes.Equal(raw, []byte("null")) {
		return false
	}
	if t.Kind() == reflect.Pointer {
		return exactFields(raw, t.Elem())
	}
	if t.Kind() != reflect.Struct {
		return true
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	known := map[string]reflect.Type{}
	required := 0
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			known[name] = field.Type
			if !strings.Contains(field.Tag.Get("json"), ",omitempty") {
				required++
			}
		}
	}
	for k, v := range fields {
		typ, ok := known[k]
		if !ok || !exactFields(v, typ) {
			return false
		}
	}
	if len(fields) < required {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name != "" && name != "-" && !strings.Contains(tag, ",omitempty") {
			if _, ok := fields[name]; !ok {
				return false
			}
		}
	}
	return true
}
