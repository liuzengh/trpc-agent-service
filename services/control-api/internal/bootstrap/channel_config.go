package bootstrap

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"

	"github.com/gowebpki/jcs"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/credentialcrypto"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

// ChannelConfig is platform configuration, not a user-managed environment.
// Nil leaves the optional Channel surface unregistered; partial config is fatal.
type ChannelConfig struct {
	RouteNATSFile      string                  `json:"route_nats_file"`
	ScopeID            string                  `json:"scope_id"`
	SourceEpoch        string                  `json:"source_epoch"`
	InternalAddress    string                  `json:"internal_address"`
	TLSCertFile        string                  `json:"tls_cert_file"`
	TLSKeyFile         string                  `json:"tls_key_file"`
	ClientCAFile       string                  `json:"client_ca_file"`
	CredentialKeysFile string                  `json:"credential_keys_file"`
	MaxTenantAccounts  int                     `json:"max_tenant_accounts"`
	Workloads          []channelWorkloadConfig `json:"workloads"`
	nats               channelNATSConfig
	cipher             *credentialcrypto.Keyring
}
type channelWorkloadConfig struct {
	PrincipalID string   `json:"principal_id"`
	InstanceID  string   `json:"instance_id"`
	ScopeID     string   `json:"scope_id"`
	Audience    string   `json:"audience"`
	Consumers   []string `json:"consumers"`
}

func (c *ChannelConfig) principals() []application.WorkloadPrincipal {
	out := make([]application.WorkloadPrincipal, 0, len(c.Workloads))
	for _, p := range c.Workloads {
		out = append(out, application.WorkloadPrincipal{PrincipalID: p.PrincipalID, InstanceID: p.InstanceID, ScopeID: p.ScopeID, Audience: p.Audience, Consumers: p.Consumers})
	}
	return out
}
func readConfigFile(path string, private bool, target any) error {
	if path == "" || !filepath.IsAbs(path) {
		return errors.New("channel config requires absolute file paths")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64*1024 {
		return errors.New("channel configuration file is unavailable or oversized")
	}
	if private && info.Mode().Perm()&0077 != 0 {
		return errors.New("channel key file must be readable only by its owner")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return errors.New("channel configuration file is unavailable")
	}
	defer clear(raw)
	if _, err = jcs.Transform(raw); err != nil {
		return errors.New("channel configuration JSON is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(target); err != nil {
		return errors.New("channel configuration shape is invalid")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return errors.New("channel configuration must contain one JSON object")
	}
	return nil
}
func loadChannelConfig(path string) (*ChannelConfig, error) {
	if path == "" {
		return nil, nil
	}
	var cfg ChannelConfig
	if err := readConfigFile(path, false, &cfg); err != nil {
		return nil, err
	}
	if !domain.ValidID(cfg.ScopeID) || !domain.ValidEpoch(cfg.SourceEpoch) || cfg.InternalAddress == "" || len(cfg.Workloads) == 0 || len(cfg.Workloads) > 32 {
		return nil, errors.New("channel scope, epoch, listener and workload mappings are required")
	}
	if err := cfg.validateWorkloads(); err != nil {
		return nil, err
	}
	if cfg.MaxTenantAccounts < 0 || cfg.MaxTenantAccounts > domain.MaxAccounts {
		return nil, errors.New("channel tenant account limit is invalid")
	}
	if err := readConfigFile(cfg.RouteNATSFile, true, &cfg.nats); err != nil {
		return nil, err
	}
	if _, err := cfg.nats.options(); err != nil {
		return nil, err
	}
	var keys struct {
		Active string `json:"active_key_id"`
		Keys   map[string]struct {
			Encryption string `json:"encryption_key"`
			MAC        string `json:"mac_key"`
		} `json:"keys"`
	}
	if err := readConfigFile(cfg.CredentialKeysFile, true, &keys); err != nil {
		return nil, err
	}
	if len(keys.Keys) == 0 || len(keys.Keys) > 8 {
		return nil, errors.New("channel keyring size is invalid")
	}
	material := make(map[string]credentialcrypto.Key, len(keys.Keys))
	defer func() {
		for _, k := range material {
			clear(k.Encryption)
			clear(k.MAC)
		}
	}()
	for id, k := range keys.Keys {
		encryption, e := base64.StdEncoding.Strict().DecodeString(k.Encryption)
		mac, m := base64.StdEncoding.Strict().DecodeString(k.MAC)
		if e != nil || m != nil || !domain.ValidID(id) {
			clear(encryption)
			clear(mac)
			return nil, errors.New("channel key encoding or identity is invalid")
		}
		material[id] = credentialcrypto.Key{Encryption: encryption, MAC: mac}
	}
	cipher, err := credentialcrypto.New(keys.Active, material)
	if err != nil {
		return nil, errors.New("channel keyring is invalid")
	}
	cfg.cipher = cipher
	if _, err = cfg.tlsConfig(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
func (c *ChannelConfig) tlsConfig() (*tls.Config, error) {
	if c == nil || c.cipher == nil {
		return nil, errors.New("channel configuration was not loaded")
	}
	for _, path := range []string{c.TLSCertFile, c.TLSKeyFile, c.ClientCAFile} {
		if !filepath.IsAbs(path) {
			return nil, errors.New("channel TLS paths must be absolute")
		}
	}
	info, err := os.Stat(c.TLSKeyFile)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("channel TLS key must be readable only by its owner")
	}
	certificate, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
	if err != nil {
		return nil, errors.New("channel server certificate is invalid")
	}
	ca, err := os.ReadFile(c.ClientCAFile)
	if err != nil {
		return nil, errors.New("channel client CA is unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("channel client CA is invalid")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}, nil
}

// validateWorkloads closes the workload capability set at the configuration
// boundary. Diagnostic permission is explicit and never inferred from runtime
// registration or credential-use permissions.
func (c *ChannelConfig) validateWorkloads() error {
	if len(c.Workloads) == 0 || len(c.Workloads) > 32 {
		return errors.New("channel workload mappings are required")
	}
	principals, instances := map[string]bool{}, map[string]bool{}
	for _, p := range c.Workloads {
		u, err := url.Parse(p.PrincipalID)
		if err != nil || u.Scheme != "spiffe" || u.Host == "" || u.Path == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil || p.Audience != application.WorkloadAudience || !domain.ValidID(p.InstanceID) || !domain.ValidID(p.ScopeID) || p.ScopeID != c.ScopeID || principals[p.PrincipalID] || instances[p.InstanceID] {
			return errors.New("channel workload identity or scope is invalid")
		}
		kinds := map[string]bool{}
		for _, kind := range p.Consumers {
			if kinds[kind] || !slices.Contains([]string{"wecom_connection", "telegram_webhook", "telegram_delivery", "telegram_registration", "telegram_receiver", "telegram_preflight", "wecom_preflight"}, kind) {
				return errors.New("channel workload consumer is invalid")
			}
			kinds[kind] = true
		}
		principals[p.PrincipalID], instances[p.InstanceID] = true, true
	}
	return nil
}
