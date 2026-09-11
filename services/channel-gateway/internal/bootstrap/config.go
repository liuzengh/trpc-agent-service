package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	telegramruntime "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/telegramruntime"
	connectiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
)

type Account struct {
	ID        string `json:"account_id"`
	SecretEnv string `json:"webhook_secret_env"`
	Secret    string `json:"-"`
}
type Config struct {
	Tracing                                                               *telemetrytrace.Config
	WeComPreflightEnabled                                                 bool
	TelegramPreflightEnabled                                              bool
	telegramFactory                                                       telegramruntime.RemoteFactory
	AccountSource                                                         string
	Control                                                               ControlConfig
	Worker                                                                WorkerConfig
	HTTPAddress, AdminAddress, DatabaseURL, MigrationDatabaseURL, NATSURL string
	Topology                                                              transport.Topology
	NATSAuth                                                              transport.Auth
	Accounts                                                              []Account
	WeComAccounts                                                         []connectiondomain.Account
	WeComAccountsFile, WeComURL, InstanceID, TelegramAPIURL               string
	ConnectionOptions                                                     connection.Options
}

func envOr(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
func LoadConfig() (Config, error) {
	c := Config{HTTPAddress: envOr("GATEWAY_HTTP_ADDRESS", ":8090"), AdminAddress: envOr("GATEWAY_ADMIN_ADDRESS", ":8091"), DatabaseURL: os.Getenv("GATEWAY_DATABASE_URL"), MigrationDatabaseURL: os.Getenv("GATEWAY_MIGRATION_DATABASE_URL"), NATSURL: os.Getenv("GATEWAY_NATS_URL"), NATSAuth: transport.Auth{User: os.Getenv("GATEWAY_NATS_USER"), Password: os.Getenv("GATEWAY_NATS_PASSWORD"), InboxPrefix: "_INBOX.gateway", CAFile: os.Getenv("GATEWAY_NATS_CA_FILE")}}
	c.AccountSource = envOr("GATEWAY_ACCOUNT_SOURCE", "control")
	c.Control = ControlConfig{URL: os.Getenv("GATEWAY_CONTROL_URL"), CAFile: os.Getenv("GATEWAY_CONTROL_CA_FILE"), CertificateFile: os.Getenv("GATEWAY_CONTROL_CERT_FILE"), KeyFile: os.Getenv("GATEWAY_CONTROL_KEY_FILE"), ScopeID: os.Getenv("GATEWAY_CONTROL_SCOPE_ID"), SourceEpoch: os.Getenv("GATEWAY_CONTROL_SOURCE_EPOCH"), PublicOrigin: os.Getenv("GATEWAY_PUBLIC_ORIGIN")}
	c.Worker = WorkerConfig{URL: os.Getenv("GATEWAY_WORKER_URL"), CAFile: os.Getenv("GATEWAY_WORKER_CA_FILE"), CertificateFile: os.Getenv("GATEWAY_WORKER_CERT_FILE"), KeyFile: os.Getenv("GATEWAY_WORKER_KEY_FILE")}
	c.WeComAccountsFile = os.Getenv("GATEWAY_WECOM_ACCOUNTS_FILE")
	c.WeComURL = os.Getenv("GATEWAY_WECOM_URL")
	c.TelegramAPIURL = os.Getenv("GATEWAY_TELEGRAM_API_URL")
	c.InstanceID = os.Getenv("GATEWAY_INSTANCE_ID")
	var err error
	c.Tracing, err = loadTracingConfig(os.Getenv("GATEWAY_TRACING_CONFIG_FILE"))
	if err != nil {
		return Config{}, err
	}
	c.WeComAccounts, err = (accountFileSource{path: c.WeComAccountsFile}).List(context.Background())
	if err != nil {
		return Config{}, err
	}
	c.Topology, err = transport.LoadTopology(envOr("GATEWAY_NATS_TOPOLOGY_FILE", "deploy/nats/streams.yaml"))
	if err != nil {
		return Config{}, err
	}
	if c.AccountSource == "control" {
		switch envOr("GATEWAY_WECOM_PREFLIGHT_ENABLED", "true") {
		case "true":
			c.WeComPreflightEnabled = true
		case "false":
		default:
			return Config{}, errors.New("invalid WeCom preflight enablement")
		}
		switch envOr("GATEWAY_TELEGRAM_PREFLIGHT_ENABLED", "true") {
		case "true":
			c.TelegramPreflightEnabled = true
		case "false":
		default:
			return Config{}, errors.New("invalid Telegram preflight enablement")
		}
		if os.Getenv("GATEWAY_TELEGRAM_ACCOUNTS_FILE") != "" {
			return Config{}, errors.New("Control mode does not accept static Telegram accounts")
		}
		return c, c.Validate()
	}
	path := os.Getenv("GATEWAY_TELEGRAM_ACCOUNTS_FILE")
	if path == "" {
		return Config{}, errors.New("GATEWAY_TELEGRAM_ACCOUNTS_FILE is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return Config{}, errors.New("open Telegram account configuration failed")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(b) > 65536 {
		return Config{}, errors.New("Telegram account configuration exceeds limit")
	}
	c.Accounts, err = decodeAccounts(b)
	if err != nil {
		return Config{}, err
	}
	for i := range c.Accounts {
		c.Accounts[i].Secret = os.Getenv(c.Accounts[i].SecretEnv)
	}
	if err = c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}
func (c Config) Validate() error {
	if c.Tracing != nil {
		if err := c.Tracing.Validate(); err != nil {
			return err
		}
		if c.InstanceID == "" {
			return errors.New("Gateway tracing requires an instance identity")
		}
	}
	if err := validateTelegramAPIURL(c.TelegramAPIURL); err != nil {
		return err
	}
	if c.AccountSource != "" && c.AccountSource != "fixture" && c.AccountSource != "control" {
		return errors.New("invalid Gateway account source mode")
	}
	if c.AccountSource == "control" {
		if len(c.Accounts) > 0 || len(c.WeComAccounts) > 0 || c.WeComAccountsFile != "" {
			return errors.New("Control mode does not accept file or environment accounts")
		}
		if err := c.Worker.validate(); err != nil {
			return err
		}
		if !c.Topology.HasStream(transport.ReplyStream) {
			return errors.New("Control mode requires the V1 Reply stream")
		}
		if err := c.Control.validate(c.InstanceID); err != nil {
			return err
		}
	}
	if c.DatabaseURL == "" || c.MigrationDatabaseURL == "" || c.NATSURL == "" {
		return errors.New("Gateway runtime database, migration database and NATS URLs are required")
	}
	if _, _, err := net.SplitHostPort(c.HTTPAddress); err != nil {
		return errors.New("invalid public HTTP address")
	}
	if _, _, err := net.SplitHostPort(c.AdminAddress); err != nil {
		return errors.New("invalid administrative HTTP address")
	}
	if c.AdminAddress == c.HTTPAddress {
		return errors.New("separate public and administrative listeners are required")
	}
	if len(c.WeComAccounts) > 100 {
		return errors.New("at most 100 configured WeCom accounts")
	}
	if c.InstanceID != "" && !accountIDPattern.MatchString(c.InstanceID) {
		return errors.New("invalid Gateway instance identity")
	}
	if c.WeComURL != "" {
		u, err := url.Parse(c.WeComURL)
		if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("invalid WeCom URL")
		}
	}
	wecomIDs, wecomBots := map[string]bool{}, map[string]bool{}
	for _, a := range c.WeComAccounts {
		if a.Validate() != nil || !secretEnvPattern.MatchString(a.CredentialRef) || wecomIDs[a.ID] || wecomBots[a.BotID] {
			return errors.New("invalid or conflicting WeCom account projection")
		}
		wecomIDs[a.ID], wecomBots[a.BotID] = true, true
	}
	if len(c.Accounts) > 100 {
		return errors.New("at most 100 configured Telegram accounts")
	}
	if err := c.Topology.Validate(); err != nil {
		return err
	}
	if err := c.NATSAuth.ValidateServerURL(c.NATSURL); err != nil {
		return err
	}
	if (c.NATSAuth.User == "") != (c.NATSAuth.Password == "") {
		return errors.New("NATS username and password must be configured together")
	}
	seen := map[string]bool{}
	for _, a := range c.Accounts {
		if !accountIDPattern.MatchString(a.ID) || seen[a.ID] || !secretEnvPattern.MatchString(a.SecretEnv) {
			return errors.New("invalid Telegram account identity or secret reference")
		}
		seen[a.ID] = true
		if !webhookSecretPattern.MatchString(a.Secret) {
			return errors.New("Telegram webhook secret must contain 16..256 URL-safe characters")
		}
	}
	return nil
}

var (
	// Config uses the intersection of domain identifiers and the Telegram
	// Handler's URL-safe account alphabet. A configured account must reach both.
	accountIDPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	secretEnvPattern     = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
	webhookSecretPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,256}$`)
)

// decodeAccounts accepts an array of exactly the two documented string fields.
// encoding/json's struct decoder alone accepts duplicate fields (last wins) and
// case-insensitive aliases even with DisallowUnknownFields. Token validation
// rejects those ambiguities before any environment secret is resolved.
func decodeAccounts(data []byte) ([]Account, error) {
	invalid := errors.New("invalid Telegram account configuration")
	if !utf8.Valid(data) {
		return nil, invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return nil, invalid
	}
	accounts := make([]Account, 0)
	for decoder.More() {
		if len(accounts) >= 100 {
			return nil, errors.New("at most 100 configured Telegram accounts")
		}
		token, err = decoder.Token()
		if err != nil || token != json.Delim('{') {
			return nil, invalid
		}
		var account Account
		seen := make(map[string]bool, 2)
		for decoder.More() {
			token, err = decoder.Token()
			if err != nil {
				return nil, invalid
			}
			key, ok := token.(string)
			if !ok || seen[key] || (key != "account_id" && key != "webhook_secret_env") {
				return nil, invalid
			}
			seen[key] = true
			token, err = decoder.Token()
			if err != nil {
				return nil, invalid
			}
			value, ok := token.(string)
			if !ok {
				return nil, invalid
			}
			if key == "account_id" {
				account.ID = value
			} else {
				account.SecretEnv = value
			}
		}
		token, err = decoder.Token()
		if err != nil || token != json.Delim('}') || !seen["account_id"] || !seen["webhook_secret_env"] {
			return nil, invalid
		}
		accounts = append(accounts, account)
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim(']') {
		return nil, invalid
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing Telegram account configuration")
	}
	return accounts, nil
}

// A self-hosted Bot API is an operator-owned origin, never an account field.
// Plain HTTP is confined to literal loopback destinations for local servers.
func validateTelegramAPIURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(raw, "#") || u.Fragment != "" || u.RawFragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("Telegram API URL must be a fixed origin")
	}
	if u.Scheme == "https" {
		return nil
	}
	if ip := net.ParseIP(u.Hostname()); u.Scheme == "http" && ip != nil && ip.IsLoopback() {
		return nil
	}
	return errors.New("Telegram API URL requires HTTPS or literal loopback HTTP")
}
