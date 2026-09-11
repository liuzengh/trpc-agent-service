package channelv1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/gowebpki/jcs"
)

// Preflight DTOs are independent of normal runtime credential consumers. They
// authorize no execution and never contain a webhook secret or raw provider URL.
type PreflightCreateRequest struct {
	ExpectedBotSecretVersion   int64 `json:"expected_bot_secret_version,omitempty"`
	AllowConnectionProbe       bool  `json:"allow_connection_probe,omitempty"`
	ExpectedAccountRevision    int64 `json:"expected_account_revision"`
	ExpectedConnectionRevision int64 `json:"expected_connection_revision"`
	ExpectedBotTokenVersion    int64 `json:"expected_bot_token_version,omitempty"`
}
type PreflightCreated struct {
	PreflightID   string    `json:"preflight_id"`
	TenantID      string    `json:"tenant_id"`
	AccountID     string    `json:"account_id"`
	RequestedAt   time.Time `json:"requested_at"`
	JobDeadlineAt time.Time `json:"job_deadline_at"`
	StatusURL     string    `json:"status_url"`
}
type PreflightView struct {
	BotSecretVersion       int64            `json:"bot_secret_version,omitempty"`
	AllowConnectionProbe   bool             `json:"allow_connection_probe,omitempty"`
	EndpointProfile        string           `json:"endpoint_profile,omitempty"`
	ReceiveMode            string           `json:"receive_mode,omitempty"`
	DiagnosticPolicy       string           `json:"diagnostic_policy,omitempty"`
	EffectiveConfigDigest  string           `json:"effective_config_digest,omitempty"`
	PreflightID            string           `json:"preflight_id"`
	TenantID               string           `json:"tenant_id"`
	AccountID              string           `json:"account_id"`
	Provider               string           `json:"provider"`
	ProviderAccountID      string           `json:"provider_account_id"`
	RequestedBy            string           `json:"requested_by"`
	AccountRevision        int64            `json:"account_revision"`
	ConnectionRevision     int64            `json:"connection_revision"`
	BotTokenVersion        int64            `json:"bot_token_version,omitempty"`
	State                  string           `json:"state"`
	Outcome                string           `json:"outcome"`
	ReasonCode             string           `json:"reason_code"`
	Freshness              string           `json:"freshness"`
	MetadataChanged        bool             `json:"metadata_changed"`
	RequestedAt            time.Time        `json:"requested_at"`
	StartedAt              *time.Time       `json:"started_at"`
	CheckedAt              *time.Time       `json:"checked_at"`
	JobDeadlineAt          time.Time        `json:"job_deadline_at"`
	ExpiresAt              *time.Time       `json:"expires_at"`
	GatewayConfigDigest    *string          `json:"gateway_config_digest"`
	GatewayConfigFreshness *string          `json:"gateway_config_freshness"`
	ExpectedPublicOrigin   *string          `json:"expected_public_origin"`
	Checks                 []PreflightCheck `json:"checks"`
}
type PreflightClaimRequest struct {
	DiagnosticPolicy     string  `json:"diagnostic_policy,omitempty"`
	SchemaVersion        int     `json:"schema_version"`
	ScopeID              string  `json:"scope_id"`
	SourceEpoch          string  `json:"source_epoch"`
	InstanceEpoch        string  `json:"instance_epoch"`
	ClaimRequestID       string  `json:"claim_request_id"`
	ClaimToken           string  `json:"claim_token"`
	GatewayConfigDigest  string  `json:"gateway_config_digest"`
	ExpectedPublicOrigin *string `json:"expected_public_origin"`
	OriginStatus         string  `json:"origin_status"`
	Limit                int     `json:"limit"`
}
type PreflightCredential struct {
	Purpose           string `json:"purpose"`
	CredentialID      string `json:"credential_id"`
	CredentialVersion int64  `json:"credential_version"`
	Configured        bool   `json:"configured"`
}
type PreflightGrant struct {
	AllowConnectionProbe    bool                `json:"allow_connection_probe,omitempty"`
	EndpointProfile         string              `json:"endpoint_profile,omitempty"`
	ReceiveMode             string              `json:"receive_mode,omitempty"`
	DiagnosticPolicy        string              `json:"diagnostic_policy,omitempty"`
	EffectiveConfigDigest   string              `json:"effective_config_digest,omitempty"`
	SchemaVersion           int                 `json:"schema_version"`
	ServerTime              time.Time           `json:"server_time"`
	PreflightID             string              `json:"preflight_id"`
	ScopeID                 string              `json:"scope_id"`
	SourceEpoch             string              `json:"source_epoch"`
	TenantID                string              `json:"tenant_id"`
	AccountID               string              `json:"account_id"`
	Provider                string              `json:"provider"`
	ProviderAccountID       string              `json:"provider_account_id"`
	AccountRevision         int64               `json:"account_revision"`
	ConnectionRevision      int64               `json:"connection_revision"`
	WebhookPath             string              `json:"webhook_path"`
	Credentials             PreflightCredential `json:"credentials"`
	WebhookSecretConfigured bool                `json:"webhook_secret_configured"`
	LeaseEpoch              int64               `json:"lease_epoch"`
	LeaseExpiresAt          time.Time           `json:"lease_expires_at"`
	JobDeadlineAt           time.Time           `json:"job_deadline_at"`
	GatewayConfigDigest     string              `json:"gateway_config_digest"`
}
type PreflightResolveRequest struct {
	SchemaVersion int    `json:"schema_version"`
	ScopeID       string `json:"scope_id"`
	SourceEpoch   string `json:"source_epoch"`
	InstanceEpoch string `json:"instance_epoch"`
	LeaseEpoch    int64  `json:"lease_epoch"`
	ClaimToken    string `json:"claim_token"`
}
type PreflightResolveResponse struct {
	SchemaVersion      int       `json:"schema_version"`
	PreflightID        string    `json:"preflight_id"`
	ConnectionRevision int64     `json:"connection_revision"`
	Purpose            string    `json:"purpose"`
	CredentialID       string    `json:"credential_id"`
	CredentialVersion  int64     `json:"credential_version"`
	Value              string    `json:"value"`
	LeaseExpiresAt     time.Time `json:"lease_expires_at"`
}
type PreflightCompleteRequest struct {
	EndpointProfile       string           `json:"endpoint_profile,omitempty"`
	ReceiveMode           string           `json:"receive_mode,omitempty"`
	DiagnosticPolicy      string           `json:"diagnostic_policy,omitempty"`
	EffectiveConfigDigest string           `json:"effective_config_digest,omitempty"`
	ConnectionRevision    int64            `json:"connection_revision,omitempty"`
	OriginStatus          string           `json:"origin_status,omitempty"`
	SchemaVersion         int              `json:"schema_version"`
	ScopeID               string           `json:"scope_id"`
	SourceEpoch           string           `json:"source_epoch"`
	InstanceEpoch         string           `json:"instance_epoch"`
	LeaseEpoch            int64            `json:"lease_epoch"`
	ClaimToken            string           `json:"claim_token"`
	GatewayConfigDigest   string           `json:"gateway_config_digest"`
	ExpectedPublicOrigin  *string          `json:"expected_public_origin"`
	ObservedAt            time.Time        `json:"observed_at"`
	Checks                []PreflightCheck `json:"checks"`
}
type PreflightCheck struct {
	ID      string          `json:"id"`
	Status  string          `json:"status"`
	Code    string          `json:"code"`
	Details json.RawMessage `json:"details"`
}

// PreflightConfigDigest hashes the frozen, non-secret diagnostic configuration.
func PreflightConfigDigest(scope, epoch string, origin *string, status string) (string, error) {
	if !preflightID.MatchString(scope) || !preflightEpoch.MatchString(epoch) {
		return "", ErrInvalidDocument
	}
	switch status {
	case "PUBLIC_ORIGIN_STATIC_VALID":
		if origin == nil {
			return "", ErrInvalidDocument
		}
		canonical, classification := normalizePreflightOrigin(*origin)
		if classification != status || canonical == nil || *canonical != *origin {
			return "", ErrInvalidDocument
		}
	case "PUBLIC_ORIGIN_INVALID", "PUBLIC_ORIGIN_NOT_PUBLIC":
		if origin != nil {
			return "", ErrInvalidDocument
		}
	default:
		return "", ErrInvalidDocument
	}

	raw, err := json.Marshal(struct {
		SchemaVersion     int     `json:"schema_version"`
		PolicyVersion     string  `json:"policy_version"`
		ScopeID           string  `json:"scope_id"`
		SourceEpoch       string  `json:"source_epoch"`
		TelegramAPIOrigin string  `json:"telegram_api_origin"`
		PublicOrigin      *string `json:"public_origin"`
		OriginStatus      string  `json:"origin_status"`
	}{1, "telegram-preflight-v1", scope, epoch, "https://api.telegram.org", origin, status})
	if err != nil {
		return "", ErrInvalidDocument
	}
	raw, err = jcs.Transform(raw)
	if err != nil {
		return "", ErrInvalidDocument
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// ValidatePreflightChecks accepts only the fixed eight redacted facts. The final
// two limitations remain UNKNOWN and never make a diagnostic PASS an admission.
func ValidatePreflightChecks(checks []PreflightCheck) (string, error) {
	compiled.Do(compile)
	if compiled.err != nil {
		return "", ErrUnknownSchema
	}
	_, ok := compiled.schemas["preflight-complete.schema.json"]
	if !ok {
		return "", ErrUnknownSchema
	}
	raw, err := json.Marshal(checks)
	if err != nil {
		return "", ErrInvalidDocument
	}
	if _, err = jcs.Transform(raw); err != nil {
		return "", ErrInvalidDocument
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil || compiled.schemas["preflight-checks.schema.json"].OneOf[0].Properties["checks"].Validate(value) != nil {
		return "", ErrInvalidDocument
	}
	// A missing token prevents all remote facts. getWebhookInfo follows verified
	// identity only; its failure leaves downstream fields unknown, never invented.
	if checks[0].Code == "BOT_TOKEN_MISSING" {
		if checks[1].Code != "NOT_EXECUTED" {
			return "", ErrInvalidDocument
		}
	} else if checks[1].Code == "NOT_EXECUTED" {
		return "", ErrInvalidDocument
	}
	if checks[1].Code != "BOT_IDENTITY_MATCH" {
		if checks[3].Code != "NOT_EXECUTED" {
			return "", ErrInvalidDocument
		}
	} else if checks[3].Code == "NOT_EXECUTED" {
		return "", ErrInvalidDocument
	}
	webhookRead := checks[3].Code == "WEBHOOK_MATCH" || checks[3].Code == "WEBHOOK_DIFFERENT" || checks[3].Code == "WEBHOOK_NONE" || checks[3].Code == "WEBHOOK_COMPARISON_UNAVAILABLE"
	if webhookRead {
		if checks[4].Code == "NOT_EXECUTED" || checks[5].Code == "NOT_EXECUTED" {
			return "", ErrInvalidDocument
		}
	} else if checks[4].Code != "NOT_EXECUTED" || checks[5].Code != "NOT_EXECUTED" {
		return "", ErrInvalidDocument
	}
	if checks[2].Code == "PUBLIC_ORIGIN_STATIC_VALID" {
		if checks[3].Code == "WEBHOOK_COMPARISON_UNAVAILABLE" {
			return "", ErrInvalidDocument
		}
	} else if checks[3].Code == "WEBHOOK_MATCH" || checks[3].Code == "WEBHOOK_DIFFERENT" {
		return "", ErrInvalidDocument
	}
	outcome := "PASS"
	rank := map[string]int{"PASS": 0, "WARN": 1, "UNKNOWN": 2, "SKIPPED": 2, "FAIL": 3}
	for _, c := range checks[:6] {
		if rank[c.Status] > rank[outcome] {
			outcome = c.Status
			if outcome == "SKIPPED" {
				outcome = "UNKNOWN"
			}
		}
	}
	return outcome, nil
}

var preflightID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var preflightEpoch = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// The diagnostic origin policy is a pure static contract: no DNS or remote IO.
func normalizePreflightOrigin(raw string) (*string, string) {
	if raw == "" || len(raw) > 2048 || strings.ContainsAny(raw, "%\\?#") {
		return nil, "PUBLIC_ORIGIN_INVALID"
	}
	for _, c := range raw {
		if c <= ' ' || c >= 127 {
			return nil, "PUBLIC_ORIGIN_INVALID"
		}
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Opaque != "" || u.User != nil || u.Host == "" || u.RawPath != "" || u.ForceQuery || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, "PUBLIC_ORIGIN_INVALID"
	}
	host, port := strings.ToLower(u.Hostname()), u.Port()
	if port != "" && port != "443" && port != "80" && port != "88" && port != "8443" {
		return nil, "PUBLIC_ORIGIN_INVALID"
	}
	if strings.HasSuffix(u.Host, ":") {
		return nil, "PUBLIC_ORIGIN_INVALID"
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		if !preflightPublicLiteral(addr) {
			return nil, "PUBLIC_ORIGIN_NOT_PUBLIC"
		}
		host = addr.String()
		if addr.Is6() {
			host = "[" + host + "]"
		}
	} else {
		if strings.ContainsAny(host, ":[]") || !preflightValidHostname(host) {
			return nil, "PUBLIC_ORIGIN_INVALID"
		}
		if !strings.Contains(host, ".") || host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".home.arpa") {
			return nil, "PUBLIC_ORIGIN_NOT_PUBLIC"
		}
		// Reject historical numeric address spellings instead of accepting them as
		// DNS names (e.g. 127.1, 0177.0.0.1, or 0x7f.0.0.1).
		last := host[strings.LastIndexByte(host, '.')+1:]
		if strings.Trim(last, "0123456789") == "" {
			return nil, "PUBLIC_ORIGIN_NOT_PUBLIC"
		}
	}
	if port != "" && port != "443" {
		if strings.HasPrefix(host, "[") {
			host = net.JoinHostPort(strings.Trim(host, "[]"), port)
		} else {
			host += ":" + port
		}
	}
	origin := "https://" + host
	return &origin, "PUBLIC_ORIGIN_STATIC_VALID"
}

func preflightValidHostname(host string) bool {
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

// The deny set is intentionally conservative for literal addresses. Domain
// names are not resolved, so STATIC_VALID never claims public reachability.
var preflightNonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

func preflightPublicLiteral(addr netip.Addr) bool {
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.Is4In6() {
		return false
	}
	if addr.Is6() && !netip.MustParsePrefix("2000::/3").Contains(addr) {
		return false
	}
	for _, prefix := range preflightNonPublicPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

func validatePreflightSemantics(name string, raw []byte) error {
	switch name {
	case "preflight-claim.schema.json":
		var claim PreflightClaimRequest
		if json.Unmarshal(raw, &claim) != nil {
			return ErrInvalidDocument
		}
		d, err := PreflightConfigDigestForPolicy(claim.DiagnosticPolicy, claim.ScopeID, claim.SourceEpoch, claim.ExpectedPublicOrigin, claim.OriginStatus)
		if err != nil || d != claim.GatewayConfigDigest {
			return ErrInvalidDocument
		}
	case "preflight-complete.schema.json":
		var result PreflightCompleteRequest
		if json.Unmarshal(raw, &result) != nil {
			return ErrInvalidDocument
		}
		if _, err := ValidatePreflightChecksForMode(result.DiagnosticPolicy, result.ReceiveMode, result.Checks); err != nil {
			return err
		}
		status := result.Checks[2].Code
		if result.DiagnosticPolicy != "" {
			status = result.OriginStatus
		}
		d, err := PreflightConfigDigestForPolicy(result.DiagnosticPolicy, result.ScopeID, result.SourceEpoch, result.ExpectedPublicOrigin, status)
		if err != nil || d != result.GatewayConfigDigest {
			return ErrInvalidDocument
		}
		if result.DiagnosticPolicy != "" {
			effective, e := PreflightEffectiveConfigDigest(result.ScopeID, result.SourceEpoch, result.ReceiveMode, result.ConnectionRevision, result.ExpectedPublicOrigin, result.OriginStatus, result.EndpointProfile)
			if e != nil || effective != result.EffectiveConfigDigest {
				return ErrInvalidDocument
			}
		}
	case "preflight-view.schema.json":
		var view PreflightView
		if json.Unmarshal(raw, &view) != nil {
			return ErrInvalidDocument
		}
		if (view.ReceiveMode == "long_polling" || view.ReceiveMode == PreflightWeComMode) && view.ExpectedPublicOrigin != nil {
			return ErrInvalidDocument
		}
		if view.ExpectedPublicOrigin != nil {
			origin, status := normalizePreflightOrigin(*view.ExpectedPublicOrigin)
			if status != "PUBLIC_ORIGIN_STATIC_VALID" || origin == nil || *origin != *view.ExpectedPublicOrigin {
				return ErrInvalidDocument
			}
		}
		if view.State == "COMPLETED" {
			outcome, err := ValidatePreflightChecksForMode(view.DiagnosticPolicy, view.ReceiveMode, view.Checks)
			if err != nil || outcome != view.Outcome {
				return ErrInvalidDocument
			}
			if view.Provider == "telegram" && (view.ExpectedPublicOrigin != nil) != (view.Checks[2].Code == "PUBLIC_ORIGIN_STATIC_VALID") {
				return ErrInvalidDocument
			}
		}
	case "preflight-grant.schema.json":
		var grant PreflightGrant
		if json.Unmarshal(raw, &grant) != nil || grant.Provider == "telegram" && grant.WebhookPath != "/v1/telegram/"+grant.AccountID {
			return ErrInvalidDocument
		}
	case "preflight-created.schema.json":
		var created PreflightCreated
		if json.Unmarshal(raw, &created) != nil || created.StatusURL != "/v1/tenants/"+created.TenantID+"/channel-accounts/"+created.AccountID+"/preflights/"+created.PreflightID {
			return ErrInvalidDocument
		}
	}
	return nil
}
