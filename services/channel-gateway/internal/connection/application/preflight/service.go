package preflight

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

// Service executes only an already authorized, short-lived diagnostic grant.
// The caller owns its monotonic deadline and completion/retry policy.
type Service struct {
	Control Control
	Probe   TelegramProbe
}

// ValidateGrant verifies pinned metadata without consulting the runtime catalog
// or using the local wall clock to reinterpret Control's lease clock.
func ValidateGrant(g Grant, cfg ConfigSnapshot) error {
	if g.DiagnosticPolicy != g.Request.DiagnosticPolicy {
		return ErrConflict
	}
	if g.DiagnosticPolicy == "" {
		if g.ReceiveMode != "" || g.EffectiveConfigDigest != "" {
			return ErrInvalid
		}
	} else {
		if g.DiagnosticPolicy != wire.PreflightReceiveModesPolicy && g.DiagnosticPolicy != "wecom_long_connection_v1" {
			return ErrInvalid
		}
		effective, e := wire.PreflightEffectiveConfigDigest(g.ScopeID, g.SourceEpoch, g.ReceiveMode, g.ConnectionRevision, cfg.PublicOrigin, cfg.OriginStatus, g.EndpointProfile)
		if e != nil || effective != g.EffectiveConfigDigest {
			return ErrConflict
		}
	}

	if ValidateConfig(cfg) != nil || ValidateConfig(g.Request.Config) != nil {
		return ErrInvalid
	}
	if g.ScopeID != cfg.ScopeID || g.SourceEpoch != cfg.SourceEpoch || g.ConfigDigest != cfg.Digest || !sameConfig(g.Request.Config, cfg) {
		return ErrConflict
	}
	if !accountcatalog.ValidID(g.PreflightID) || !accountcatalog.ValidID(g.TenantID) || !accountcatalog.ValidID(g.AccountID) || !accountcatalog.ValidRevision(g.AccountRevision) || !accountcatalog.ValidRevision(g.ConnectionRevision) || g.ConnectionRevision > g.AccountRevision || g.LeaseEpoch < 1 || g.LeaseEpoch > 2 {
		return ErrInvalid
	}
	if !accountcatalog.ValidID(g.Credential.ID) || !accountcatalog.ValidRevision(g.Credential.Version) || !accountcatalog.ValidEpoch(g.Request.InstanceEpoch) || !accountcatalog.ValidEpoch(g.Request.RequestID) {
		return ErrInvalid
	}
	if g.Provider == "wecom" {
		if cfg.Policy != "wecom_long_connection_v1" || g.DiagnosticPolicy != cfg.Policy || g.ReceiveMode != "long_connection" || !g.AllowConnectionProbe || g.WebhookPath != "" || g.WebhookSecretConfigured || g.Credential.Purpose != "wecom.bot_secret" || !validWeComIdentity(g.ProviderAccountID) {
			return ErrInvalid
		}
	} else if g.Provider != "telegram" || cfg.Policy != "" || g.DiagnosticPolicy == "wecom_long_connection_v1" || g.AllowConnectionProbe || !positiveDecimal(g.ProviderAccountID) || g.WebhookPath != "/v1/telegram/"+g.AccountID || g.Credential.Purpose != "telegram.bot_token" {
		return ErrInvalid
	}
	token, err := base64.RawURLEncoding.DecodeString(g.Request.Token.Reveal())
	if err != nil || len(token) != 32 || base64.RawURLEncoding.EncodeToString(token) != g.Request.Token.Reveal() {
		return ErrInvalid
	}
	if g.ServerTime.IsZero() || g.LeaseExpiresAt.IsZero() || g.JobDeadlineAt.IsZero() {
		return ErrInvalid
	}
	lease, job := g.LeaseExpiresAt.Sub(g.ServerTime), g.JobDeadlineAt.Sub(g.ServerTime)
	if lease <= 0 || job <= 0 {
		return ErrExpired
	}
	if lease > 30*time.Second || job > 120*time.Second {
		return ErrInvalid
	}
	return nil
}

func (s Service) Execute(ctx context.Context, g Grant, cfg ConfigSnapshot) (Result, error) {
	if ctx == nil {
		return Result{}, ErrInvalid
	}
	if g.Provider != "telegram" {
		return Result{}, ErrInvalid
	}
	if err := ValidateGrant(g, cfg); err != nil {
		return Result{}, err
	}
	if ctx.Err() != nil {
		return Result{}, ErrExpired
	}
	// Copy pointer-bearing configuration before handing the grant to an adapter.
	cfg = cloneConfig(cfg)
	probe := ProbeResult{IdentityCode: "NOT_EXECUTED", WebhookCode: "NOT_EXECUTED", Relation: "UNKNOWN"}
	if g.Credential.Configured {
		if s.Control == nil || s.Probe == nil {
			return Result{}, ErrInvalid
		}
		token, err := s.Control.ResolveCredential(ctx, g)
		if err != nil {
			return Result{}, stableError(err)
		}
		if ctx.Err() != nil {
			return Result{}, ErrExpired
		}
		if token.Reveal() == "" {
			return Result{}, ErrInvalid
		}
		var expected *string
		if cfg.PublicOrigin != nil {
			value := *cfg.PublicOrigin + g.WebhookPath
			expected = &value
		}
		probe, err = s.Probe.Inspect(ctx, ProbeRequest{EndpointProfile: g.EndpointProfile, Token: token, ExpectedIdentity: g.ProviderAccountID, ExpectedWebhook: expected})
		if err != nil {
			return Result{}, stableError(err)
		}
		if ctx.Err() != nil {
			return Result{}, ErrExpired
		}
		if err := validateProbe(probe, cfg); err != nil {
			return Result{}, err
		}
	}
	checks := assembleChecks(g, cfg, probe)
	return Result{Config: cfg, ObservedAt: time.Now().UTC(), Checks: checks}, nil
}

func sameConfig(a, b ConfigSnapshot) bool {
	return a.Policy == b.Policy && a.ScopeID == b.ScopeID && a.SourceEpoch == b.SourceEpoch && a.Digest == b.Digest && a.OriginStatus == b.OriginStatus && (a.PublicOrigin == nil && b.PublicOrigin == nil || a.PublicOrigin != nil && b.PublicOrigin != nil && *a.PublicOrigin == *b.PublicOrigin)
}

func cloneConfig(cfg ConfigSnapshot) ConfigSnapshot {
	if cfg.PublicOrigin != nil {
		origin := *cfg.PublicOrigin
		cfg.PublicOrigin = &origin
	}
	return cfg
}

func positiveDecimal(value string) bool {
	if len(value) == 0 || len(value) > 1024 || value[0] < '1' || value[0] > '9' {
		return false
	}
	return strings.Trim(value, "0123456789") == ""
}

func stableError(err error) error {
	for _, stable := range []error{ErrInvalid, ErrDenied, ErrConflict, ErrUnavailable, ErrExpired} {
		if errors.Is(err, stable) {
			return stable
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ErrExpired
	}
	return ErrUnavailable
}

func providerFailure(code string) bool {
	switch code {
	case "PROVIDER_NETWORK", "PROVIDER_TIMEOUT", "PROVIDER_RATE_LIMITED", "PROVIDER_UNAVAILABLE", "PROVIDER_RESPONSE_INVALID":
		return true
	}
	return false
}

func validateProbe(p ProbeResult, cfg ConfigSnapshot) error {
	switch p.IdentityCode {
	case "BOT_IDENTITY_MATCH":
		if p.IdentityMatch == nil || !*p.IdentityMatch {
			return ErrInvalid
		}
	case "BOT_IDENTITY_MISMATCH":
		if p.IdentityMatch == nil || *p.IdentityMatch {
			return ErrInvalid
		}
	case "TOKEN_REJECTED":
		if p.IdentityMatch != nil {
			return ErrInvalid
		}
	default:
		if !providerFailure(p.IdentityCode) || p.IdentityMatch != nil {
			return ErrInvalid
		}
	}
	if p.IdentityCode != "BOT_IDENTITY_MATCH" {
		if p.WebhookCode != "" && p.WebhookCode != "NOT_EXECUTED" || p.Presence != nil || p.Relation != "" && p.Relation != "UNKNOWN" || p.PendingUpdates != nil || p.HasLastError != nil || p.LastErrorAt != nil {
			return ErrInvalid
		}
		return nil
	}
	read := false
	switch p.WebhookCode {
	case "WEBHOOK_MATCH":
		read = cfg.PublicOrigin != nil && p.Presence != nil && *p.Presence && p.Relation == "MATCH"
	case "WEBHOOK_DIFFERENT":
		read = cfg.PublicOrigin != nil && p.Presence != nil && *p.Presence && p.Relation == "DIFFERENT"
	case "WEBHOOK_NONE":
		read = p.Presence != nil && !*p.Presence && p.Relation == "NONE"
	case "WEBHOOK_COMPARISON_UNAVAILABLE":
		read = cfg.PublicOrigin == nil && p.Presence != nil && *p.Presence && p.Relation == "UNKNOWN"
	case "TOKEN_REJECTED":
		return validateUnreadWebhook(p)
	default:
		if providerFailure(p.WebhookCode) {
			return validateUnreadWebhook(p)
		}
		return ErrInvalid
	}
	if !read || p.PendingUpdates == nil || *p.PendingUpdates < 0 || *p.PendingUpdates > accountcatalog.MaxRevision || p.HasLastError == nil {
		return ErrInvalid
	}
	if !*p.HasLastError && p.LastErrorAt != nil {
		return ErrInvalid
	}
	if p.LastErrorAt != nil {
		_, offset := p.LastErrorAt.Zone()
		if offset != 0 || p.LastErrorAt.Year() < 1970 || p.LastErrorAt.Year() > 9999 {
			return ErrInvalid
		}
	}
	return nil
}

func validateUnreadWebhook(p ProbeResult) error {
	if p.Presence != nil || p.Relation != "UNKNOWN" || p.PendingUpdates != nil || p.HasLastError != nil || p.LastErrorAt != nil {
		return ErrInvalid
	}
	return nil
}

func assembleChecks(g Grant, cfg ConfigSnapshot, p ProbeResult) []Check {
	credentialCode, credentialStatus := "CREDENTIALS_CONFIGURED", "PASS"
	if !g.Credential.Configured {
		credentialCode, credentialStatus = "BOT_TOKEN_MISSING", "FAIL"
	} else if !g.WebhookSecretConfigured && g.ReceiveMode != "long_polling" {
		credentialCode, credentialStatus = "WEBHOOK_SECRET_MISSING", "FAIL"
	}
	identityStatus := "UNKNOWN"
	switch p.IdentityCode {
	case "BOT_IDENTITY_MATCH":
		identityStatus = "PASS"
	case "BOT_IDENTITY_MISMATCH", "TOKEN_REJECTED":
		identityStatus = "FAIL"
	case "NOT_EXECUTED":
		identityStatus = "SKIPPED"
	}
	originStatus := "FAIL"
	if cfg.OriginStatus == OriginValid {
		originStatus = "PASS"
	}
	webhookCode, webhookStatus, relation := p.WebhookCode, "UNKNOWN", p.Relation
	if p.IdentityCode != "BOT_IDENTITY_MATCH" {
		webhookCode, relation = "NOT_EXECUTED", "UNKNOWN"
	}
	switch webhookCode {
	case "WEBHOOK_MATCH":
		webhookStatus = "PASS"
	case "WEBHOOK_DIFFERENT", "WEBHOOK_NONE":
		webhookStatus = "WARN"
	case "TOKEN_REJECTED":
		webhookStatus = "FAIL"
	case "NOT_EXECUTED":
		webhookStatus = "SKIPPED"
	}
	pendingCode, pendingStatus := "NOT_EXECUTED", "SKIPPED"
	if p.PendingUpdates != nil {
		pendingCode, pendingStatus = "PENDING_UPDATES_ZERO", "PASS"
		if *p.PendingUpdates > 0 {
			pendingCode, pendingStatus = "PENDING_UPDATES_PRESENT", "WARN"
		}
	}
	deliveryCode, deliveryStatus := "NOT_EXECUTED", "SKIPPED"
	if p.HasLastError != nil {
		deliveryCode, deliveryStatus = "DELIVERY_ERROR_NOT_REPORTED", "PASS"
		if *p.HasLastError {
			deliveryCode, deliveryStatus = "DELIVERY_ERROR_REPORTED", "WARN"
		}
	}
	checks := []Check{
		check("credential_configuration", credentialStatus, credentialCode, struct {
			BotToken      bool `json:"bot_token_configured"`
			WebhookSecret bool `json:"webhook_secret_configured"`
		}{g.Credential.Configured, g.WebhookSecretConfigured}),
		check("bot_identity", identityStatus, p.IdentityCode, struct {
			Match *bool `json:"identity_match"`
		}{p.IdentityMatch}),
		check("public_origin", originStatus, cfg.OriginStatus, struct {
			Validation string `json:"validation"`
		}{"STATIC_ONLY"}),
		check("webhook_registration", webhookStatus, webhookCode, struct {
			Presence *bool  `json:"presence"`
			Relation string `json:"relation"`
		}{p.Presence, relation}),
		check("pending_updates", pendingStatus, pendingCode, struct {
			Count *int64 `json:"pending_update_count"`
		}{p.PendingUpdates}),
		check("delivery_errors", deliveryStatus, deliveryCode, struct {
			HasLastError *bool      `json:"has_last_error"`
			LastErrorAt  *time.Time `json:"last_error_at"`
		}{p.HasLastError, p.LastErrorAt}),
		check("recovery_materials", "UNKNOWN", "RECOVERY_MATERIALS_UNAVAILABLE", struct {
			Readable bool `json:"secret_token_readable"`
			Restore  bool `json:"restore_available"`
		}{false, false}),
		check("delivery_verification", "UNKNOWN", "DELIVERY_NOT_TESTED", struct {
			Verification string `json:"verification"`
		}{"NOT_TESTED"}),
	}
	if g.ReceiveMode == "long_polling" {
		for _, item := range []struct {
			index int
			code  string
		}{{2, "PUBLIC_ORIGIN_NOT_APPLICABLE"}, {5, "DELIVERY_ERRORS_NOT_APPLICABLE"}, {6, "RECOVERY_MATERIALS_NOT_APPLICABLE"}} {
			checks[item.index] = check(checks[item.index].ID, "NOT_APPLICABLE", item.code, struct {
				Applicability string `json:"applicability"`
			}{"NOT_APPLICABLE"})
		}
		if p.Presence != nil {
			status, code, relation := "PASS", "WEBHOOK_NONE", "NONE"
			if *p.Presence {
				status, code, relation = "FAIL", "WEBHOOK_BLOCKS_LONG_POLLING", "DIFFERENT"
			}
			checks[3] = check("webhook_registration", status, code, struct {
				Presence *bool  `json:"presence"`
				Relation string `json:"relation"`
			}{p.Presence, relation})
		}
	}
	return checks
}

func check(id, status, code string, details any) Check {
	// All details values above are closed, validated structs; no provider JSON or
	// arbitrary diagnostic text is copied into this output.
	body, _ := json.Marshal(details)
	return Check{ID: id, Status: status, Code: code, Details: body}
}
