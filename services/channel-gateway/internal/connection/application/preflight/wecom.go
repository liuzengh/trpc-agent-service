package preflight

import (
	"context"
	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"time"
	"unicode/utf8"
)

// WeComService never invokes Telegram APIs or normal runtime credential permits.
type WeComService struct {
	Control Control
	Probe   WeComProbe
}

func validWeComIdentity(id string) bool {
	if len(id) == 0 || len(id) > 1024 || !utf8.ValidString(id) {
		return false
	}
	for _, r := range id {
		if r <= 32 || r == 127 {
			return false
		}
	}
	return true
}
func (s WeComService) Execute(ctx context.Context, g Grant, cfg ConfigSnapshot) (Result, error) {
	if ctx == nil || g.Provider != "wecom" {
		return Result{}, ErrInvalid
	}
	if err := ValidateGrant(g, cfg); err != nil {
		return Result{}, err
	}
	if ctx.Err() != nil {
		return Result{}, ErrExpired
	}
	cfg = cloneConfig(cfg)
	code, status := "BOT_SECRET_MISSING", "FAIL"
	observed := ConnectionProbeResult{Code: "NOT_EXECUTED"}
	if g.Credential.Configured {
		code, status = "CREDENTIALS_CONFIGURED", "PASS"
		if s.Control == nil || s.Probe == nil {
			return Result{}, ErrInvalid
		}
		secret, err := s.Control.ResolveCredential(ctx, g)
		if err != nil {
			return Result{}, stableError(err)
		}
		if ctx.Err() != nil {
			return Result{}, ErrExpired
		}
		if secret.Reveal() == "" {
			return Result{}, ErrInvalid
		}
		observed, err = s.Probe.InspectConnection(ctx, secret, g.ProviderAccountID)
		if err != nil {
			return Result{}, stableError(err)
		}
		if ctx.Err() != nil {
			return Result{}, ErrExpired
		}
	}
	authStatus := "UNKNOWN"
	switch observed.Code {
	case "WECOM_AUTHENTICATED":
		authStatus = "PASS"
	case "WECOM_AUTH_REJECTED":
		authStatus = "FAIL"
	case "NOT_EXECUTED":
		authStatus = "SKIPPED"
	}
	checks := []Check{
		check("credential_configuration", status, code, struct {
			Configured bool `json:"bot_secret_configured"`
		}{g.Credential.Configured}),
		check("connection_authentication", authStatus, observed.Code, struct {
			Authenticated *bool `json:"authenticated"`
		}{observed.Authenticated}),
		check("delivery_verification", "UNKNOWN", "DELIVERY_NOT_TESTED", struct {
			Verification string `json:"verification"`
		}{"NOT_TESTED"}),
	}
	closed := make([]wire.PreflightCheck, len(checks))
	for i, c := range checks {
		closed[i] = wire.PreflightCheck{ID: c.ID, Status: c.Status, Code: c.Code, Details: c.Details}
	}
	if _, err := wire.ValidatePreflightChecksForMode(g.DiagnosticPolicy, g.ReceiveMode, closed); err != nil {
		return Result{}, ErrInvalid
	}
	return Result{Config: cfg, ObservedAt: time.Now().UTC(), Checks: checks}, nil
}
