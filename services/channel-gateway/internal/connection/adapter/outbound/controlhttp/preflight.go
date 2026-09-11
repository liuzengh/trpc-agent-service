package controlhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	p "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

const preflightResponseLimit = 20 << 10

// PreflightClient has a dedicated mTLS pool: diagnostics cannot occupy the
// normal catalog/registration transport. No runtime-account bypass is involved.
type PreflightClient struct{ client *Client }

func NewPreflight(o Options) (*PreflightClient, error) {
	cl, err := New(o)
	if err != nil {
		return nil, p.ErrInvalid
	}
	return &PreflightClient{client: cl}, nil
}
func (cl *PreflightClient) Close() { cl.client.Close() }

// Control owns diagnostic wire DTOs and schemas. Application grants remain
// independent of that transport contract and never marshal as runtime permits.
func (cl *PreflightClient) validateClaim(r p.ClaimRequest) error {
	if (r.DiagnosticPolicy != "" && r.DiagnosticPolicy != wire.PreflightReceiveModesPolicy && r.DiagnosticPolicy != "wecom_long_connection_v1") || cl == nil || cl.client == nil || p.ValidateConfig(r.Config) != nil || r.Config.ScopeID != cl.client.scope || r.Config.SourceEpoch != cl.client.epoch || !c.ValidEpoch(r.InstanceEpoch) || !c.ValidEpoch(r.RequestID) {
		return p.ErrInvalid
	}
	if (r.DiagnosticPolicy == "wecom_long_connection_v1") != (r.Config.Policy == "wecom_long_connection_v1") {
		return p.ErrInvalid
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(r.Token.Reveal())
	if err != nil || len(raw) != 32 || len(r.Token.Reveal()) != 43 {
		return p.ErrInvalid
	}
	clear(raw)
	return nil
}

func (cl *PreflightClient) Claim(ctx context.Context, r p.ClaimRequest) (*p.Grant, error) {
	if err := cl.validateClaim(r); err != nil {
		return nil, err
	}
	req := wire.PreflightClaimRequest{DiagnosticPolicy: r.DiagnosticPolicy, SchemaVersion: 1, ScopeID: r.Config.ScopeID, SourceEpoch: r.Config.SourceEpoch, InstanceEpoch: r.InstanceEpoch, ClaimRequestID: r.RequestID, ClaimToken: r.Token.Reveal(), GatewayConfigDigest: r.Config.Digest, ExpectedPublicOrigin: r.Config.PublicOrigin, OriginStatus: r.Config.OriginStatus, Limit: 1}
	raw, status, err := cl.exchange(ctx, "/internal/v1/channel-preflights:claim", req, 4<<10, "preflight-claim.schema.json")
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if status == http.StatusNoContent {
		return nil, nil
	}
	var w wire.PreflightGrant
	if err := wire.Decode("preflight-grant.schema.json", raw, &w); err != nil {
		return nil, preflightWireError(err)
	}
	g := p.Grant{EndpointProfile: w.EndpointProfile, AllowConnectionProbe: w.AllowConnectionProbe, ReceiveMode: w.ReceiveMode, DiagnosticPolicy: w.DiagnosticPolicy, EffectiveConfigDigest: w.EffectiveConfigDigest, PreflightID: w.PreflightID, ScopeID: w.ScopeID, SourceEpoch: w.SourceEpoch, TenantID: w.TenantID, AccountID: w.AccountID, Provider: w.Provider, ProviderAccountID: w.ProviderAccountID, WebhookPath: w.WebhookPath, AccountRevision: w.AccountRevision, ConnectionRevision: w.ConnectionRevision, LeaseEpoch: w.LeaseEpoch, Credential: p.Credential{Purpose: w.Credentials.Purpose, ID: w.Credentials.CredentialID, Version: w.Credentials.CredentialVersion, Configured: w.Credentials.Configured}, WebhookSecretConfigured: w.WebhookSecretConfigured, ServerTime: w.ServerTime, LeaseExpiresAt: w.LeaseExpiresAt, JobDeadlineAt: w.JobDeadlineAt, ConfigDigest: w.GatewayConfigDigest, Request: r}
	if err := p.ValidateGrant(g, r.Config); err != nil {
		return nil, err
	}
	return &g, nil
}

func (cl *PreflightClient) validateGrant(g p.Grant) error {
	if err := cl.validateClaim(g.Request); err != nil {
		return err
	}
	return p.ValidateGrant(g, g.Request.Config)
}
func leaseRequest(g p.Grant) wire.PreflightResolveRequest {
	return wire.PreflightResolveRequest{SchemaVersion: 1, ScopeID: g.ScopeID, SourceEpoch: g.SourceEpoch, InstanceEpoch: g.Request.InstanceEpoch, LeaseEpoch: g.LeaseEpoch, ClaimToken: g.Request.Token.Reveal()}
}
func (cl *PreflightClient) ResolveCredential(ctx context.Context, g p.Grant) (p.Secret, error) {
	if err := cl.validateGrant(g); err != nil {
		return p.Secret{}, err
	}
	if !g.Credential.Configured {
		return p.Secret{}, p.ErrInvalid
	}
	raw, status, err := cl.exchange(ctx, "/internal/v1/channel-preflights/"+g.PreflightID+"/credentials:resolve", leaseRequest(g), 4<<10, "preflight-resolve.schema.json")
	if err != nil {
		return p.Secret{}, err
	}
	defer clear(raw)
	if status != http.StatusOK {
		return p.Secret{}, p.ErrInvalid
	}
	var w wire.PreflightResolveResponse
	if err := wire.Decode("preflight-resolved.schema.json", raw, &w); err != nil {
		return p.Secret{}, preflightWireError(err)
	}
	if w.PreflightID != g.PreflightID || w.ConnectionRevision != g.ConnectionRevision || w.Purpose != g.Credential.Purpose || w.CredentialID != g.Credential.ID || w.CredentialVersion != g.Credential.Version || !w.LeaseExpiresAt.Equal(g.LeaseExpiresAt) || w.Value == "" || len(w.Value) > 16<<10 {
		return p.Secret{}, p.ErrInvalid
	}
	if ctx.Err() != nil {
		return p.Secret{}, p.ErrExpired
	}
	return p.NewSecret(w.Value), nil
}
func (cl *PreflightClient) Complete(ctx context.Context, g p.Grant, r p.Result) error {
	if err := cl.validateGrant(g); err != nil {
		return err
	}
	if err := validatePreflightResult(g, r); err != nil {
		return err
	}
	req := wire.PreflightCompleteRequest{SchemaVersion: 1, ScopeID: g.ScopeID, SourceEpoch: g.SourceEpoch, InstanceEpoch: g.Request.InstanceEpoch, LeaseEpoch: g.LeaseEpoch, ClaimToken: g.Request.Token.Reveal(), GatewayConfigDigest: r.Config.Digest, ExpectedPublicOrigin: r.Config.PublicOrigin, ObservedAt: r.ObservedAt, Checks: preflightWireChecks(r.Checks)}
	if g.DiagnosticPolicy != "" {
		req.ReceiveMode = g.ReceiveMode
		req.EndpointProfile = g.EndpointProfile
		req.DiagnosticPolicy = g.DiagnosticPolicy
		req.EffectiveConfigDigest = g.EffectiveConfigDigest
		req.ConnectionRevision = g.ConnectionRevision
		req.OriginStatus = r.Config.OriginStatus
	}
	raw, status, err := cl.exchange(ctx, "/internal/v1/channel-preflights/"+g.PreflightID+":complete", req, 16<<10, "preflight-complete.schema.json")
	clear(raw)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return p.ErrInvalid
	}
	return nil
}

// exchange never returns an upstream body, URL, credential, or TLS detail as an
// error. It does not retry: the runner owns the original claim/result replay.
func (cl *PreflightClient) exchange(ctx context.Context, path string, payload any, requestLimit int, requestSchema string) ([]byte, int, error) {
	if ctx == nil {
		return nil, 0, p.ErrInvalid
	}
	if ctx.Err() != nil {
		return nil, 0, p.ErrExpired
	}
	body, err := json.Marshal(payload)
	if err != nil || len(body) > requestLimit {
		return nil, 0, p.ErrInvalid
	}
	defer clear(body)
	if err := wire.Validate(requestSchema, body); err != nil {
		return nil, 0, preflightWireError(err)
	}
	// An HTTP-attempt deadline is not the diagnostic task/lease deadline.
	// Preserve the caller so an uncertain local timeout remains retryable.
	parentCtx := ctx
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()
	u := *cl.client.base
	u.Path = path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, 0, p.ErrInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Cache-Control", "no-store")
	res, err := cl.client.http.Do(req)
	if err != nil {
		if parentCtx.Err() != nil {
			return nil, 0, p.ErrExpired
		}
		return nil, 0, p.ErrUnavailable
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent {
		switch res.StatusCode {
		case 400, 422:
			return nil, 0, p.ErrInvalid
		case 401, 403, 404:
			return nil, 0, p.ErrDenied
		case 409:
			// Inspect only this closed code. The message/body is never surfaced.
			raw, _ := io.ReadAll(io.LimitReader(res.Body, 4097))
			defer clear(raw)
			var e struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if len(raw) <= 4096 && json.Unmarshal(raw, &e) == nil && e.Error.Code == "CHANNEL_PREFLIGHT_LEASE_EXPIRED" {
				return nil, 0, p.ErrExpired
			}
			return nil, 0, p.ErrConflict
		default:
			return nil, 0, p.ErrUnavailable
		}
	}
	if encoding := res.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, 0, p.ErrInvalid
	}
	if !headerNoStore(res.Header.Values("Cache-Control")) {
		return nil, 0, p.ErrInvalid
	}
	if res.StatusCode == http.StatusOK {
		media, _, e := mime.ParseMediaType(res.Header.Get("Content-Type"))
		if e != nil || media != "application/json" {
			return nil, 0, p.ErrInvalid
		}
	}
	if res.ContentLength > preflightResponseLimit {
		return nil, 0, p.ErrInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, preflightResponseLimit+1))
	if err != nil {
		clear(raw)
		if parentCtx.Err() != nil {
			return nil, 0, p.ErrExpired
		}
		return nil, 0, p.ErrUnavailable
	}
	if len(raw) > preflightResponseLimit || res.StatusCode == http.StatusNoContent && len(raw) != 0 {
		clear(raw)
		return nil, 0, p.ErrInvalid
	}
	if ctx.Err() != nil {
		clear(raw)
		if parentCtx.Err() != nil {
			return nil, 0, p.ErrExpired
		}
		return nil, 0, p.ErrUnavailable
	}
	return raw, res.StatusCode, nil
}
func headerNoStore(values []string) bool {
	for _, v := range values {
		for _, directive := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(directive), "no-store") {
				return true
			}
		}
	}
	return false
}

// Only the stable application errors cross this adapter; schema diagnostics
// must not leak rejected credential-bearing values or implementation details.
func preflightWireError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, wire.ErrInvalidDocument) {
		return p.ErrInvalid
	}
	return p.ErrUnavailable
}

func preflightWireChecks(checks []p.Check) []wire.PreflightCheck {
	result := make([]wire.PreflightCheck, len(checks))
	for i, check := range checks {
		result[i] = wire.PreflightCheck{ID: check.ID, Status: check.Status, Code: check.Code, Details: check.Details}
	}
	return result
}

var _ p.Control = (*PreflightClient)(nil)

// Shared validation owns the eight closed facts and their cross-check semantics.
// The adapter retains only the local grant/config/time and metadata fences.
func validatePreflightResult(g p.Grant, r p.Result) error {
	_, observedOffset := r.ObservedAt.Zone()
	if p.ValidateConfig(r.Config) != nil || r.Config.Digest != g.ConfigDigest || r.Config.ScopeID != g.ScopeID || r.Config.SourceEpoch != g.SourceEpoch || !sameOrigin(r.Config.PublicOrigin, g.Request.Config.PublicOrigin) || r.Config.OriginStatus != g.Request.Config.OriginStatus || r.ObservedAt.IsZero() || observedOffset != 0 {
		return p.ErrInvalid
	}
	checks := preflightWireChecks(r.Checks)
	if _, err := wire.ValidatePreflightChecksForMode(g.DiagnosticPolicy, g.ReceiveMode, checks); err != nil {
		return preflightWireError(err)
	}
	// This is a projection of already schema-validated details, not another wire
	// definition. Compare to the exact trusted credential metadata of this claim.
	if g.Provider == "wecom" {
		var d struct {
			Configured bool `json:"bot_secret_configured"`
		}
		if json.Unmarshal(checks[0].Details, &d) != nil || d.Configured != g.Credential.Configured {
			return p.ErrInvalid
		}
		return nil
	}
	var credentials struct {
		Bot     bool `json:"bot_token_configured"`
		Webhook bool `json:"webhook_secret_configured"`
	}
	if json.Unmarshal(checks[0].Details, &credentials) != nil || credentials.Bot != g.Credential.Configured || credentials.Webhook != g.WebhookSecretConfigured || (g.ReceiveMode != "long_polling" && checks[2].Code != r.Config.OriginStatus) {
		return p.ErrInvalid
	}
	return nil
}
func sameOrigin(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
