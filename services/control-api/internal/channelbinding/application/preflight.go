package application

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/gowebpki/jcs"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

const (
	preflightLifetime   = 120 * time.Second
	preflightLease      = 30 * time.Second
	preflightRetention  = 24 * time.Hour
	preflightReceiptTTL = 5 * time.Minute
)

// PreflightService is a diagnostic-only state machine. It never writes an
// Account, normal runtime permit, route, observation, admission or Outbox.
type PreflightService struct{ deps PreflightDependencies }

func NewPreflightService(deps PreflightDependencies) (*PreflightService, error) {
	if deps.Store == nil || deps.Access == nil || deps.Accounts == nil || deps.Cipher == nil || deps.NewID == nil || !domain.ValidID(deps.ScopeID) || !domain.ValidEpoch(deps.SourceEpoch) {
		return nil, ErrDependencyUnavailable
	}
	return &PreflightService{deps}, nil
}
func preflightError(code string) error { return &domain.Error{Code: "CHANNEL_PREFLIGHT_" + code} }
func preflightHash(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func preflightDigest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", invalid("")
	}
	defer clear(raw)
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return "", invalid("")
	}
	defer clear(canonical)
	return preflightHash(canonical), nil
}
func preflightWire(schema string, value any) error {
	raw, err := json.Marshal(value)
	defer clear(raw)
	if err != nil || channelv1.Validate(schema, raw) != nil {
		return invalid("")
	}
	return nil
}
func (s *PreflightService) publicAccount(ctx context.Context, actor Actor, account string, owner bool) error {
	if !domain.ValidID(actor.TenantID) || !domain.ValidID(actor.UserID) {
		return ErrPermissionDenied
	}
	ok, err := s.deps.Access.IsActiveMember(ctx, actor.TenantID, actor.UserID)
	if err != nil {
		return ErrDependencyUnavailable
	}
	if !ok {
		// Preflight hides accounts from nonmembers. A visible member who
		// lacks OWNER, or loses their Session at commit, still receives 403.
		return ErrAccountNotFound
	}
	if !domain.ValidID(account) {
		return invalid("/account_id")
	}
	if _, err = s.deps.Accounts.GetAccount(ctx, actor.TenantID, account); err != nil {
		return err
	}
	if owner {
		ok, err = s.deps.Access.IsActiveOwner(ctx, actor.TenantID, actor.UserID)
		if err != nil {
			return ErrDependencyUnavailable
		}
		if !ok {
			return ErrPermissionDenied
		}
	}
	return nil
}
func (s *PreflightService) authorize(p WorkloadPrincipal, scope, epoch string, consumer ...string) error {
	if p.PrincipalID == "" || p.Audience != WorkloadAudience || p.ScopeID != s.deps.ScopeID || scope != p.ScopeID || !domain.ValidID(p.InstanceID) || (!slices.Contains(p.Consumers, "telegram_preflight") && !slices.Contains(p.Consumers, "wecom_preflight")) {
		return ErrWorkloadDenied
	}
	if len(consumer) == 1 && !slices.Contains(p.Consumers, consumer[0]) {
		return ErrWorkloadDenied
	}
	if epoch != s.deps.SourceEpoch {
		return ErrEpochMismatch
	}
	return nil
}
func preflightToken(a PreflightAccount) domain.CredentialRecord {
	for _, c := range a.Credentials {
		if c.Meta.Purpose == preflightPurpose(a.Account.Provider) {
			return c
		}
	}
	return domain.CredentialRecord{}
}
func preflightWebhookConfigured(a PreflightAccount) bool {
	for _, c := range a.Credentials {
		if c.Meta.Purpose == domain.TelegramWebhookSecret {
			return c.Meta.Configured
		}
	}
	return false
}
func preflightActive(r PreflightRecord) bool {
	return r.View.State == "QUEUED" || r.View.State == "RUNNING"
}
func preflightStale(r *PreflightRecord, reason string) {
	r.View.State = "STALE"
	r.View.Outcome = "UNKNOWN"
	r.View.ReasonCode = reason
	r.View.Freshness = "STALE"
}
func preflightMismatch(r PreflightRecord, a PreflightAccount, epoch string, checkRequester bool) string {
	switch {
	case r.SourceEpoch != epoch:
		return "CHANNEL_SOURCE_EPOCH_MISMATCH"
	case !a.TenantActive:
		return "CHANNEL_PREFLIGHT_TENANT_INACTIVE"
	case checkRequester && !a.RequesterOwner:
		return "CHANNEL_PREFLIGHT_REQUESTER_REVOKED"
	case r.View.DiagnosticPolicy != "" && r.View.ReceiveMode != preflightMode(a.Account):
		return "CHANNEL_PREFLIGHT_ACCOUNT_CHANGED"
	case a.Account.Enabled || string(a.Account.Provider) != r.View.Provider || a.Account.ProviderAccountID != r.View.ProviderAccountID || a.Account.ConnectionRevision != r.View.ConnectionRevision:
		return "CHANNEL_PREFLIGHT_ACCOUNT_CHANGED"
	}
	token := preflightToken(a)
	if token.Meta.ID != r.CredentialID || token.Meta.Version != r.View.CredentialVersion() || token.Meta.Configured != r.CredentialConfigured() || preflightWebhookConfigured(a) != r.WebhookSecretConfigured {
		return "CHANNEL_PREFLIGHT_ACCOUNT_CHANGED"
	}
	return ""
}

// Database time, not client observed_at or local wall time, determines authority.
func preflightReconcile(r *PreflightRecord, a PreflightAccount, epoch string, now time.Time, checkRequester bool) {
	if !preflightActive(*r) {
		return
	}
	r.View.MetadataChanged = a.Account.Revision != r.View.AccountRevision
	if reason := preflightMismatch(*r, a, epoch, checkRequester); reason != "" {
		preflightStale(r, reason)
		return
	}
	if !now.Before(r.View.JobDeadlineAt) || (r.LeaseEpoch >= 2 && r.LeaseExpiresAt != nil && !now.Before(*r.LeaseExpiresAt)) {
		r.View.State = "TIMED_OUT"
		r.View.Freshness = "EXPIRED"
		r.View.ReasonCode = "CHANNEL_PREFLIGHT_NO_EXECUTOR"
		if r.View.StartedAt != nil {
			r.View.ReasonCode = "CHANNEL_PREFLIGHT_EXECUTION_TIMEOUT"
		}
	}
}
func preflightView(r PreflightRecord, a PreflightAccount, epoch string, now time.Time) channelv1.PreflightView {
	view := r.View
	view.MetadataChanged = a.Account.Revision != view.AccountRevision
	if view.State == "COMPLETED" {
		view.Freshness = "CURRENT"
		if preflightMismatch(r, a, epoch, false) != "" {
			view.Freshness = "STALE"
		} else if view.ExpiresAt == nil || !now.Before(*view.ExpiresAt) {
			view.Freshness = "EXPIRED"
		}
	}
	return view
}
func preflightUsable(r PreflightRecord, now time.Time) error {
	if r.View.State == "STALE" {
		return preflightError("STALE")
	}
	if r.View.State != "RUNNING" || r.LeaseExpiresAt == nil || !now.Before(*r.LeaseExpiresAt) || !now.Before(r.View.JobDeadlineAt) {
		return preflightError("LEASE_EXPIRED")
	}
	return nil
}
func preflightOwner(r PreflightRecord, p WorkloadPrincipal, instanceEpoch string, lease int64, token string) error {
	if !slices.Contains(p.Consumers, preflightConsumer(r.View.Provider)) {
		return ErrWorkloadDenied
	}
	hash := preflightHash([]byte(token))
	if r.PrincipalID != p.PrincipalID || r.InstanceID != p.InstanceID || r.InstanceEpoch != instanceEpoch || r.LeaseEpoch != lease || subtle.ConstantTimeCompare([]byte(r.ClaimTokenHash), []byte(hash)) != 1 {
		return preflightError("CLAIM_CONFLICT")
	}
	return nil
}
func preflightCreated(r PreflightRecord) channelv1.PreflightCreated {
	v := r.View
	return channelv1.PreflightCreated{PreflightID: v.PreflightID, TenantID: v.TenantID, AccountID: v.AccountID, RequestedAt: v.RequestedAt, JobDeadlineAt: v.JobDeadlineAt, StatusURL: "/v1/tenants/" + v.TenantID + "/channel-accounts/" + v.AccountID + "/preflights/" + v.PreflightID}
}
func (s *PreflightService) Create(ctx context.Context, actor Actor, account, key string, input channelv1.PreflightCreateRequest) (channelv1.PreflightCreated, error) {
	for attempt := 0; attempt < 3; attempt++ {
		result, err := s.createOnce(ctx, actor, account, key, input)
		if !errors.Is(err, errPreparationChanged) {
			return result, err
		}
	}
	return channelv1.PreflightCreated{}, ErrDependencyUnavailable
}
func (s *PreflightService) createOnce(ctx context.Context, actor Actor, account, key string, input channelv1.PreflightCreateRequest) (channelv1.PreflightCreated, error) {
	var response channelv1.PreflightCreated
	if err := s.publicAccount(ctx, actor, account, true); err != nil {
		return response, err
	}
	if !validKey(key) {
		return response, invalid("/Idempotency-Key")
	}
	if err := preflightWire("preflight-create.schema.json", input); err != nil {
		return response, err
	}
	// Discover the old requester without locks. The transaction locks all
	// discovered Identity rows in sorted order before Tenant/Account, then
	// rechecks the active task. A changed requester restarts this bounded read.
	old, found, err := s.deps.Store.ActiveAccount(ctx, s.deps.ScopeID, actor.TenantID, account)
	if err != nil {
		return response, err
	}
	var additionalRequesters []string
	if found && old.RequestedBy != actor.UserID {
		additionalRequesters = append(additionalRequesters, old.RequestedBy)
	}
	receiptKey, _ := preflightDigest([]string{s.deps.ScopeID, actor.TenantID, account, key})
	digest, _ := preflightDigest(struct {
		Actor Actor
		Input channelv1.PreflightCreateRequest
	}{actor, input})
	var operationErr error
	err = s.deps.Store.WithTransaction(ctx, "tenant:"+actor.TenantID, func(tx PreflightTransaction) error {
		a, err := tx.LockAccount(ctx, actor.TenantID, account, actor.UserID, true, additionalRequesters...)
		if err != nil {
			return err
		}
		if !a.SessionOwner || !a.RequesterOwner || !a.TenantActive {
			return ErrPermissionDenied
		}
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		receipt, found, err := tx.FindRequest(ctx, "create", receiptKey)
		if err != nil {
			return err
		}
		if found && now.Before(receipt.ExpiresAt) {
			if receipt.Digest != digest {
				return ErrIdempotencyConflict
			}
			if json.Unmarshal(receipt.Response, &response) != nil {
				return ErrDependencyUnavailable
			}
			return nil
		}
		if tx.SourceEpoch() != s.deps.SourceEpoch {
			return ErrEpochMismatch
		}
		if a.Account.Provider != domain.Telegram && a.Account.Provider != domain.WeCom {
			return preflightError("PROVIDER_UNSUPPORTED")
		}
		if a.Account.Enabled {
			return &domain.Error{Code: domain.AccountMustBeDisabled}
		}
		if a.Account.Revision != input.ExpectedAccountRevision || a.Account.ConnectionRevision != input.ExpectedConnectionRevision {
			return &domain.Error{Code: domain.RevisionConflict}
		}
		token := preflightToken(a)
		expectedVersion := input.ExpectedBotTokenVersion
		if a.Account.Provider == domain.WeCom {
			if input.ExpectedBotTokenVersion != 0 || input.ExpectedBotSecretVersion == 0 {
				return invalid("/expected_bot_secret_version")
			}
			expectedVersion = input.ExpectedBotSecretVersion
		} else if input.ExpectedBotSecretVersion != 0 || input.AllowConnectionProbe {
			return invalid("/allow_connection_probe")
		}
		if token.Meta.Version != expectedVersion {
			return &domain.Error{Code: domain.CredentialVersionConflict}
		}
		if a.Account.Provider == domain.WeCom && !input.AllowConnectionProbe {
			return preflightError("CONNECTION_PROBE_CONFIRMATION_REQUIRED")
		}
		active, err := tx.Active(ctx, actor.TenantID, account)
		if err != nil {
			return err
		}
		running := false
		for _, old := range active {
			oldAccount := a
			if old.View.RequestedBy != actor.UserID {
				allowed, known := a.RequesterOwners[old.View.RequestedBy]
				if !known {
					return errPreparationChanged
				}
				oldAccount.RequesterOwner = allowed
			}
			preflightReconcile(&old, oldAccount, tx.SourceEpoch(), now, true)
			if err = tx.Save(ctx, old); err != nil {
				return err
			}
			running = running || preflightActive(old)
		}
		if running {
			operationErr = preflightError("ALREADY_RUNNING")
			return nil
		}
		ac, tc, ta, err := tx.Counts(ctx, actor.TenantID, account, now.Add(-time.Minute))
		if err != nil {
			return err
		}
		if ac >= 3 || tc >= 30 || ta >= 20 {
			operationErr = preflightError("RATE_LIMITED")
			return nil
		}
		id, err := s.deps.NewID("cpf")
		if err != nil {
			return ErrDependencyUnavailable
		}
		r := PreflightRecord{ScopeID: s.deps.ScopeID, SourceEpoch: tx.SourceEpoch(), CredentialID: token.Meta.ID, BotTokenConfigured: token.Meta.Configured, WebhookSecretConfigured: preflightWebhookConfigured(a), WebhookPath: a.Account.Config.WebhookPath, LastCheckedAt: now, View: channelv1.PreflightView{EndpointProfile: a.Account.Config.EndpointProfile, ReceiveMode: preflightMode(a.Account), DiagnosticPolicy: preflightPolicy(a.Account.Provider), PreflightID: id, TenantID: actor.TenantID, AccountID: account, Provider: string(a.Account.Provider), ProviderAccountID: a.Account.ProviderAccountID, RequestedBy: actor.UserID, AccountRevision: a.Account.Revision, ConnectionRevision: a.Account.ConnectionRevision, BotTokenVersion: token.Meta.Version, State: "QUEUED", Outcome: "UNKNOWN", ReasonCode: "CHANNEL_PREFLIGHT_QUEUED", Freshness: "NOT_CHECKED", RequestedAt: now, JobDeadlineAt: now.Add(preflightLifetime), Checks: []channelv1.PreflightCheck{}}}
		if a.Account.Provider == domain.WeCom {
			r.View.BotSecretVersion, r.View.BotTokenVersion = token.Meta.Version, 0
			r.View.AllowConnectionProbe = true
			r.BotSecretConfigured, r.BotTokenConfigured = token.Meta.Configured, false
		}
		if err = tx.Save(ctx, r); err != nil {
			return err
		}
		response = preflightCreated(r)
		raw, err := json.Marshal(response)
		if err != nil {
			return err
		}
		return tx.SaveRequest(ctx, PreflightRequest{Kind: "create", Key: receiptKey, Digest: digest, TenantID: actor.TenantID, AccountID: account, RequestedBy: actor.UserID, PreflightID: id, CreatedAt: now, ExpiresAt: now.Add(preflightRetention), Response: raw})
	})
	if err != nil {
		return channelv1.PreflightCreated{}, err
	}
	if operationErr != nil {
		return channelv1.PreflightCreated{}, operationErr
	}
	return response, nil
}
func (s *PreflightService) lookup(ctx context.Context, id string) (PreflightKey, error) {
	if !domain.ValidID(id) {
		return PreflightKey{}, invalid("/preflight_id")
	}
	k, ok, err := s.deps.Store.Lookup(ctx, s.deps.ScopeID, id)
	if err != nil {
		return k, err
	}
	if !ok {
		return k, preflightError("NOT_FOUND")
	}
	return k, nil
}
func (s *PreflightService) Get(ctx context.Context, actor Actor, account, id string) (channelv1.PreflightView, error) {
	var response channelv1.PreflightView
	if err := s.publicAccount(ctx, actor, account, false); err != nil {
		return response, err
	}
	k, err := s.lookup(ctx, id)
	if err != nil {
		return response, err
	}
	if k.TenantID != actor.TenantID || k.AccountID != account {
		return response, preflightError("NOT_FOUND")
	}
	err = s.deps.Store.WithTransaction(ctx, "task:"+id, func(tx PreflightTransaction) error {
		a, err := tx.LockAccount(ctx, k.TenantID, k.AccountID, k.RequestedBy, false)
		if err != nil {
			return err
		}
		r, found, err := tx.Load(ctx, id)
		if err != nil {
			return err
		}
		if !found {
			return preflightError("NOT_FOUND")
		}
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		active := preflightActive(r)
		preflightReconcile(&r, a, tx.SourceEpoch(), now, true)
		if active {
			r.LastCheckedAt = now
			if err = tx.Save(ctx, r); err != nil {
				return err
			}
		}
		response = preflightView(r, a, tx.SourceEpoch(), now)
		return nil
	})
	return response, err
}

func preflightGrant(r PreflightRecord, now time.Time) channelv1.PreflightGrant {
	v := r.View
	return channelv1.PreflightGrant{EndpointProfile: v.EndpointProfile, AllowConnectionProbe: v.AllowConnectionProbe, ReceiveMode: v.ReceiveMode, DiagnosticPolicy: v.DiagnosticPolicy, EffectiveConfigDigest: v.EffectiveConfigDigest, SchemaVersion: 1, ServerTime: now, PreflightID: v.PreflightID, ScopeID: r.ScopeID, SourceEpoch: r.SourceEpoch, TenantID: v.TenantID, AccountID: v.AccountID, Provider: v.Provider, ProviderAccountID: v.ProviderAccountID, AccountRevision: v.AccountRevision, ConnectionRevision: v.ConnectionRevision, WebhookPath: r.WebhookPath, Credentials: channelv1.PreflightCredential{Purpose: preflightPurpose(domain.Provider(v.Provider)), CredentialID: r.CredentialID, CredentialVersion: v.CredentialVersion(), Configured: r.CredentialConfigured()}, WebhookSecretConfigured: r.WebhookSecretConfigured, LeaseEpoch: r.LeaseEpoch, LeaseExpiresAt: *r.LeaseExpiresAt, JobDeadlineAt: v.JobDeadlineAt, GatewayConfigDigest: *v.GatewayConfigDigest}
}
func (s *PreflightService) Claim(ctx context.Context, p WorkloadPrincipal, input channelv1.PreflightClaimRequest) (*channelv1.PreflightGrant, error) {
	if err := s.authorize(p, input.ScopeID, input.SourceEpoch, preflightClaimConsumer(input.DiagnosticPolicy)); err != nil {
		return nil, err
	}
	if err := preflightWire("preflight-claim.schema.json", input); err != nil {
		return nil, err
	}
	digest, err := preflightDigest(input)
	if err != nil {
		return nil, err
	}
	key, _ := preflightDigest([]string{input.ScopeID, p.PrincipalID, p.InstanceID, input.InstanceEpoch, input.ClaimRequestID})
	candidate, hasCandidate, err := s.deps.Store.Candidate(ctx, input.ScopeID, input.DiagnosticPolicy)
	if err != nil {
		return nil, err
	}
	var response *channelv1.PreflightGrant
	var operationErr error
	err = s.deps.Store.WithTransaction(ctx, "instance:"+p.PrincipalID+":"+p.InstanceID, func(tx PreflightTransaction) error {
		if tx.SourceEpoch() != input.SourceEpoch {
			return ErrEpochMismatch
		}
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		receipt, found, err := tx.FindRequest(ctx, "claim", key)
		if err != nil {
			return err
		}
		if found && now.Before(receipt.ExpiresAt) {
			if receipt.Digest != digest {
				return preflightError("CLAIM_CONFLICT")
			}
			if len(receipt.Response) == 0 || string(receipt.Response) == "null" {
				return nil
			}
			var original channelv1.PreflightGrant
			if json.Unmarshal(receipt.Response, &original) != nil {
				return ErrDependencyUnavailable
			}
			a, err := tx.LockAccount(ctx, receipt.TenantID, receipt.AccountID, receipt.RequestedBy, false)
			if err != nil {
				return err
			}
			r, ok, err := tx.Load(ctx, receipt.PreflightID)
			if err != nil {
				return err
			}
			if !ok {
				return preflightError("LEASE_EXPIRED")
			}
			now, err = tx.Now(ctx)
			if err != nil {
				return err
			}
			if err = preflightOwner(r, p, input.InstanceEpoch, original.LeaseEpoch, input.ClaimToken); err != nil {
				return err
			}
			if r.View.State != "COMPLETED" {
				preflightReconcile(&r, a, tx.SourceEpoch(), now, true)
				if err = tx.Save(ctx, r); err != nil {
					return err
				}
				if operationErr = preflightUsable(r, now); operationErr != nil {
					return nil
				}
			}
			original.ServerTime = now
			response = &original
			return nil
		}
		count, err := tx.ClaimCount(ctx, p.PrincipalID, p.InstanceID, now.Add(-time.Second))
		if err != nil {
			return err
		}
		if count >= 2 {
			return preflightError("RATE_LIMITED")
		}
		receipt = PreflightRequest{Kind: "claim", Key: key, Digest: digest, PrincipalID: p.PrincipalID, InstanceID: p.InstanceID, InstanceEpoch: input.InstanceEpoch, CreatedAt: now, ExpiresAt: now.Add(preflightReceiptTTL)}
		saveEmpty := func() error {
			receipt.CreatedAt, receipt.ExpiresAt = now, now.Add(preflightReceiptTTL)
			return tx.SaveRequest(ctx, receipt)
		}
		if !hasCandidate {
			return saveEmpty()
		}
		a, err := tx.LockAccount(ctx, candidate.TenantID, candidate.AccountID, candidate.RequestedBy, false)
		if err != nil {
			return err
		}
		r, ok, err := tx.Load(ctx, candidate.ID)
		if err != nil {
			return err
		}
		if !ok || r.View.DiagnosticPolicy != input.DiagnosticPolicy || !slices.Contains(p.Consumers, preflightConsumer(r.View.Provider)) {
			return saveEmpty()
		}
		now, err = tx.Now(ctx)
		if err != nil {
			return err
		}
		preflightReconcile(&r, a, tx.SourceEpoch(), now, true)
		if !preflightActive(r) {
			if err = tx.Save(ctx, r); err != nil {
				return err
			}
			return saveEmpty()
		}
		if r.View.State == "RUNNING" && r.LeaseExpiresAt != nil && now.Before(*r.LeaseExpiresAt) {
			return saveEmpty()
		}
		effective := ""
		configChanged := r.View.GatewayConfigDigest != nil && *r.View.GatewayConfigDigest != input.GatewayConfigDigest
		if r.View.DiagnosticPolicy != "" {
			effective, err = channelv1.PreflightEffectiveConfigDigest(input.ScopeID, input.SourceEpoch, r.View.ReceiveMode, r.View.ConnectionRevision, input.ExpectedPublicOrigin, input.OriginStatus, r.View.EndpointProfile)
			if err != nil {
				return invalid("/gateway_config_digest")
			}
			configChanged = r.View.EffectiveConfigDigest != "" && r.View.EffectiveConfigDigest != effective
		}
		if configChanged {
			preflightStale(&r, "CHANNEL_PREFLIGHT_GATEWAY_CONFIG_CHANGED")
			if err = tx.Save(ctx, r); err != nil {
				return err
			}
			operationErr = preflightError("STALE")
			return nil
		}
		if r.LeaseEpoch >= 2 {
			return preflightError("LEASE_EXPIRED")
		}
		if r.View.StartedAt == nil {
			r.View.StartedAt = &now
			unconfirmed := "UNCONFIRMED"
			r.View.GatewayConfigFreshness = &unconfirmed
		}
		// Each lease freezes its global instance evidence. An expired polling
		// lease may refresh irrelevant inbound origin only when effective
		// configuration remains identical; old claim receipts stay unchanged.
		r.View.GatewayConfigDigest = &input.GatewayConfigDigest
		r.View.ExpectedPublicOrigin = input.ExpectedPublicOrigin
		r.OriginStatus = input.OriginStatus
		if r.View.DiagnosticPolicy != "" {
			r.View.EffectiveConfigDigest = effective
			r.GatewayPublicOrigin = input.ExpectedPublicOrigin
			if r.View.ReceiveMode == domain.LongPolling {
				r.View.ExpectedPublicOrigin = nil
			}
		}
		r.LeaseEpoch++
		expiry := now.Add(preflightLease)
		if expiry.After(r.View.JobDeadlineAt) {
			expiry = r.View.JobDeadlineAt
		}
		r.LeaseExpiresAt = &expiry
		r.PrincipalID = p.PrincipalID
		r.InstanceID = p.InstanceID
		r.InstanceEpoch = input.InstanceEpoch
		r.ClaimTokenHash = preflightHash([]byte(input.ClaimToken))
		r.View.State = "RUNNING"
		r.View.ReasonCode = "CHANNEL_PREFLIGHT_RUNNING"
		r.LastCheckedAt = now
		if err = tx.Save(ctx, r); err != nil {
			return err
		}
		grant := preflightGrant(r, now)
		response = &grant
		receipt.TenantID = r.View.TenantID
		receipt.AccountID = r.View.AccountID
		receipt.RequestedBy = r.View.RequestedBy
		receipt.PreflightID = r.View.PreflightID
		receipt.CreatedAt, receipt.ExpiresAt = now, now.Add(preflightReceiptTTL)
		receipt.Response, err = json.Marshal(grant)
		if err != nil {
			return err
		}
		return tx.SaveRequest(ctx, receipt)
	})
	if err != nil {
		return nil, err
	}
	if operationErr != nil {
		return nil, operationErr
	}
	return response, nil
}
func (s *PreflightService) Resolve(ctx context.Context, p WorkloadPrincipal, id string, input channelv1.PreflightResolveRequest) (channelv1.PreflightResolveResponse, error) {
	var response channelv1.PreflightResolveResponse
	if err := s.authorize(p, input.ScopeID, input.SourceEpoch); err != nil {
		return response, err
	}
	if err := preflightWire("preflight-resolve.schema.json", input); err != nil {
		return response, err
	}
	k, err := s.lookup(ctx, id)
	if err != nil {
		return response, err
	}
	var operationErr error
	err = s.deps.Store.WithTransaction(ctx, "task:"+id, func(tx PreflightTransaction) error {
		if tx.SourceEpoch() != input.SourceEpoch {
			return ErrEpochMismatch
		}
		a, err := tx.LockAccount(ctx, k.TenantID, k.AccountID, k.RequestedBy, false)
		if err != nil {
			return err
		}
		r, ok, err := tx.Load(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return preflightError("NOT_FOUND")
		}
		if err = preflightOwner(r, p, input.InstanceEpoch, input.LeaseEpoch, input.ClaimToken); err != nil {
			return err
		}
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		preflightReconcile(&r, a, tx.SourceEpoch(), now, true)
		if err = tx.Save(ctx, r); err != nil {
			return err
		}
		if operationErr = preflightUsable(r, now); operationErr != nil {
			return nil
		}
		c := preflightToken(a)
		if !r.CredentialConfigured() || !c.Meta.Configured {
			return &domain.Error{Code: domain.CredentialRequired}
		}
		aad, err := c.AAD()
		if err != nil {
			return err
		}
		plaintext, err := s.deps.Cipher.Decrypt(ctx, c.KeyID, aad, c.Ciphertext)
		if err != nil {
			return ErrDependencyUnavailable
		}
		defer clear(plaintext)
		value := string(plaintext)
		if err = (domain.CredentialEdit{Action: "replace", Value: &value}).Validate(domain.Provider(r.View.Provider), preflightPurpose(domain.Provider(r.View.Provider))); err != nil {
			return &domain.Error{Code: domain.SourceIntegrity}
		}
		// Re-read the database clock after potentially delayed decryption. Holding the
		// account lock fences rotation, but does not extend the execution lease.
		now, err = tx.Now(ctx)
		if err != nil {
			return err
		}
		if err = preflightUsable(r, now); err != nil {
			return err
		}
		response = channelv1.PreflightResolveResponse{SchemaVersion: 1, PreflightID: id, ConnectionRevision: r.View.ConnectionRevision, Purpose: preflightPurpose(domain.Provider(r.View.Provider)), CredentialID: r.CredentialID, CredentialVersion: r.View.CredentialVersion(), Value: value, LeaseExpiresAt: *r.LeaseExpiresAt}
		return nil
	})
	if err != nil {
		return channelv1.PreflightResolveResponse{}, err
	}
	if operationErr != nil {
		return channelv1.PreflightResolveResponse{}, operationErr
	}
	return response, nil
}
func (s *PreflightService) Complete(ctx context.Context, p WorkloadPrincipal, id string, input channelv1.PreflightCompleteRequest) error {
	if err := s.authorize(p, input.ScopeID, input.SourceEpoch); err != nil {
		return err
	}
	if err := preflightWire("preflight-complete.schema.json", input); err != nil {
		return err
	}
	// The token authenticates the claim; it is not part of the business result.
	digestInput := input
	digestInput.ClaimToken = ""
	digest, err := preflightDigest(digestInput)
	if err != nil {
		return err
	}
	k, err := s.lookup(ctx, id)
	if err != nil {
		return err
	}
	var operationErr error
	err = s.deps.Store.WithTransaction(ctx, "task:"+id, func(tx PreflightTransaction) error {
		if tx.SourceEpoch() != input.SourceEpoch {
			return ErrEpochMismatch
		}
		a, err := tx.LockAccount(ctx, k.TenantID, k.AccountID, k.RequestedBy, false)
		if err != nil {
			return err
		}
		r, ok, err := tx.Load(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return preflightError("NOT_FOUND")
		}
		if err = preflightOwner(r, p, input.InstanceEpoch, input.LeaseEpoch, input.ClaimToken); err != nil {
			return err
		}
		// Historical acknowledgement is not new authorization. Keep it before
		// requester/tenant/lease checks, but after the exact original claim proof.
		if r.View.State == "COMPLETED" {
			if r.CompleteDigest == digest {
				return nil
			}
			return preflightError("RESULT_CONFLICT")
		}
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		preflightReconcile(&r, a, tx.SourceEpoch(), now, true)
		if err = tx.Save(ctx, r); err != nil {
			return err
		}
		if operationErr = preflightUsable(r, now); operationErr != nil {
			return nil
		}
		if input.EndpointProfile != r.View.EndpointProfile || input.DiagnosticPolicy != r.View.DiagnosticPolicy || input.ReceiveMode != r.View.ReceiveMode || (input.DiagnosticPolicy != "" && input.ConnectionRevision != r.View.ConnectionRevision) {
			return preflightError("RESULT_CONFLICT")
		}
		originStatus := input.Checks[2].Code
		if input.DiagnosticPolicy != "" {
			originStatus = input.OriginStatus
		}
		computed, err := channelv1.PreflightConfigDigestForPolicy(input.DiagnosticPolicy, input.ScopeID, input.SourceEpoch, input.ExpectedPublicOrigin, originStatus)
		if err != nil || computed != input.GatewayConfigDigest {
			return invalid("/gateway_config_digest")
		}
		if r.View.GatewayConfigDigest == nil {
			return ErrDependencyUnavailable
		}
		configChanged := computed != *r.View.GatewayConfigDigest
		if input.DiagnosticPolicy != "" {
			configChanged = input.EffectiveConfigDigest != r.View.EffectiveConfigDigest
			if !configChanged && computed != *r.View.GatewayConfigDigest {
				// Same lease cannot substitute fresh global config evidence.
				return preflightError("RESULT_CONFLICT")
			}
		}
		if configChanged {
			preflightStale(&r, "CHANNEL_PREFLIGHT_GATEWAY_CONFIG_CHANGED")
			if err = tx.Save(ctx, r); err != nil {
				return err
			}
			operationErr = preflightError("STALE")
			return nil
		}
		var configured struct {
			BotTokenConfigured      bool `json:"bot_token_configured"`
			WebhookSecretConfigured bool `json:"webhook_secret_configured"`
		}
		if r.View.Provider == "wecom" {
			var wecom struct {
				BotSecretConfigured bool `json:"bot_secret_configured"`
			}
			if json.Unmarshal(input.Checks[0].Details, &wecom) != nil || wecom.BotSecretConfigured != r.BotSecretConfigured || originStatus != r.OriginStatus {
				return preflightError("RESULT_CONFLICT")
			}
		} else if json.Unmarshal(input.Checks[0].Details, &configured) != nil || configured.BotTokenConfigured != r.BotTokenConfigured || configured.WebhookSecretConfigured != r.WebhookSecretConfigured || originStatus != r.OriginStatus {
			return preflightError("RESULT_CONFLICT")
		}
		outcome, err := channelv1.ValidatePreflightChecksForMode(input.DiagnosticPolicy, input.ReceiveMode, input.Checks)
		if err != nil {
			return invalid("/checks")
		}
		// observed_at is diagnostic evidence in the request digest only. It cannot
		// change server-owned checked_at, expiry, or the lease decision.
		r.View.State = "COMPLETED"
		r.View.Outcome = outcome
		r.View.ReasonCode = "CHANNEL_PREFLIGHT_COMPLETED"
		r.View.Freshness = "CURRENT"
		r.View.CheckedAt = &now
		expiry := now.Add(preflightReceiptTTL)
		r.View.ExpiresAt = &expiry
		r.View.Checks = append([]channelv1.PreflightCheck(nil), input.Checks...)
		r.CompleteDigest = digest
		r.LastCheckedAt = now
		return tx.Save(ctx, r)
	})
	if err != nil {
		return err
	}
	return operationErr
}
func (s *PreflightService) maintainOne(ctx context.Context, k PreflightKey) error {
	return s.deps.Store.WithTransaction(ctx, "task:"+k.ID, func(tx PreflightTransaction) error {
		a, err := tx.LockAccount(ctx, k.TenantID, k.AccountID, k.RequestedBy, false)
		if err != nil {
			return err
		}
		r, ok, err := tx.Load(ctx, k.ID)
		if err != nil {
			return err
		}
		if !ok || !preflightActive(r) {
			return nil
		}
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		preflightReconcile(&r, a, tx.SourceEpoch(), now, true)
		r.LastCheckedAt = now
		return tx.Save(ctx, r)
	})
}
func (s *PreflightService) Maintain(ctx context.Context) error {
	candidates, err := s.deps.Store.MaintenanceCandidates(ctx, s.deps.ScopeID, 64)
	if err != nil {
		return err
	}
	var result error
	for _, k := range candidates {
		result = errors.Join(result, s.maintainOne(ctx, k))
	}
	return errors.Join(result, s.deps.Store.Cleanup(ctx))
}

func preflightPurpose(provider domain.Provider) string {
	if provider == domain.WeCom {
		return domain.WeComBotSecret
	}
	return domain.TelegramBotToken
}
func preflightMode(account domain.Account) string {
	if account.Provider == domain.WeCom {
		return channelv1.PreflightWeComMode
	}
	return account.Config.ReceiveMode
}
func preflightPolicy(provider domain.Provider) string {
	if provider == domain.WeCom {
		return channelv1.PreflightWeComPolicy
	}
	return channelv1.PreflightReceiveModesPolicy
}
func preflightConsumer(provider string) string {
	if provider == "wecom" {
		return "wecom_preflight"
	}
	return "telegram_preflight"
}
func preflightClaimConsumer(policy string) string {
	if policy == channelv1.PreflightWeComPolicy {
		return "wecom_preflight"
	}
	return "telegram_preflight"
}
