package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const channelFailureEventID = "platform_channel:failure"

// Admit atomically revalidates the trusted request identity, fixes the active
// config version, allocates a session turn, and inserts the execution and its
// transactional dispatch outbox record.
// The transaction commit is the request admission linearization point.
func (s *Store) Admit(
	ctx context.Context,
	request gateway.AdmissionRequest,
) (gateway.AdmissionResult, error) {
	if err := s.validate(); err != nil {
		return gateway.AdmissionResult{}, err
	}
	if err := request.Validate(); err != nil {
		return gateway.AdmissionResult{}, err
	}
	if request.Identity.Source != gateway.TenantSourceAuthenticatedClaims &&
		request.Identity.Source != gateway.TenantSourceVerifiedChannelBinding {
		return gateway.AdmissionResult{}, gateway.ErrUnsupportedAdmissionSource
	}
	if request.Identity.Source == gateway.TenantSourceVerifiedChannelBinding &&
		request.ChannelInput == nil {
		return gateway.AdmissionResult{}, gateway.ErrChannelInputRequired
	}
	if request.ChannelInput != nil {
		// Channel event idempotency is defined by the binding-scoped provider
		// message ID. Do not let a caller-supplied key alias another message.
		request.IdempotencyKey = request.ChannelInput.ExternalMessageID
	}
	request.Identity.Tenant.TraceParent = request.TraceParent
	request.Identity.Tenant.TraceState = request.TraceState
	var command []byte
	var payloadHash [sha256.Size]byte
	var err error
	if request.ChannelInput == nil {
		command, payloadHash, err = marshalAdmissionCommand(request.Identity.Tenant, request.Message)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("begin admission: %w", err)
	}
	defer func() {
		rollback(tx)
	}()

	var credential auth.Credential
	tenantID := request.Identity.Tenant.TenantID
	appID := request.Identity.Tenant.AppID
	if request.Identity.Source == gateway.TenantSourceAuthenticatedClaims {
		credential, err = lockCredential(ctx, tx, request.Identity.CredentialDigest)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return gateway.AdmissionResult{}, auth.ErrUnauthenticated
			}
			return gateway.AdmissionResult{}, err
		}
		if err := validateAdmissionCredential(credential, request.Identity); err != nil {
			return gateway.AdmissionResult{}, err
		}
		tenantID = credential.TenantID
		appID = credential.AppID
	} else {
		credential = auth.Credential{
			ID:       request.Identity.SourceID,
			TenantID: tenantID,
			AppID:    appID,
			Status:   auth.CredentialActive,
		}
	}

	tnt, err := lockTenant(ctx, tx, tenantID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return gateway.AdmissionResult{}, auth.ErrUnauthenticated
		}
		return gateway.AdmissionResult{}, err
	}
	if tnt.Status != tenant.StatusActive {
		return gateway.AdmissionResult{}, auth.ErrTenantInactive
	}
	if tnt.ID != request.Identity.Tenant.TenantID {
		return gateway.AdmissionResult{}, auth.ErrUnauthenticated
	}

	app, err := lockAgentApp(ctx, tx, tenantID, appID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return gateway.AdmissionResult{}, auth.ErrUnauthenticated
		}
		return gateway.AdmissionResult{}, err
	}
	if app.Status != tenant.StatusActive {
		return gateway.AdmissionResult{}, auth.ErrAppInactive
	}
	if app.TenantID != request.Identity.Tenant.TenantID ||
		app.AppID != request.Identity.Tenant.AppID {
		return gateway.AdmissionResult{}, auth.ErrUnauthenticated
	}
	if request.Identity.Source == gateway.TenantSourceVerifiedChannelBinding {
		if err := revalidateChannelBindingSnapshot(ctx, tx, request.Identity); err != nil {
			return gateway.AdmissionResult{}, err
		}
	}
	appConfig, err := resolveAppConfigFrom(ctx, tx, app.TenantID, app.AppID, app.ActiveConfigVersion)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	admission := admissionTransaction{
		ctx:              ctx,
		tx:               tx,
		request:          request,
		credential:       credential,
		app:              app,
		command:          command,
		payloadHash:      payloadHash,
		runtimeContext:   request.Identity.Tenant,
		configVersion:    app.ActiveConfigVersion,
		channelAdmission: request.ChannelInput != nil,
		tenantQuota:      tnt.Quota,
		appBudget:        appConfig.Budget,
		metrics:          s.metrics,
	}
	if request.ChannelInput == nil {
		if request.Identity.ConfigVersionPinned() {
			admission.configVersion = request.Identity.Tenant.ConfigVersion
			if admission.configVersion == "" {
				return gateway.AdmissionResult{}, errors.New("pinned channel config version is required")
			}
			appConfig, err = resolveAppConfigFrom(
				ctx, tx, app.TenantID, app.AppID, admission.configVersion,
			)
		} else {
			admission.configVersion, appConfig, err = resolveAdmissionConfig(
				ctx, tx, app, admission.runtimeContext, appConfig,
			)
		}
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		admission.runtimeContext.ConfigVersion = admission.configVersion
	}
	// Canary and pinned-config selection above may replace the initially
	// resolved active version. Quota reservation must use the pinned version's
	// application budget.
	admission.appBudget = appConfig.Budget
	if request.ChannelInput != nil {
		if s.identityMapper == nil {
			return gateway.AdmissionResult{}, errors.New("channel identity mapper is required")
		}
		mappingRequest, err := channelIdentityMappingRequest(request)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		payloadHash, err = s.identityMapper.channelPayloadHash(ctx, mappingRequest, *request.ChannelInput)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		admission.payloadHash = payloadHash
		inbox, found, err := findChannelInbox(
			ctx,
			tx,
			request.Identity.Tenant.TenantID,
			request.Identity.Tenant.AppID,
			request.Identity.Tenant.BindingID,
			request.ChannelInput.ExternalMessageID,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		if found {
			return admission.reconcileChannelInbox(inbox)
		}
		rejectReason, err := channelInputRejectReason(*request.ChannelInput)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		if rejectReason != "" {
			if err := insertChannelInbox(
				ctx,
				tx,
				request,
				payloadHash[:],
				channelInboxStatusRejected,
				rejectReason,
				nil,
				nil,
			); err != nil {
				return gateway.AdmissionResult{}, admissionInsertError("insert rejected channel inbox", err)
			}
			if result, replayed, err := admission.reconcileInsertedChannelInbox(); err != nil {
				return gateway.AdmissionResult{}, err
			} else if replayed {
				return result, nil
			}
			if err := tx.Commit(ctx); err != nil {
				return gateway.AdmissionResult{}, fmt.Errorf("commit rejected channel admission: %w", err)
			}
			return gateway.AdmissionResult{
				RequestID: request.RequestID,
				Status:    gateway.AdmissionStatusRejected,
			}, nil
		}
		targetEnvelope, targetExpiresAt, err := request.ChannelInput.SealMessageReplyTarget(
			ctx,
			s.identityMapper.protector,
			request.Identity.Tenant.Scope(),
			request.RequestID,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		if err := insertChannelInbox(
			ctx,
			tx,
			request,
			payloadHash[:],
			channelInboxStatusAdmitted,
			"",
			targetEnvelope,
			targetExpiresAt,
		); err != nil {
			return gateway.AdmissionResult{}, admissionInsertError("insert channel inbox", err)
		}
		if result, replayed, err := admission.reconcileInsertedChannelInbox(); err != nil {
			return gateway.AdmissionResult{}, err
		} else if replayed {
			return result, nil
		}
		mapped, err := s.identityMapper.mapInTransaction(ctx, tx, mappingRequest)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		mapped.SessionID, err = ensureActiveSessionTx(
			ctx,
			tx,
			request.Identity.Tenant.Scope(),
			request.Identity.Tenant.BindingID,
			mapped.SessionPrincipalID,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		admission.runtimeContext = request.Identity.Tenant
		admission.runtimeContext.ConfigVersion = app.ActiveConfigVersion
		admission.runtimeContext.BindingRevision = request.Identity.BindingRevision
		admission.runtimeContext.SessionPrincipalID = mapped.SessionPrincipalID
		admission.runtimeContext.SessionID = mapped.SessionID
		admission.runtimeContext.UserID = mapped.Identity.UserID
		admission.runtimeContext.TraceParent = request.TraceParent
		admission.runtimeContext.TraceState = request.TraceState
		if request.Identity.ConfigVersionPinned() {
			admission.configVersion = request.Identity.Tenant.ConfigVersion
			if admission.configVersion == "" {
				return gateway.AdmissionResult{}, errors.New("pinned channel config version is required")
			}
			appConfig, err = resolveAppConfigFrom(
				ctx, tx, app.TenantID, app.AppID, admission.configVersion,
			)
		} else {
			admission.configVersion, appConfig, err = resolveAdmissionConfig(
				ctx, tx, app, admission.runtimeContext, appConfig,
			)
		}
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		admission.runtimeContext.ConfigVersion = admission.configVersion
		if !appConfig.IMAccess.Allows(
			admission.runtimeContext.UserID,
			admission.runtimeContext.SessionPrincipalID,
		) {
			if err := rejectChannelInboxForAccess(
				ctx,
				tx,
				request,
			); err != nil {
				return gateway.AdmissionResult{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return gateway.AdmissionResult{}, fmt.Errorf("commit denied channel admission: %w", err)
			}
			if appConfig.Audit.Enabled {
				event := platformaudit.Event{
					TenantID:      admission.runtimeContext.TenantID,
					AppID:         admission.runtimeContext.AppID,
					Channel:       admission.runtimeContext.Channel,
					UserID:        admission.runtimeContext.UserID,
					SessionID:     admission.runtimeContext.SessionID,
					AgentName:     "assistant",
					Decision:      "rejected",
					ErrorType:     "im_access_denied",
					TraceID:       admission.runtimeContext.TraceID,
					RequestID:     request.RequestID,
					ConfigVersion: admission.configVersion,
					EventType:     platformaudit.IMAccessDenied,
				}
				if appConfig.Audit.RedactPII {
					event = platformaudit.RedactEvent(event)
				}
				s.recordAuditBestEffort(ctx, event)
			}
			if s.metrics != nil {
				s.metrics.RecordGovernanceRejected(ctx, platformmetrics.Labels{
					TenantID:      admission.runtimeContext.TenantID,
					AppID:         admission.runtimeContext.AppID,
					ConfigVersion: admission.configVersion,
					Channel:       admission.runtimeContext.Channel,
				}, platformaudit.IMAccessDenied)
			}
			return gateway.AdmissionResult{
				RequestID: request.RequestID,
				Status:    gateway.AdmissionStatusRejected,
			}, nil
		}
		admission.command, _, err = marshalAdmissionCommand(
			admission.runtimeContext,
			request.Message,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
	}
	admission.appBudget = appConfig.Budget
	existing, found, err := findExecutionByIdempotency(
		ctx,
		tx,
		idempotencyLookup{
			TenantID:       credential.TenantID,
			AppID:          credential.AppID,
			Source:         request.Identity.Source,
			SourceID:       request.Identity.SourceID,
			IdempotencyKey: request.IdempotencyKey,
		},
	)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if found {
		return admission.reconcileExisting(existing)
	}
	return admission.createExecution()
}

// RecordChannelFailure durably records a rejected Inbox row and its
// provider-neutral failure reply. Replays are idempotent, and an admission
// that committed despite an ambiguous caller error always wins.
func (s *Store) RecordChannelFailure(ctx context.Context, request gateway.AdmissionRequest) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Identity.Source != gateway.TenantSourceVerifiedChannelBinding || request.ChannelInput == nil {
		return gateway.ErrChannelInputRequired
	}
	if s.identityMapper == nil {
		return errors.New("channel identity mapper is required")
	}
	mappingRequest, err := channelIdentityMappingRequest(request)
	if err != nil {
		return err
	}
	payloadHash, err := s.identityMapper.channelPayloadHash(ctx, mappingRequest, *request.ChannelInput)
	if err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin channel failure: %w", err)
	}
	defer func() { rollback(tx) }()
	if err := revalidateChannelBindingSnapshot(ctx, tx, request.Identity); err != nil {
		return err
	}
	inbox, found, err := findChannelInbox(
		ctx, tx, request.Identity.Tenant.TenantID, request.Identity.Tenant.AppID,
		request.Identity.Tenant.BindingID, request.ChannelInput.ExternalMessageID,
	)
	if err != nil {
		return err
	}
	if found {
		if !bytes.Equal(inbox.PayloadHash, payloadHash[:]) {
			return gateway.ErrIdempotencyConflict
		}
		if inbox.Status == channelInboxStatusAdmitted {
			return tx.Commit(ctx)
		}
		if inbox.Status != channelInboxStatusRejected {
			return fmt.Errorf("channel inbox status %q is invalid", inbox.Status)
		}
		request.RequestID = inbox.RequestID
	}
	targetEnvelope, targetExpiresAt, err := request.ChannelInput.SealMessageReplyTarget(
		ctx, s.identityMapper.protector, request.Identity.Tenant.Scope(), request.RequestID,
	)
	if err != nil {
		return err
	}
	if found {
		encodedTarget, err := json.Marshal(targetEnvelope)
		if err != nil {
			return fmt.Errorf("marshal channel failure reply target: %w", err)
		}
		if _, err := tx.Exec(ctx, `
UPDATE platform.channel_inbox
SET provider_reply_target_envelope = $5, reply_target_expires_at = $6,
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND status = 'REJECTED'`,
			request.Identity.Tenant.TenantID, request.Identity.Tenant.AppID,
			request.Identity.Tenant.BindingID, request.ChannelInput.ExternalMessageID,
			encodedTarget, targetExpiresAt,
		); err != nil {
			return fmt.Errorf("update channel failure reply target: %w", err)
		}
	} else if err := insertChannelInbox(
		ctx, tx, request, payloadHash[:], channelInboxStatusRejected,
		"CHANNEL_PROCESSING_FAILED", targetEnvelope, targetExpiresAt,
	); err != nil {
		return admissionInsertError("insert failed channel inbox", err)
	}
	reply := channels.Reply{
		TenantID: request.Identity.Tenant.TenantID, AppID: request.Identity.Tenant.AppID,
		RequestID: request.RequestID, SourceEventID: channelFailureEventID,
		Channel: channels.Channel(request.Identity.Tenant.Channel), BindingID: request.Identity.Tenant.BindingID,
		BindingRevision: request.Identity.BindingRevision, Revision: 1,
		Target: channels.ReplyTarget{Kind: channels.TargetKindMessage, InternalEntityID: request.RequestID},
		Text:   channels.ChannelFailureReply,
	}
	if err := insertReplyOutboxTxWithSourceKind(
		ctx, tx, reply.TenantID, reply.AppID, reply.BindingID, reply.RequestID,
		"channel_failure", []channels.Reply{reply},
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit channel failure: %w", err)
	}
	return nil
}

// PinChannelConfig selects the exact config version that a channel request
// will use before provider media is materialized. The identity mapping runs
// in the same transaction as the app canary read, then the transaction is
// rolled back because the real admission repeats and commits the mapping.
func (s *Store) PinChannelConfig(
	ctx context.Context,
	request gateway.AdmissionRequest,
) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if err := request.Validate(); err != nil {
		return "", err
	}
	if request.ChannelInput == nil || request.Identity.Source != gateway.TenantSourceVerifiedChannelBinding {
		return "", gateway.ErrChannelInputRequired
	}
	if s.identityMapper == nil {
		return "", errors.New("channel identity mapper is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("begin channel config pin: %w", err)
	}
	defer func() { rollback(tx) }()
	tenantID := request.Identity.Tenant.TenantID
	appID := request.Identity.Tenant.AppID
	tnt, err := lockTenant(ctx, tx, tenantID)
	if err != nil {
		return "", err
	}
	if tnt.Status != tenant.StatusActive {
		return "", auth.ErrTenantInactive
	}
	app, err := lockAgentApp(ctx, tx, tenantID, appID)
	if err != nil {
		return "", err
	}
	if app.Status != tenant.StatusActive {
		return "", auth.ErrAppInactive
	}
	if err := revalidateChannelBindingSnapshot(ctx, tx, request.Identity); err != nil {
		return "", err
	}
	mappingRequest, err := channelIdentityMappingRequest(request)
	if err != nil {
		return "", err
	}
	mapped, err := s.identityMapper.mapInTransaction(ctx, tx, mappingRequest)
	if err != nil {
		return "", err
	}
	mapped.SessionID, err = ensureActiveSessionTx(
		ctx,
		tx,
		request.Identity.Tenant.Scope(),
		request.Identity.Tenant.BindingID,
		mapped.SessionPrincipalID,
	)
	if err != nil {
		return "", err
	}
	runtimeContext := request.Identity.Tenant
	runtimeContext.SessionPrincipalID = mapped.SessionPrincipalID
	runtimeContext.SessionID = mapped.SessionID
	runtimeContext.UserID = mapped.Identity.UserID
	active, err := resolveAppConfigFrom(ctx, tx, app.TenantID, app.AppID, app.ActiveConfigVersion)
	if err != nil {
		return "", err
	}
	version := selectCanaryConfigVersion(app, runtimeContext)
	if version != active.Version {
		if _, err := resolveAppConfigFrom(ctx, tx, app.TenantID, app.AppID, version); err != nil {
			return "", err
		}
	}
	return version, nil
}

func rejectChannelInboxForAccess(
	ctx context.Context,
	tx pgx.Tx,
	request gateway.AdmissionRequest,
) error {
	if request.ChannelInput == nil {
		return errors.New("channel input is required")
	}
	tag, err := tx.Exec(ctx, `
UPDATE platform.channel_inbox
SET status = 'REJECTED', reject_reason = 'IM_ACCESS_DENIED',
    provider_reply_target_envelope = NULL, reply_target_expires_at = NULL,
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND status = 'ADMITTED'`,
		request.ChannelInput.TenantID,
		request.ChannelInput.AppID,
		request.ChannelInput.BindingID,
		request.ChannelInput.ExternalMessageID,
	)
	if err != nil {
		return fmt.Errorf("reject channel inbox for access: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("reject channel inbox for access: inbox row is unavailable")
	}
	return nil
}

type admissionTransaction struct {
	ctx              context.Context
	tx               pgx.Tx
	request          gateway.AdmissionRequest
	credential       auth.Credential
	app              tenant.AgentApp
	command          []byte
	payloadHash      [sha256.Size]byte
	runtimeContext   tenant.RuntimeContext
	configVersion    string
	channelAdmission bool
	tenantQuota      tenant.QuotaPolicy
	appBudget        tenant.BudgetPolicy
	metrics          *platformmetrics.Recorder
}

func resolveAdmissionConfig(
	ctx context.Context,
	tx pgx.Tx,
	app tenant.AgentApp,
	runtimeContext tenant.RuntimeContext,
	active tenant.AppConfig,
) (string, tenant.AppConfig, error) {
	version := selectCanaryConfigVersion(app, runtimeContext)
	if version == active.Version {
		return version, active, nil
	}
	selected, err := resolveAppConfigFrom(ctx, tx, app.TenantID, app.AppID, version)
	if err != nil {
		return "", tenant.AppConfig{}, err
	}
	return version, selected, nil
}

// selectCanaryConfigVersion deterministically assigns one routing identity to
// one bucket. The identity comes from the authenticated credential or channel
// mapping and is never generated from user-provided message content.
func selectCanaryConfigVersion(app tenant.AgentApp, runtimeContext tenant.RuntimeContext) string {
	if app.CanaryStatus != tenant.CanaryEnabled ||
		app.CanaryConfigVersion == "" || app.CanaryPercentage <= 0 {
		return app.ActiveConfigVersion
	}
	if runtimeContext.SessionPrincipalID == "" || runtimeContext.SessionID == "" {
		return app.ActiveConfigVersion
	}
	key := app.TenantID + "\x00" + app.AppID + "\x00" +
		runtimeContext.SessionPrincipalID + "\x00" + runtimeContext.SessionID
	digest := sha256.Sum256([]byte(key))
	bucket := int(binary.BigEndian.Uint32(digest[:4]) % 100)
	if bucket < app.CanaryPercentage {
		return app.CanaryConfigVersion
	}
	return app.ActiveConfigVersion
}

func channelIdentityMappingRequest(request gateway.AdmissionRequest) (IdentityMappingRequest, error) {
	if request.ChannelInput == nil {
		return IdentityMappingRequest{}, errors.New("channel input is required")
	}
	mapping, ok := request.ChannelInput.MappingInput()
	if !ok {
		return IdentityMappingRequest{}, errors.New("channel mapping input is required")
	}
	return IdentityMappingRequest{
		Scope:                      request.Identity.Tenant.Scope(),
		BindingID:                  request.Identity.Tenant.BindingID,
		Channel:                    channels.Channel(request.Identity.Tenant.Channel),
		Kind:                       request.ChannelInput.Conversation.Kind,
		ExternalSenderID:           mapping.ExternalSenderID,
		ExternalChatID:             mapping.ExternalChatID,
		ExternalThreadID:           mapping.ExternalThreadID,
		ProviderSenderTarget:       mapping.ProviderSenderTarget,
		ProviderConversationTarget: mapping.ProviderConversationTarget,
		ProviderThreadTarget:       mapping.ProviderThreadTarget,
	}, nil
}

func (a admissionTransaction) reconcileChannelInbox(
	inbox channelInboxRecord,
) (gateway.AdmissionResult, error) {
	if !bytes.Equal(inbox.PayloadHash, a.payloadHash[:]) {
		return gateway.AdmissionResult{}, gateway.ErrIdempotencyConflict
	}
	switch inbox.Status {
	case channelInboxStatusRejected:
		if err := a.tx.Commit(a.ctx); err != nil {
			return gateway.AdmissionResult{}, fmt.Errorf("commit rejected channel replay: %w", err)
		}
		return gateway.AdmissionResult{
			RequestID: inbox.RequestID,
			Replayed:  true,
			Status:    gateway.AdmissionStatusRejected,
		}, nil
	case channelInboxStatusAdmitted:
		existing, found, err := findExecutionByRequestID(
			a.ctx,
			a.tx,
			a.credential.TenantID,
			a.credential.AppID,
			inbox.RequestID,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		if !found {
			return gateway.AdmissionResult{}, errors.New("admitted channel inbox has no execution")
		}
		if !bytes.Equal(existing.PayloadHash, a.payloadHash[:]) {
			return gateway.AdmissionResult{}, gateway.ErrIdempotencyConflict
		}
		result := gateway.AdmissionResult{
			RequestID:     existing.RequestID,
			ConfigVersion: existing.ConfigVersion,
			TurnSeq:       existing.TurnSeq,
			Replayed:      true,
			Status:        gateway.AdmissionStatusAdmitted,
		}
		if err := a.tx.Commit(a.ctx); err != nil {
			return gateway.AdmissionResult{}, fmt.Errorf("commit admitted channel replay: %w", err)
		}
		return result, nil
	default:
		return gateway.AdmissionResult{}, fmt.Errorf("channel inbox status %q is invalid", inbox.Status)
	}
}

func (a admissionTransaction) reconcileInsertedChannelInbox() (gateway.AdmissionResult, bool, error) {
	input := a.request.ChannelInput
	if input == nil {
		return gateway.AdmissionResult{}, false, errors.New("channel input is required")
	}
	inbox, found, err := findChannelInbox(
		a.ctx,
		a.tx,
		input.TenantID,
		input.AppID,
		input.BindingID,
		input.ExternalMessageID,
	)
	if err != nil {
		return gateway.AdmissionResult{}, false, err
	}
	if !found {
		return gateway.AdmissionResult{}, false, errors.New("channel inbox was not available after insert")
	}
	if !bytes.Equal(inbox.PayloadHash, a.payloadHash[:]) {
		return gateway.AdmissionResult{}, false, gateway.ErrIdempotencyConflict
	}
	if inbox.RequestID == a.request.RequestID {
		return gateway.AdmissionResult{}, false, nil
	}
	result, err := a.reconcileChannelInbox(inbox)
	return result, true, err
}

func (a admissionTransaction) reconcileExisting(existing admissionExecution) (gateway.AdmissionResult, error) {
	if !bytes.Equal(existing.PayloadHash, a.payloadHash[:]) {
		return gateway.AdmissionResult{}, gateway.ErrIdempotencyConflict
	}
	status := gateway.AdmissionStatus("")
	if a.channelAdmission {
		status = gateway.AdmissionStatusAdmitted
	}
	result := gateway.AdmissionResult{
		RequestID:     existing.RequestID,
		ConfigVersion: existing.ConfigVersion,
		TurnSeq:       existing.TurnSeq,
		Replayed:      true,
		Status:        status,
	}
	if err := a.tx.Commit(a.ctx); err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("commit replayed admission: %w", err)
	}
	return result, nil
}

func (a admissionTransaction) createExecution() (gateway.AdmissionResult, error) {
	blocked, err := migrationBlocksAdmission(a.ctx, a.tx, a.app.TenantID, a.app.AppID)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if blocked {
		return gateway.AdmissionResult{}, gateway.ErrAdmissionDraining
	}
	requestExists, err := executionRequestExists(a.ctx, a.tx, a.credential.TenantID, a.credential.AppID, a.request.RequestID)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if requestExists {
		return gateway.AdmissionResult{}, fmt.Errorf("request_id already exists: %w", gateway.ErrIdempotencyConflict)
	}
	if err := reserveExecutionQuota(a.ctx, a.tx, a.credential.TenantID, a.credential.AppID,
		a.request.RequestID, a.tenantQuota, a.appBudget); err != nil {
		if errors.Is(err, tenant.ErrQuotaExceeded) {
			if a.metrics != nil {
				a.metrics.RecordGovernanceRejected(a.ctx, platformmetrics.Labels{
					TenantID:      a.credential.TenantID,
					AppID:         a.credential.AppID,
					ConfigVersion: a.configVersion,
					Channel:       a.runtimeContext.Channel,
				}, "quota_exceeded")
			}
			return gateway.AdmissionResult{}, fmt.Errorf("%w: %w", gateway.ErrAdmissionQuotaExceeded, err)
		}
		return gateway.AdmissionResult{}, err
	}

	runtimeContext := a.runtimeContext
	if runtimeContext == (tenant.RuntimeContext{}) {
		runtimeContext = a.request.Identity.Tenant
	}
	turnSeq, err := allocateSessionTurn(
		a.ctx,
		a.tx,
		a.credential.TenantID,
		a.credential.AppID,
		runtimeContext.SessionPrincipalID,
		runtimeContext.SessionID,
	)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if err := lockReferencedArtifacts(
		a.ctx,
		a.tx,
		runtimeContext,
		a.configVersion,
		a.request.Message.ArtifactRefs,
	); err != nil {
		return gateway.AdmissionResult{}, err
	}
	if _, err := a.tx.Exec(
		a.ctx,
		`INSERT INTO platform.execution (
    tenant_id,
    app_id,
    request_id,
    session_principal_id,
    session_id,
    user_id,
    turn_seq,
    config_version,
    tenant_source,
    source_id,
    idempotency_key,
    payload_hash,
    command,
    status,
    trace_id,
    trace_parent,
    trace_state
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'PENDING', $14, $15, $16)`,
		a.credential.TenantID,
		a.credential.AppID,
		a.request.RequestID,
		runtimeContext.SessionPrincipalID,
		runtimeContext.SessionID,
		runtimeContext.UserID,
		turnSeq,
		a.configVersion,
		a.request.Identity.Source,
		a.request.Identity.SourceID,
		a.request.IdempotencyKey,
		a.payloadHash[:],
		a.command,
		runtimeContext.TraceID,
		runtimeContext.TraceParent,
		runtimeContext.TraceState,
	); err != nil {
		return gateway.AdmissionResult{}, admissionInsertError("insert execution", err)
	}
	if a.request.ChannelInput != nil {
		// Keep the execution row in the same transaction, but insert it before
		// attaching staged artifacts. Cleanup can then observe either the
		// still-PENDING inbox row (which it excludes) or the committed execution
		// reference; it cannot delete an artifact in the admission gap.
		if err := attachStagedInboundArtifacts(
			a.ctx,
			a.tx,
			*a.request.ChannelInput,
			a.configVersion,
			runtimeContext.SessionPrincipalID,
			runtimeContext.SessionID,
		); err != nil {
			return gateway.AdmissionResult{}, err
		}
	}
	if _, err := a.tx.Exec(
		a.ctx,
		`INSERT INTO platform.dispatch_outbox (tenant_id, app_id, request_id)
VALUES ($1, $2, $3)`,
		a.credential.TenantID,
		a.credential.AppID,
		a.request.RequestID,
	); err != nil {
		return gateway.AdmissionResult{}, admissionInsertError("insert dispatch outbox", err)
	}

	if err := a.tx.Commit(a.ctx); err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("commit admission: %w", err)
	}
	return gateway.AdmissionResult{
		RequestID:     a.request.RequestID,
		ConfigVersion: a.configVersion,
		TurnSeq:       turnSeq,
		Status:        gateway.AdmissionStatusAdmitted,
	}, nil
}

type databaseQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func lockCredential(
	ctx context.Context,
	tx pgx.Tx,
	digest gateway.CredentialDigest,
) (auth.Credential, error) {
	var credential auth.Credential
	var expiresAt *time.Time
	err := tx.QueryRow(
		ctx,
		`SELECT credential_id, tenant_id, app_id, key_prefix, status, expires_at
FROM platform.api_credential
WHERE key_digest = $1
FOR UPDATE`,
		digest[:],
	).Scan(
		&credential.ID,
		&credential.TenantID,
		&credential.AppID,
		&credential.KeyPrefix,
		&credential.Status,
		&expiresAt,
	)
	if err != nil {
		return auth.Credential{}, resolveError("api credential", err)
	}
	if expiresAt != nil {
		credential.ExpiresAt = expiresAt.UTC()
	}
	return credential, nil
}

func validateAdmissionCredential(
	credential auth.Credential,
	identity gateway.AdmissionIdentity,
) error {
	if err := credential.Validate(); err != nil {
		return fmt.Errorf("stored api credential: %w", err)
	}
	if credential.Status != auth.CredentialActive ||
		(!credential.ExpiresAt.IsZero() && !time.Now().Before(credential.ExpiresAt)) {
		return auth.ErrCredentialInactive
	}
	if credential.ID != identity.SourceID ||
		credential.TenantID != identity.Tenant.TenantID ||
		credential.AppID != identity.Tenant.AppID {
		return auth.ErrUnauthenticated
	}
	return nil
}

func lockTenant(ctx context.Context, tx pgx.Tx, tenantID string) (tenant.Tenant, error) {
	var value tenant.Tenant
	var auditPolicy, quotaPolicy []byte
	err := tx.QueryRow(
		ctx,
		`SELECT tenant_id, name, status, audit_policy, quota_policy
FROM platform.tenant
WHERE tenant_id = $1
FOR UPDATE`,
		tenantID,
	).Scan(&value.ID, &value.Name, &value.Status, &auditPolicy, &quotaPolicy)
	if err != nil {
		return tenant.Tenant{}, resolveError("tenant", err)
	}
	value.Audit, err = unmarshalAuditPolicy(auditPolicy)
	if err != nil {
		return tenant.Tenant{}, err
	}
	if err := json.Unmarshal(quotaPolicy, &value.Quota); err != nil {
		return tenant.Tenant{}, fmt.Errorf("unmarshal tenant quota policy: %w", err)
	}
	if err := value.Validate(); err != nil {
		return tenant.Tenant{}, fmt.Errorf("stored tenant: %w", err)
	}
	return value, nil
}

func lockAgentApp(ctx context.Context, tx pgx.Tx, tenantID, appID string) (tenant.AgentApp, error) {
	var app tenant.AgentApp
	err := tx.QueryRow(
		ctx,
		`SELECT tenant_id, app_id, name, active_config_version,
       COALESCE(canary_config_version, ''), canary_percentage, canary_status, status
FROM platform.agent_app
WHERE tenant_id = $1 AND app_id = $2
FOR UPDATE`,
		tenantID,
		appID,
	).Scan(
		&app.TenantID,
		&app.AppID,
		&app.Name,
		&app.ActiveConfigVersion,
		&app.CanaryConfigVersion,
		&app.CanaryPercentage,
		&app.CanaryStatus,
		&app.Status,
	)
	if err != nil {
		return tenant.AgentApp{}, resolveError("agent app", err)
	}
	if err := app.Validate(); err != nil {
		return tenant.AgentApp{}, fmt.Errorf("stored agent app: %w", err)
	}
	return app, nil
}

func revalidateChannelBindingSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	identity gateway.AdmissionIdentity,
) error {
	binding, err := lockChannelBinding(
		ctx,
		tx,
		identity.Tenant.TenantID,
		identity.Tenant.AppID,
		identity.Tenant.BindingID,
	)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return auth.ErrUnauthenticated
		}
		return err
	}
	if binding.Status != channels.BindingActive {
		return channels.ErrBindingInactive
	}
	if binding.TenantID != identity.Tenant.TenantID ||
		binding.AppID != identity.Tenant.AppID ||
		binding.BindingID != identity.Tenant.BindingID ||
		binding.BindingID != identity.SourceID ||
		binding.Channel != channels.Channel(identity.Tenant.Channel) {
		return gateway.ErrChannelBindingSnapshotStale
	}
	if binding.PublicRouteID != identity.PublicRouteID ||
		binding.BindingRevision != identity.BindingRevision {
		return gateway.ErrChannelBindingSnapshotStale
	}
	return nil
}

func resolveAppConfigFrom(
	ctx context.Context,
	db databaseQueryer,
	tenantID, appID, version string,
) (tenant.AppConfig, error) {
	var encoded appConfigColumns
	err := db.QueryRow(
		ctx,
		`SELECT model_config, tool_policy, backend_config, audit_policy,
       secret_refs, channel_binding_ids, knowledge_base_ids
FROM platform.app_config_version
WHERE tenant_id = $1 AND app_id = $2 AND version = $3 AND status = 'PUBLISHED'`,
		tenantID,
		appID,
		version,
	).Scan(
		&encoded.modelConfig,
		&encoded.toolPolicy,
		&encoded.backendConfig,
		&encoded.auditPolicy,
		&encoded.secretRefs,
		&encoded.channelBindingIDs,
		&encoded.knowledgeBaseIDs,
	)
	if err != nil {
		return tenant.AppConfig{}, resolveError("app config", err)
	}
	return unmarshalAppConfig(tenantID, appID, version, encoded)
}

type admissionExecution struct {
	RequestID     string
	ConfigVersion string
	TurnSeq       int64
	PayloadHash   []byte
	Status        string
}

type idempotencyLookup struct {
	TenantID       string
	AppID          string
	Source         gateway.TenantSource
	SourceID       string
	IdempotencyKey string
}

func findExecutionByIdempotency(
	ctx context.Context,
	tx pgx.Tx,
	lookup idempotencyLookup,
) (admissionExecution, bool, error) {
	var value admissionExecution
	err := tx.QueryRow(
		ctx,
		`SELECT request_id, config_version, turn_seq, payload_hash, status
FROM platform.execution
WHERE tenant_id = $1
  AND app_id = $2
  AND tenant_source = $3
  AND source_id = $4
  AND idempotency_key = $5
FOR UPDATE`,
		lookup.TenantID,
		lookup.AppID,
		lookup.Source,
		lookup.SourceID,
		lookup.IdempotencyKey,
	).Scan(&value.RequestID, &value.ConfigVersion, &value.TurnSeq, &value.PayloadHash, &value.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return admissionExecution{}, false, nil
	}
	if err != nil {
		return admissionExecution{}, false, fmt.Errorf("find idempotent execution: %w", err)
	}
	return value, true, nil
}

func findExecutionByRequestID(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, requestID string,
) (admissionExecution, bool, error) {
	var value admissionExecution
	err := tx.QueryRow(
		ctx,
		`SELECT request_id, config_version, turn_seq, payload_hash, status
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
FOR UPDATE`,
		tenantID,
		appID,
		requestID,
	).Scan(&value.RequestID, &value.ConfigVersion, &value.TurnSeq, &value.PayloadHash, &value.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return admissionExecution{}, false, nil
	}
	if err != nil {
		return admissionExecution{}, false, fmt.Errorf("find channel execution: %w", err)
	}
	return value, true, nil
}

func executionRequestExists(ctx context.Context, tx pgx.Tx, tenantID, appID, requestID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(
		ctx,
		`SELECT EXISTS (
    SELECT 1
    FROM platform.execution
    WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
)`,
		tenantID,
		appID,
		requestID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check request id: %w", err)
	}
	return exists, nil
}

func allocateSessionTurn(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, principalID, sessionID string,
) (int64, error) {
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO platform.session_lane (
    tenant_id, app_id, session_principal_id, session_id
) VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, app_id, session_principal_id, session_id) DO NOTHING`,
		tenantID,
		appID,
		principalID,
		sessionID,
	); err != nil {
		return 0, fmt.Errorf("create session lane: %w", err)
	}
	var nextTurnSeq int64
	if err := tx.QueryRow(
		ctx,
		`SELECT next_turn_seq
FROM platform.session_lane
WHERE tenant_id = $1
  AND app_id = $2
  AND session_principal_id = $3
  AND session_id = $4
FOR UPDATE`,
		tenantID,
		appID,
		principalID,
		sessionID,
	).Scan(&nextTurnSeq); err != nil {
		return 0, fmt.Errorf("lock session lane: %w", err)
	}
	if nextTurnSeq <= 0 {
		return 0, errors.New("session lane next_turn_seq is invalid")
	}
	if _, err := tx.Exec(
		ctx,
		`UPDATE platform.session_lane
SET next_turn_seq = next_turn_seq + 1, updated_at = now()
WHERE tenant_id = $1
  AND app_id = $2
  AND session_principal_id = $3
  AND session_id = $4`,
		tenantID,
		appID,
		principalID,
		sessionID,
	); err != nil {
		return 0, fmt.Errorf("advance session lane: %w", err)
	}
	return nextTurnSeq, nil
}

// lockReferencedArtifacts serializes ordinary execution admission with
// artifact cleanup. Cleanup locks the same artifact row before marking it
// deleted; admission therefore either wins the lock and creates an active
// execution, or observes the cleanup result and rejects the stale reference.
func lockReferencedArtifacts(
	ctx context.Context,
	tx pgx.Tx,
	runtimeContext tenant.RuntimeContext,
	configVersion string,
	refs []string,
) error {
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		filename, version, err := gateway.ParseArtifactRef(ref)
		if err != nil {
			return fmt.Errorf("lock artifact reference: %w", err)
		}
		key := fmt.Sprintf("%s@%d", filename, version)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		var status string
		err = tx.QueryRow(ctx, `
SELECT status
FROM platform.artifact
WHERE tenant_id = $1
  AND app_id = $2
  AND session_principal_id = $3
  AND session_id = $4
  AND filename = $5
  AND version = $6
  AND config_version = $7
FOR UPDATE`,
			runtimeContext.TenantID,
			runtimeContext.AppID,
			runtimeContext.SessionPrincipalID,
			runtimeContext.SessionID,
			filename,
			version,
			configVersion,
		).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			// Inbound artifacts are attached later in this same admission
			// transaction. They have no platform.artifact row to lock yet.
			continue
		}
		if err != nil {
			return fmt.Errorf("lock artifact reference %s: %w", key, err)
		}
		if status != "AVAILABLE" {
			return fmt.Errorf("artifact reference %s is not available", key)
		}
	}
	return nil
}

type admissionCommand struct {
	TenantID           string   `json:"tenant_id"`
	AppID              string   `json:"app_id"`
	Channel            string   `json:"channel,omitempty"`
	BindingID          string   `json:"binding_id,omitempty"`
	BindingRevision    int64    `json:"binding_revision,omitempty"`
	SessionPrincipalID string   `json:"session_principal_id"`
	SessionID          string   `json:"session_id"`
	UserID             string   `json:"user_id"`
	Text               string   `json:"text"`
	ArtifactRefs       []string `json:"artifact_refs,omitempty"`
}

func marshalAdmissionCommand(
	runtimeContext tenant.RuntimeContext,
	message gateway.Message,
) ([]byte, [sha256.Size]byte, error) {
	command, err := json.Marshal(admissionCommand{
		TenantID:           runtimeContext.TenantID,
		AppID:              runtimeContext.AppID,
		Channel:            runtimeContext.Channel,
		BindingID:          runtimeContext.BindingID,
		BindingRevision:    runtimeContext.BindingRevision,
		SessionPrincipalID: runtimeContext.SessionPrincipalID,
		SessionID:          runtimeContext.SessionID,
		UserID:             runtimeContext.UserID,
		Text:               message.Text,
		ArtifactRefs:       append([]string(nil), message.ArtifactRefs...),
	})
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("marshal admission command: %w", err)
	}
	return command, sha256.Sum256(command), nil
}

func admissionInsertError(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) &&
		(pgErr.ConstraintName == "channel_inbox_pkey" ||
			pgErr.ConstraintName == "execution_pkey" ||
			pgErr.ConstraintName == "execution_idempotency_idx") {
		return fmt.Errorf("%s: %w", operation, gateway.ErrIdempotencyConflict)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
