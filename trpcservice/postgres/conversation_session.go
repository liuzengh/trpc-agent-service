package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const newSessionCommandEventID = "platform_command:new_session"

// ensureActiveSessionTx creates the first active pointer with the historical
// default lane, then locks the exact tenant/app/binding/principal row. The
// pointer row is the serialization boundary shared by ordinary admission and
// /new; it is never kept in process memory.
func ensureActiveSessionTx(
	ctx context.Context,
	tx pgx.Tx,
	scope tenant.Scope,
	bindingID, sessionPrincipalID string,
) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", fmt.Errorf("active session scope: %w", err)
	}
	if bindingID == "" {
		return "", errors.New("active session binding_id is required")
	}
	if sessionPrincipalID == "" {
		return "", errors.New("active session principal is required")
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.conversation_session (
    tenant_id, app_id, binding_id, session_principal_id,
    active_session_id, version
) VALUES ($1, $2, $3, $4, $5, 1)
ON CONFLICT (tenant_id, app_id, binding_id, session_principal_id) DO NOTHING`,
		scope.TenantID,
		scope.AppID,
		bindingID,
		sessionPrincipalID,
		channels.DefaultSessionID,
	); err != nil {
		return "", fmt.Errorf("create active session pointer: %w", err)
	}
	var sessionID string
	if err := tx.QueryRow(ctx, `
SELECT active_session_id
FROM platform.conversation_session
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND session_principal_id = $4
FOR UPDATE`,
		scope.TenantID,
		scope.AppID,
		bindingID,
		sessionPrincipalID,
	).Scan(&sessionID); err != nil {
		return "", fmt.Errorf("lock active session pointer: %w", err)
	}
	if sessionID == "" {
		return "", errors.New("active session pointer is empty")
	}
	return sessionID, nil
}

func currentSessionIDTx(
	ctx context.Context,
	tx pgx.Tx,
	scope tenant.Scope,
	bindingID, sessionPrincipalID string,
) (string, error) {
	var sessionID string
	err := tx.QueryRow(ctx, `
SELECT active_session_id
FROM platform.conversation_session
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND session_principal_id = $4`,
		scope.TenantID,
		scope.AppID,
		bindingID,
		sessionPrincipalID,
	).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return channels.DefaultSessionID, nil
	}
	if err != nil {
		return "", fmt.Errorf("resolve active session pointer: %w", err)
	}
	if sessionID == "" {
		return "", errors.New("active session pointer is empty")
	}
	return sessionID, nil
}

func switchActiveSessionTx(
	ctx context.Context,
	tx pgx.Tx,
	scope tenant.Scope,
	bindingID, sessionPrincipalID string,
) (string, error) {
	if _, err := ensureActiveSessionTx(ctx, tx, scope, bindingID, sessionPrincipalID); err != nil {
		return "", err
	}
	newSessionID := uuid.NewString()
	tag, err := tx.Exec(ctx, `
UPDATE platform.conversation_session
SET active_session_id = $5,
    version = version + 1,
    updated_at = clock_timestamp()
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND session_principal_id = $4`,
		scope.TenantID,
		scope.AppID,
		bindingID,
		sessionPrincipalID,
		newSessionID,
	)
	if err != nil {
		return "", fmt.Errorf("switch active session: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return "", errors.New("switch active session: pointer is unavailable")
	}
	return newSessionID, nil
}

// ResolveActiveSession reads the durable active pointer for one exact scope.
// It does not create a row; ordinary channel admission creates the pointer on
// first use inside its own transaction.
func (s *Store) ResolveActiveSession(
	ctx context.Context,
	scope tenant.Scope,
	bindingID, sessionPrincipalID string,
) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if bindingID == "" {
		return "", errors.New("binding_id is required")
	}
	if sessionPrincipalID == "" {
		return "", errors.New("session_principal_id is required")
	}
	var sessionID string
	err := s.pool.QueryRow(ctx, `
SELECT active_session_id
FROM platform.conversation_session
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND session_principal_id = $4`,
		scope.TenantID,
		scope.AppID,
		bindingID,
		sessionPrincipalID,
	).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("resolve active session: %w", err)
	}
	if sessionID == "" {
		return "", errors.New("active session pointer is empty")
	}
	return sessionID, nil
}

// HandleNewSession is the PostgreSQL platform command handler for /new. It
// maps the provider principal, applies the existing IM access policy, switches
// the active pointer, and enqueues the acknowledgement in the existing Reply
// Outbox without creating an execution or dispatch record.
func (s *Store) HandleNewSession(
	ctx context.Context,
	request channels.NewSessionRequest,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if s.identityMapper == nil {
		return errors.New("channel identity mapper is required")
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	input := request.Input
	mapping, ok := input.MappingInput()
	if !ok {
		return errors.New("new session channel mapping is required")
	}
	scope := tenant.Scope{TenantID: input.TenantID, AppID: input.AppID}
	mappingRequest, err := normalizeIdentityMappingRequest(IdentityMappingRequest{
		Scope:                      scope,
		BindingID:                  input.BindingID,
		Channel:                    input.Channel,
		Kind:                       input.Conversation.Kind,
		ExternalSenderID:           mapping.ExternalSenderID,
		ExternalChatID:             mapping.ExternalChatID,
		ExternalThreadID:           mapping.ExternalThreadID,
		ProviderSenderTarget:       mapping.ProviderSenderTarget,
		ProviderConversationTarget: mapping.ProviderConversationTarget,
		ProviderThreadTarget:       mapping.ProviderThreadTarget,
	})
	if err != nil {
		return err
	}
	payloadHash, err := s.identityMapper.channelPayloadHash(ctx, mappingRequest, input)
	if err != nil {
		return err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin new session command: %w", err)
	}
	defer func() { rollback(tx) }()

	tnt, err := lockTenant(ctx, tx, scope.TenantID)
	if err != nil {
		return err
	}
	if tnt.Status != tenant.StatusActive {
		return fmt.Errorf("new session tenant: %w", gateway.ErrAdmissionDraining)
	}
	app, err := lockAgentApp(ctx, tx, scope.TenantID, scope.AppID)
	if err != nil {
		return err
	}
	if app.Status != tenant.StatusActive {
		return gateway.ErrAdmissionDraining
	}
	if err := validateNewSessionBindingTx(ctx, tx, input); err != nil {
		return err
	}
	inbox, found, err := findChannelInbox(
		ctx, tx, input.TenantID, input.AppID, input.BindingID, input.ExternalMessageID,
	)
	if err != nil {
		return err
	}
	if found {
		if !bytes.Equal(payloadHash[:], inbox.PayloadHash) {
			return gateway.ErrIdempotencyConflict
		}
		commandReply, err := findNewSessionCommandReplyTx(ctx, tx, inbox.RequestID, input)
		if err != nil {
			return err
		}
		if !commandReply {
			return gateway.ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit replayed new session command: %w", err)
		}
		return nil
	}

	mapped, err := s.identityMapper.mapInTransaction(ctx, tx, mappingRequest)
	if err != nil {
		return err
	}
	commandRequest := gateway.AdmissionRequest{
		RequestID:      request.RequestID,
		IdempotencyKey: input.ExternalMessageID,
		ChannelInput:   &input,
		Identity: gateway.AdmissionIdentity{Tenant: tenant.RuntimeContext{
			TenantID:        input.TenantID,
			AppID:           input.AppID,
			Channel:         string(input.Channel),
			BindingID:       input.BindingID,
			BindingRevision: input.BindingRevision,
		}},
	}
	mapped.SessionID, err = currentSessionIDTx(
		ctx, tx, scope, input.BindingID, mapped.SessionPrincipalID,
	)
	if err != nil {
		return err
	}
	runtimeContext := commandRequest.Identity.Tenant
	runtimeContext.SessionPrincipalID = mapped.SessionPrincipalID
	runtimeContext.SessionID = mapped.SessionID
	runtimeContext.UserID = mapped.Identity.UserID
	activeConfig, err := resolveAppConfigFrom(ctx, tx, app.TenantID, app.AppID, app.ActiveConfigVersion)
	if err != nil {
		return err
	}
	_, appConfig, err := resolveAdmissionConfig(ctx, tx, app, runtimeContext, activeConfig)
	if err != nil {
		return err
	}
	if !allowsNewSession(appConfig.IMAccess, input.Conversation.Kind, mapped) {
		if err := insertChannelInbox(
			ctx,
			tx,
			commandRequest,
			payloadHash[:],
			channelInboxStatusRejected,
			"IM_ACCESS_DENIED",
			nil,
			nil,
		); err != nil {
			return admissionInsertError("insert denied new session inbox", err)
		}
		reply := newSessionCommandReply(input, mapped, request.RequestID, channels.NewSessionFailureReply)
		if err := insertReplyOutboxTxWithSourceKind(
			ctx,
			tx,
			input.TenantID,
			input.AppID,
			input.BindingID,
			request.RequestID,
			"channel_command",
			[]channels.Reply{reply},
		); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit denied new session command: %w", err)
		}
		return nil
	}

	if _, err := switchActiveSessionTx(
		ctx,
		tx,
		scope,
		input.BindingID,
		mapped.SessionPrincipalID,
	); err != nil {
		return err
	}
	if err := insertChannelInbox(
		ctx,
		tx,
		commandRequest,
		payloadHash[:],
		channelInboxStatusAdmitted,
		"",
		nil,
		nil,
	); err != nil {
		return admissionInsertError("insert new session inbox", err)
	}
	reply := newSessionCommandReply(input, mapped, request.RequestID, channels.NewSessionSuccessReply)
	if err := insertReplyOutboxTxWithSourceKind(
		ctx,
		tx,
		input.TenantID,
		input.AppID,
		input.BindingID,
		request.RequestID,
		"channel_command",
		[]channels.Reply{reply},
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit new session command: %w", err)
	}
	return nil
}

func validateNewSessionBindingTx(
	ctx context.Context,
	tx pgx.Tx,
	input channels.ChannelInput,
) error {
	binding, err := lockChannelBinding(ctx, tx, input.TenantID, input.AppID, input.BindingID)
	if err != nil {
		return err
	}
	if binding.Channel != input.Channel {
		return channels.ErrBindingChannelMismatch
	}
	if binding.Status != channels.BindingActive {
		return channels.ErrBindingInactive
	}
	if binding.BindingRevision != input.BindingRevision {
		return gateway.ErrChannelBindingSnapshotStale
	}
	return nil
}

func allowsNewSession(
	policy tenant.IMAccessPolicy,
	kind channels.ConversationKind,
	mapped channels.MappedPrincipal,
) bool {
	if mapped.Identity.UserID == "" {
		return false
	}
	if kind == channels.ConversationDirect {
		return policy.Allows(mapped.Identity.UserID, "")
	}
	// A group/topic reset affects every member. An empty policy therefore
	// denies it, and a conversation allowlist alone is not command authority.
	for _, userID := range policy.AllowedUsers {
		if userID == mapped.Identity.UserID {
			return true
		}
	}
	return false
}

func newSessionCommandReply(
	input channels.ChannelInput,
	mapped channels.MappedPrincipal,
	requestID, text string,
) channels.Reply {
	target := channels.ReplyTarget{
		Kind:             channels.TargetKindUser,
		InternalEntityID: mapped.Identity.UserID,
	}
	if mapped.Conversation != nil {
		target = channels.ReplyTarget{
			Kind:             channels.TargetKindConversation,
			InternalEntityID: mapped.Conversation.ConversationID,
		}
		if mapped.Conversation.Scope == channels.ConversationScopeTopic {
			target.Kind = channels.TargetKindTopic
		}
	}
	return channels.Reply{
		TenantID:        input.TenantID,
		AppID:           input.AppID,
		RequestID:       requestID,
		SourceEventID:   newSessionCommandEventID,
		Channel:         input.Channel,
		BindingID:       input.BindingID,
		BindingRevision: input.BindingRevision,
		Revision:        1,
		Target:          target,
		Text:            text,
	}
}

func findNewSessionCommandReplyTx(
	ctx context.Context,
	tx pgx.Tx,
	requestID string,
	input channels.ChannelInput,
) (bool, error) {
	var found bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM platform.reply_outbox
    WHERE tenant_id = $1
      AND app_id = $2
      AND binding_id = $3
      AND request_id = $4
      AND source_event_id = $5
      AND revision = 1
      AND source_kind = 'channel_command'
)`, input.TenantID, input.AppID, input.BindingID, requestID, newSessionCommandEventID).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("find new session command reply: %w", err)
	}
	return found, nil
}

var _ channels.NewSessionHandler = (*Store)(nil)
