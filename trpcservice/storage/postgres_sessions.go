package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func (s *PostgresStateStore) GetSession(ctx context.Context, tenantID, sessionKey string) (Session, error) {
	if strings.TrimSpace(tenantID) == "" {
		return Session{}, fmt.Errorf("session tenant ID is required")
	}
	if strings.TrimSpace(sessionKey) == "" {
		return Session{}, fmt.Errorf("session key is required")
	}
	row := s.database.QueryRowContext(ctx, `
SELECT tenant_id, app_code, session_key, last_message_id, revision,
       subject_id, COALESCE(owner_platform_user_id,''), status, archived_at, updated_at
FROM sessions
WHERE tenant_id = $1 AND session_key = $2`, tenantID, sessionKey)
	var session Session
	if err := row.Scan(
		&session.TenantID, &session.AppCode, &session.SessionKey, &session.LastMessageID,
		&session.Revision, &session.SubjectID, &session.OwnerPlatformUserID,
		&session.Status, &session.ArchivedAt, &session.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Session{}, ErrSessionNotFound
		}
		return Session{}, fmt.Errorf("get session: %w", err)
	}
	conversations, err := s.listSessionConversations(ctx, tenantID, []string{sessionKey})
	if err != nil {
		return Session{}, err
	}
	session.Conversations = conversations[sessionKey]
	return session, nil
}

func (s *PostgresStateStore) ListSessions(ctx context.Context, tenantID string, limit int) ([]Session, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("session list tenant ID is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("session list limit must be positive")
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT tenant_id, app_code, session_key, last_message_id, revision,
       subject_id, COALESCE(owner_platform_user_id,''), status, archived_at, updated_at
FROM sessions
WHERE tenant_id = $1
ORDER BY updated_at DESC, session_key
LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}
	defer rows.Close()

	sessions := make([]Session, 0, limit)
	for rows.Next() {
		var session Session
		if err := rows.Scan(
			&session.TenantID, &session.AppCode, &session.SessionKey, &session.LastMessageID,
			&session.Revision, &session.SubjectID, &session.OwnerPlatformUserID,
			&session.Status, &session.ArchivedAt, &session.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan session: %w", err)
		}
		sessions = append(sessions, session)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sessions: %w", err)
	}
	keys := make([]string, 0, len(sessions))
	for _, session := range sessions {
		keys = append(keys, session.SessionKey)
	}
	conversations, err := s.listSessionConversations(ctx, tenantID, keys)
	if err != nil {
		return nil, err
	}
	for index := range sessions {
		sessions[index].Conversations = conversations[sessions[index].SessionKey]
	}
	return sessions, nil
}

func (s *PostgresStateStore) ListApplicationSessions(ctx context.Context, tenantID, appCode string) ([]Session, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
		return nil, errors.New("application Session catalog requires tenant and application")
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT tenant_id, app_code, session_key, last_message_id, revision,
       subject_id, COALESCE(owner_platform_user_id,''), status, archived_at, updated_at
FROM sessions
WHERE tenant_id = $1 AND app_code = $2
ORDER BY updated_at, session_key`, tenantID, appCode)
	if err != nil {
		return nil, fmt.Errorf("list application Sessions: %w", err)
	}
	defer rows.Close()
	result := make([]Session, 0)
	for rows.Next() {
		var current Session
		if err := rows.Scan(
			&current.TenantID, &current.AppCode, &current.SessionKey, &current.LastMessageID,
			&current.Revision, &current.SubjectID, &current.OwnerPlatformUserID,
			&current.Status, &current.ArchivedAt, &current.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan application Session: %w", err)
		}
		result = append(result, current)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate application Sessions: %w", err)
	}
	return result, nil
}

func (s *PostgresStateStore) EndChannelIdentityRoutes(ctx context.Context, tenantID, channel, bindingID, externalUserID string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(externalUserID) == "" {
		return fmt.Errorf("channel identity route is incomplete")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin channel route transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
UPDATE channel_conversations
SET ended_at=NOW(), updated_at=NOW()
WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND external_user_id=$4 AND ended_at IS NULL
RETURNING session_key`, tenantID, channel, bindingID, externalUserID)
	if err != nil {
		return fmt.Errorf("end channel identity routes: %w", err)
	}
	for rows.Next() {
		var sessionKey string
		if err := rows.Scan(&sessionKey); err != nil {
			rows.Close()
			return fmt.Errorf("scan ended channel route: %w", err)
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close ended channel routes: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit channel route transition: %w", err)
	}
	return nil
}

func (s *PostgresStateStore) ListClaimableSessions(ctx context.Context, tenantID, channel, bindingID, externalUserID string, limit int) ([]Session, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(externalUserID) == "" || limit <= 0 {
		return nil, fmt.Errorf("claimable session query is incomplete")
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT DISTINCT s.tenant_id, s.app_code, s.session_key, s.last_message_id, s.revision,
       s.subject_id, COALESCE(s.owner_platform_user_id,''), s.status, s.archived_at, s.updated_at
FROM sessions s
JOIN channel_conversations c ON c.tenant_id=s.tenant_id AND c.session_key=s.session_key
WHERE s.tenant_id=$1 AND s.owner_platform_user_id IS NULL
  AND c.channel_type=$2 AND c.binding_id=$3 AND c.external_user_id=$4 AND c.scope='direct'
ORDER BY s.updated_at DESC, s.session_key
LIMIT $5`, tenantID, channel, bindingID, externalUserID, limit)
	if err != nil {
		return nil, fmt.Errorf("list claimable sessions: %w", err)
	}
	defer rows.Close()
	result := make([]Session, 0)
	for rows.Next() {
		var session Session
		if err := rows.Scan(
			&session.TenantID, &session.AppCode, &session.SessionKey, &session.LastMessageID,
			&session.Revision, &session.SubjectID, &session.OwnerPlatformUserID, &session.Status,
			&session.ArchivedAt, &session.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan claimable session: %w", err)
		}
		result = append(result, session)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimable sessions: %w", err)
	}
	keys := make([]string, 0, len(result))
	for _, session := range result {
		keys = append(keys, session.SessionKey)
	}
	conversations, err := s.listSessionConversations(ctx, tenantID, keys)
	if err != nil {
		return nil, err
	}
	for index := range result {
		result[index].Conversations = conversations[result[index].SessionKey]
	}
	return result, nil
}

func (s *PostgresStateStore) ClaimSession(ctx context.Context, tenantID, sessionKey, platformUserID string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(sessionKey) == "" || strings.TrimSpace(platformUserID) == "" {
		return fmt.Errorf("session claim is incomplete")
	}
	result, err := s.database.ExecContext(ctx, `
UPDATE sessions
SET owner_platform_user_id=$3, updated_at=NOW()
WHERE tenant_id=$1 AND session_key=$2
  AND (owner_platform_user_id IS NULL OR owner_platform_user_id=$3)`, tenantID, sessionKey, platformUserID)
	if err != nil {
		return fmt.Errorf("claim session: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read session claim result: %w", err)
	}
	if updated > 0 {
		return nil
	}
	var owner sql.NullString
	err = s.database.QueryRowContext(ctx, `SELECT owner_platform_user_id FROM sessions WHERE tenant_id=$1 AND session_key=$2`, tenantID, sessionKey).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrSessionNotFound
	}
	if err != nil {
		return fmt.Errorf("read session after rejected claim: %w", err)
	}
	return ErrSessionOwnedByAnotherUser
}

func (s *PostgresStateStore) ListInboundMessageRoutes(ctx context.Context, tenantID, sessionKey string) ([]InboundMessageRoute, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(sessionKey) == "" {
		return nil, fmt.Errorf("message route tenant and session are required")
	}
	return s.listInboundMessageRoutes(ctx, `
SELECT tenant_id, app_code, channel_type, message_id, session_key, binding_id,
       external_conversation_id, scope, actor_external_user_id,
       COALESCE(actor_platform_user_id,''), trigger_type, created_at
FROM inbound_message_routes
WHERE tenant_id=$1 AND session_key=$2
ORDER BY created_at, message_id`, tenantID, sessionKey)
}

func (s *PostgresStateStore) ListInboundMessageRoutesByMessageIDs(ctx context.Context, tenantID, sessionKey string, messageIDs []string) ([]InboundMessageRoute, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(sessionKey) == "" {
		return nil, fmt.Errorf("message route tenant and session are required")
	}
	ids := make([]string, 0, len(messageIDs))
	seen := make(map[string]struct{}, len(messageIDs))
	for _, messageID := range messageIDs {
		messageID = strings.TrimSpace(messageID)
		if messageID == "" {
			continue
		}
		if _, ok := seen[messageID]; ok {
			continue
		}
		seen[messageID] = struct{}{}
		ids = append(ids, messageID)
	}
	if len(ids) == 0 {
		return []InboundMessageRoute{}, nil
	}
	args := make([]any, 0, len(ids)+2)
	args = append(args, tenantID, sessionKey)
	placeholders := make([]string, len(ids))
	for index, messageID := range ids {
		placeholders[index] = fmt.Sprintf("$%d", index+3)
		args = append(args, messageID)
	}
	query := `
SELECT tenant_id, app_code, channel_type, message_id, session_key, binding_id,
       external_conversation_id, scope, actor_external_user_id,
       COALESCE(actor_platform_user_id,''), trigger_type, created_at
FROM inbound_message_routes
WHERE tenant_id=$1 AND session_key=$2 AND message_id IN (` + strings.Join(placeholders, ",") + `)
ORDER BY created_at, message_id`
	return s.listInboundMessageRoutes(ctx, query, args...)
}

func (s *PostgresStateStore) listInboundMessageRoutes(ctx context.Context, query string, args ...any) ([]InboundMessageRoute, error) {
	rows, err := s.database.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list inbound message routes: %w", err)
	}
	defer rows.Close()
	result := make([]InboundMessageRoute, 0)
	for rows.Next() {
		var route InboundMessageRoute
		if err := rows.Scan(
			&route.TenantID, &route.AppCode, &route.Channel, &route.MessageID, &route.SessionKey,
			&route.BindingID, &route.ConversationID, &route.Scope, &route.ActorExternalUserID,
			&route.ActorPlatformUserID, &route.TriggerType, &route.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan inbound message route: %w", err)
		}
		result = append(result, route)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inbound message routes: %w", err)
	}
	return result, nil
}

func (s *PostgresStateStore) ResolveSession(ctx context.Context, route SessionRoute, preferred string) (string, error) {
	if err := validateSessionRoute(route, preferred); err != nil {
		return "", err
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin session routing transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var sessionKey, subjectID, ownerPlatformUserID string
	err = tx.QueryRowContext(ctx, `
SELECT conversation.session_key, session.subject_id, COALESCE(session.owner_platform_user_id,'')
FROM channel_conversations AS conversation
JOIN sessions AS session
  ON session.tenant_id=conversation.tenant_id AND session.session_key=conversation.session_key
WHERE conversation.tenant_id=$1 AND conversation.app_code=$2 AND conversation.channel_type=$3 AND conversation.binding_id=$4
  AND conversation.external_conversation_id=$5 AND conversation.ended_at IS NULL
FOR SHARE OF conversation, session`, route.TenantID, route.AppCode, route.Channel, route.BindingID, route.ConversationID).
		Scan(&sessionKey, &subjectID, &ownerPlatformUserID)
	if err == nil {
		if route.Scope == "group" {
			if _, err := tx.ExecContext(ctx, `
UPDATE sessions
SET subject_id=$3, owner_platform_user_id=NULL
WHERE tenant_id=$1 AND session_key=$2
  AND (subject_id<>$3 OR owner_platform_user_id IS NOT NULL)`, route.TenantID, sessionKey, route.SubjectID); err != nil {
				return "", fmt.Errorf("repair group session subject: %w", err)
			}
		} else {
			if ownerPlatformUserID != "" && route.OwnerPlatformUserID != "" && ownerPlatformUserID != route.OwnerPlatformUserID {
				return "", ErrSessionOwnedByAnotherUser
			}
			if subjectID != route.SubjectID || ownerPlatformUserID != route.OwnerPlatformUserID {
				_ = tx.Rollback()
				return s.resolveSessionUnderRouteLock(ctx, route, preferred)
			}
		}
		if err := tx.Commit(); err != nil {
			return "", fmt.Errorf("commit session route lookup: %w", err)
		}
		return sessionKey, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("read active channel conversation: %w", err)
	}
	_ = tx.Rollback()
	return s.resolveSessionUnderRouteLock(ctx, route, preferred)
}

func (s *PostgresStateStore) resolveSessionUnderRouteLock(ctx context.Context, route SessionRoute, preferred string) (string, error) {
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin locked session routing transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, sessionRouteAdvisoryLockKey(route)); err != nil {
		return "", fmt.Errorf("lock session route: %w", err)
	}

	var sessionKey, subjectID, ownerPlatformUserID string
	err = tx.QueryRowContext(ctx, `
SELECT conversation.session_key, session.subject_id, COALESCE(session.owner_platform_user_id,'')
FROM channel_conversations AS conversation
JOIN sessions AS session
  ON session.tenant_id=conversation.tenant_id AND session.session_key=conversation.session_key
WHERE conversation.tenant_id=$1 AND conversation.app_code=$2 AND conversation.channel_type=$3 AND conversation.binding_id=$4
  AND conversation.external_conversation_id=$5 AND conversation.ended_at IS NULL
FOR UPDATE OF conversation, session`, route.TenantID, route.AppCode, route.Channel, route.BindingID, route.ConversationID).
		Scan(&sessionKey, &subjectID, &ownerPlatformUserID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("read locked active channel conversation: %w", err)
	}
	if err == nil {
		if route.Scope == "group" {
			if _, err := tx.ExecContext(ctx, `
UPDATE sessions
SET subject_id=$3, owner_platform_user_id=NULL
WHERE tenant_id=$1 AND session_key=$2
  AND (subject_id<>$3 OR owner_platform_user_id IS NOT NULL)`, route.TenantID, sessionKey, route.SubjectID); err != nil {
				return "", fmt.Errorf("repair locked group session subject: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return "", fmt.Errorf("commit locked group session route: %w", err)
			}
			return sessionKey, nil
		}
		if ownerPlatformUserID != "" && route.OwnerPlatformUserID != "" && ownerPlatformUserID != route.OwnerPlatformUserID {
			return "", ErrSessionOwnedByAnotherUser
		}
		if subjectID == route.SubjectID && ownerPlatformUserID == route.OwnerPlatformUserID {
			if err := tx.Commit(); err != nil {
				return "", fmt.Errorf("commit locked session route lookup: %w", err)
			}
			return sessionKey, nil
		}
		if route.OwnerPlatformUserID != "" {
			if err := claimChannelIdentityHistoryTx(ctx, tx, route); err != nil {
				return "", err
			}
			if subjectID == route.SubjectID && ownerPlatformUserID == "" {
				if err := tx.Commit(); err != nil {
					return "", fmt.Errorf("commit claimed session route: %w", err)
				}
				return sessionKey, nil
			}
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE channel_conversations
SET ended_at=NOW(), updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2 AND channel_type=$3 AND binding_id=$4
  AND external_conversation_id=$5 AND session_key=$6 AND ended_at IS NULL`,
			route.TenantID, route.AppCode, route.Channel, route.BindingID, route.ConversationID, sessionKey); err != nil {
			return "", fmt.Errorf("close stale subject channel route: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE sessions AS session
SET status='archived', archived_at=NOW(), updated_at=NOW()
WHERE session.tenant_id=$1 AND session.session_key=$2 AND session.status='active'
  AND NOT EXISTS (
    SELECT 1 FROM channel_conversations AS conversation
    WHERE conversation.tenant_id=session.tenant_id AND conversation.session_key=session.session_key
      AND conversation.ended_at IS NULL
  )`, route.TenantID, sessionKey); err != nil {
			return "", fmt.Errorf("archive stale subject session: %w", err)
		}
	} else if route.Scope == "direct" && route.OwnerPlatformUserID != "" {
		if err := claimChannelIdentityHistoryTx(ctx, tx, route); err != nil {
			return "", err
		}
	}

	sessionKey = preferred
	if _, err := tx.ExecContext(ctx, `
INSERT INTO sessions (tenant_id, app_code, session_key, last_message_id, subject_id, owner_platform_user_id, status)
VALUES ($1,$2,$3,'',$4,NULLIF($5,''),'active')
ON CONFLICT (tenant_id, session_key) DO UPDATE
SET subject_id = CASE WHEN sessions.subject_id = '' THEN EXCLUDED.subject_id ELSE sessions.subject_id END,
    owner_platform_user_id = COALESCE(sessions.owner_platform_user_id, EXCLUDED.owner_platform_user_id)`,
		route.TenantID, route.AppCode, sessionKey, route.SubjectID, route.OwnerPlatformUserID); err != nil {
		return "", fmt.Errorf("create session index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO channel_conversations (
    tenant_id, app_code, channel_type, binding_id, external_conversation_id, external_user_id,
    session_key, scope, started_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW(),NOW())`, route.TenantID, route.AppCode, route.Channel, route.BindingID,
		route.ConversationID, route.ExternalUserID, sessionKey, route.Scope); err != nil {
		return "", fmt.Errorf("create channel conversation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit locked session routing: %w", err)
	}
	return sessionKey, nil
}

func claimChannelIdentityHistoryTx(ctx context.Context, tx *sql.Tx, route SessionRoute) error {
	if route.Scope != "direct" || strings.TrimSpace(route.OwnerPlatformUserID) == "" || strings.TrimSpace(route.ExternalUserID) == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE sessions AS session
SET owner_platform_user_id=$6
FROM channel_conversations AS conversation
WHERE conversation.tenant_id=session.tenant_id AND conversation.session_key=session.session_key
  AND conversation.tenant_id=$1 AND conversation.app_code=$2 AND conversation.channel_type=$3
  AND conversation.binding_id=$4 AND conversation.external_user_id=$5 AND conversation.scope='direct'
  AND session.owner_platform_user_id IS NULL`, route.TenantID, route.AppCode, route.Channel, route.BindingID,
		route.ExternalUserID, route.OwnerPlatformUserID); err != nil {
		return fmt.Errorf("claim channel identity session history: %w", err)
	}
	return nil
}

// SwitchSession atomically closes the current channel route, creates a fresh
// platform Session, activates the new route and enqueues the command reply.
// Re-delivery of the same provider command reuses the committed result through
// the outbox request_id unique key and never rotates the session twice.
func (s *PostgresStateStore) SwitchSession(ctx context.Context, request SessionSwitchRequest) (SessionSwitchResult, error) {
	if err := ctx.Err(); err != nil {
		return SessionSwitchResult{}, err
	}
	if err := validateSessionSwitchRequest(request); err != nil {
		return SessionSwitchResult{}, err
	}
	eventID, err := newStateID()
	if err != nil {
		return SessionSwitchResult{}, err
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return SessionSwitchResult{}, fmt.Errorf("begin session switch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existingSession, existingType string
	err = tx.QueryRowContext(ctx, `
SELECT aggregate_key, event_type
FROM outbox_events
WHERE tenant_id=$1 AND request_id=$2
FOR UPDATE`, request.Route.TenantID, request.RequestID).Scan(&existingSession, &existingType)
	if err == nil {
		if existingType != request.OutboxType {
			return SessionSwitchResult{}, fmt.Errorf("session command request ID already belongs to %q", existingType)
		}
		if err := tx.Commit(); err != nil {
			return SessionSwitchResult{}, fmt.Errorf("commit replayed session switch: %w", err)
		}
		return SessionSwitchResult{SessionKey: existingSession, Replayed: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SessionSwitchResult{}, fmt.Errorf("read existing session command: %w", err)
	}

	lockKey := sessionRouteAdvisoryLockKey(request.Route)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return SessionSwitchResult{}, fmt.Errorf("lock session route: %w", err)
	}

	var previous string
	err = tx.QueryRowContext(ctx, `
SELECT session_key
FROM channel_conversations
WHERE tenant_id=$1 AND app_code=$2 AND channel_type=$3 AND binding_id=$4
  AND external_conversation_id=$5 AND ended_at IS NULL
FOR UPDATE`, request.Route.TenantID, request.Route.AppCode, request.Route.Channel,
		request.Route.BindingID, request.Route.ConversationID).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return SessionSwitchResult{}, fmt.Errorf("lock active session route: %w", err)
	}
	if err == nil {
		if _, err := tx.ExecContext(ctx, `
UPDATE channel_conversations
SET ended_at=NOW(), updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2 AND channel_type=$3 AND binding_id=$4
  AND external_conversation_id=$5 AND session_key=$6 AND ended_at IS NULL`,
			request.Route.TenantID, request.Route.AppCode, request.Route.Channel, request.Route.BindingID,
			request.Route.ConversationID, previous); err != nil {
			return SessionSwitchResult{}, fmt.Errorf("close previous session route: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO sessions (
    tenant_id, app_code, session_key, last_message_id, subject_id,
    owner_platform_user_id, status, updated_at
) VALUES ($1,$2,$3,'',$4,NULLIF($5,''),'active',NOW())`,
		request.Route.TenantID, request.Route.AppCode, request.SessionKey,
		request.Route.SubjectID, request.Route.OwnerPlatformUserID); err != nil {
		return SessionSwitchResult{}, fmt.Errorf("create switched session: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO channel_conversations (
    tenant_id, app_code, channel_type, binding_id, external_conversation_id,
    external_user_id, session_key, scope, started_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW(),NOW())`,
		request.Route.TenantID, request.Route.AppCode, request.Route.Channel, request.Route.BindingID,
		request.Route.ConversationID, request.Route.ExternalUserID, request.SessionKey, request.Route.Scope); err != nil {
		return SessionSwitchResult{}, fmt.Errorf("activate switched session route: %w", err)
	}
	if previous != "" {
		if _, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status='archived', archived_at=NOW(), updated_at=NOW()
WHERE tenant_id=$1 AND session_key=$2
  AND NOT EXISTS (
      SELECT 1 FROM channel_conversations c
      WHERE c.tenant_id=$1 AND c.session_key=$2 AND c.ended_at IS NULL
  )`, request.Route.TenantID, previous); err != nil {
			return SessionSwitchResult{}, fmt.Errorf("archive previous session: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO outbox_events (
    id, request_id, tenant_id, aggregate_key, event_type, payload, available_at, created_at
) VALUES ($1,$2,$3,$4,$5,$6::jsonb,NOW(),NOW())`,
		eventID, request.RequestID, request.Route.TenantID, request.SessionKey,
		request.OutboxType, string(request.OutboxPayload)); err != nil {
		return SessionSwitchResult{}, fmt.Errorf("enqueue session switch reply: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return SessionSwitchResult{}, fmt.Errorf("commit session switch: %w", err)
	}
	return SessionSwitchResult{SessionKey: request.SessionKey}, nil
}

func sessionRouteAdvisoryLockKey(route SessionRoute) string {
	// PostgreSQL text parameters cannot contain NUL bytes, so hash the canonical
	// route tuple before passing it to hashtextextended().
	digest := sha256.Sum256([]byte(strings.Join([]string{
		route.TenantID, route.AppCode, route.Channel, route.BindingID, route.ConversationID,
	}, "\x00")))
	return fmt.Sprintf("%x", digest[:])
}

func (s *PostgresStateStore) ArchiveSession(ctx context.Context, tenantID, sessionKey string) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("archive tenant ID is required")
	}
	if strings.TrimSpace(sessionKey) == "" {
		return fmt.Errorf("archive session key is required")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin session archive: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE sessions SET status='archived', archived_at=NOW(), updated_at=NOW()
WHERE tenant_id=$1 AND session_key=$2`, tenantID, sessionKey)
	if err != nil {
		return fmt.Errorf("archive session: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read archived session result: %w", err)
	}
	if rows == 0 {
		return ErrSessionNotFound
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE channel_conversations
SET ended_at=NOW(), updated_at=NOW()
WHERE tenant_id=$1 AND session_key=$2 AND ended_at IS NULL`, tenantID, sessionKey); err != nil {
		return fmt.Errorf("close channel conversations: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit session archive: %w", err)
	}
	return nil
}

func (s *PostgresStateStore) listSessionConversations(ctx context.Context, tenantID string, sessionKeys []string) (map[string][]SessionConversation, error) {
	keys := make([]string, 0, len(sessionKeys))
	seen := make(map[string]struct{}, len(sessionKeys))
	for _, sessionKey := range sessionKeys {
		sessionKey = strings.TrimSpace(sessionKey)
		if sessionKey == "" {
			continue
		}
		if _, exists := seen[sessionKey]; exists {
			continue
		}
		seen[sessionKey] = struct{}{}
		keys = append(keys, sessionKey)
	}
	result := make(map[string][]SessionConversation, len(keys))
	if len(keys) == 0 {
		return result, nil
	}
	args := make([]any, 0, len(keys)+1)
	args = append(args, tenantID)
	placeholders := make([]string, len(keys))
	for index, sessionKey := range keys {
		placeholders[index] = fmt.Sprintf("$%d", index+2)
		args = append(args, sessionKey)
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT tenant_id, app_code, session_key, channel_type, binding_id,
       external_conversation_id, external_user_id, scope, started_at, updated_at, ended_at
FROM channel_conversations
WHERE tenant_id=$1 AND session_key IN (`+strings.Join(placeholders, ",")+`)
ORDER BY updated_at DESC, channel_type, external_conversation_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("list session conversations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var conversation SessionConversation
		if err := rows.Scan(
			&conversation.TenantID, &conversation.AppCode, &conversation.SessionKey,
			&conversation.Channel, &conversation.BindingID, &conversation.ConversationID,
			&conversation.ExternalUserID, &conversation.Scope, &conversation.StartedAt,
			&conversation.UpdatedAt, &conversation.EndedAt,
		); err != nil {
			return nil, fmt.Errorf("scan session conversation: %w", err)
		}
		result[conversation.SessionKey] = append(result[conversation.SessionKey], conversation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session conversations: %w", err)
	}
	return result, nil
}
