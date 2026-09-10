package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisSessionStore is the shared login-session backend for multi-node deployments.
type RedisSessionStore struct {
	client redis.UniversalClient
}

func NewRedisSessionStore(client redis.UniversalClient) (*RedisSessionStore, error) {
	if client == nil {
		return nil, errors.New("Redis client is required")
	}
	return &RedisSessionStore{client: client}, nil
}

func (s *RedisSessionStore) Create(ctx context.Context, user SessionUser, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", errors.New("session TTL must be positive")
	}
	if strings.TrimSpace(user.PlatformUserID) == "" {
		return "", errors.New("session payload missing identity")
	}
	sessionID, err := newSessionID()
	if err != nil {
		return "", err
	}
	payload, err := encodeSessionUser(user)
	if err != nil {
		return "", err
	}
	userSessionsKey := "dsh_user_sessions:" + user.PlatformUserID
	if _, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, "dsh_session:"+sessionID, payload, ttl)
		pipe.SAdd(ctx, userSessionsKey, sessionID)
		pipe.Expire(ctx, userSessionsKey, ttl)
		return nil
	}); err != nil {
		return "", fmt.Errorf("store session: %w", err)
	}
	return sessionID, nil
}

func (s *RedisSessionStore) Get(ctx context.Context, sessionID string) (SessionUser, error) {
	if sessionID == "" {
		return SessionUser{}, ErrSessionNotFound
	}
	payload, err := s.client.Get(ctx, "dsh_session:"+sessionID).Result()
	if errors.Is(err, redis.Nil) {
		return SessionUser{}, ErrSessionNotFound
	}
	if err != nil {
		return SessionUser{}, fmt.Errorf("read session: %w", err)
	}
	user, err := decodeSessionUser(payload)
	if err != nil {
		return SessionUser{}, err
	}
	return user, nil
}

func (s *RedisSessionStore) Delete(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	key := "dsh_session:" + sessionID
	payload, getErr := s.client.Get(ctx, key).Result()
	if getErr != nil && !errors.Is(getErr, redis.Nil) {
		return fmt.Errorf("read session before delete: %w", getErr)
	}
	userID := ""
	if getErr == nil {
		if user, err := decodeSessionUser(payload); err == nil {
			userID = user.PlatformUserID
		}
	}
	if _, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, key)
		if userID != "" {
			pipe.SRem(ctx, "dsh_user_sessions:"+userID, sessionID)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	return nil
}

func (s *RedisSessionStore) DeleteForUser(ctx context.Context, platformUserID string) error {
	platformUserID = strings.TrimSpace(platformUserID)
	if platformUserID == "" {
		return nil
	}
	indexKey := "dsh_user_sessions:" + platformUserID
	sessions, err := s.client.SMembers(ctx, indexKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("list user sessions: %w", err)
	}
	keys := make([]string, 0, len(sessions)+1)
	for _, sessionID := range sessions {
		if sessionID != "" {
			keys = append(keys, "dsh_session:"+sessionID)
		}
	}
	keys = append(keys, indexKey)
	if err := s.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("delete user sessions: %w", err)
	}
	return nil
}

type sessionUserJSON struct {
	PlatformUserID string `json:"platform_user_id"`
	DisplayName    string `json:"display_name"`
	Email          string `json:"email"`
	Role           string `json:"role"`
	IsSystemAdmin  bool   `json:"is_system_admin"`
	Tenants        []struct {
		TenantID                 string `json:"tenant_id"`
		DisplayName              string `json:"display_name"`
		Role                     string `json:"role"`
		Status                   string `json:"status"`
		ConversationContentAudit bool   `json:"conversation_content_audit"`
	} `json:"tenants"`
}

func encodeSessionUser(user SessionUser) (string, error) {
	encoded := sessionUserJSON{
		PlatformUserID: user.PlatformUserID, DisplayName: user.DisplayName, Email: user.Email, Role: string(user.Role), IsSystemAdmin: user.IsSystemAdmin,
	}
	for _, membership := range user.Tenants {
		encoded.Tenants = append(encoded.Tenants, struct {
			TenantID                 string `json:"tenant_id"`
			DisplayName              string `json:"display_name"`
			Role                     string `json:"role"`
			Status                   string `json:"status"`
			ConversationContentAudit bool   `json:"conversation_content_audit"`
		}{
			TenantID: membership.TenantID, DisplayName: membership.DisplayName, Role: string(membership.Role), Status: membership.Status,
			ConversationContentAudit: membership.ConversationContentAudit,
		})
	}
	bytes, err := json.Marshal(encoded)
	if err != nil {
		return "", fmt.Errorf("encode session user: %w", err)
	}
	return string(bytes), nil
}

func decodeSessionUser(payload string) (SessionUser, error) {
	var encoded sessionUserJSON
	if err := json.Unmarshal([]byte(payload), &encoded); err != nil {
		return SessionUser{}, fmt.Errorf("decode session user: %w", err)
	}
	if strings.TrimSpace(encoded.PlatformUserID) == "" {
		return SessionUser{}, errors.New("session payload missing identity")
	}
	user := SessionUser{
		PlatformUserID: encoded.PlatformUserID, DisplayName: encoded.DisplayName, Email: encoded.Email, Role: Role(encoded.Role), IsSystemAdmin: encoded.IsSystemAdmin,
	}
	for _, membership := range encoded.Tenants {
		user.Tenants = append(user.Tenants, TenantRole{
			TenantID: membership.TenantID, DisplayName: membership.DisplayName, Role: Role(membership.Role), Status: membership.Status,
			ConversationContentAudit: membership.ConversationContentAudit,
		})
	}
	return user, nil
}

var _ SessionStore = (*RedisSessionStore)(nil)
