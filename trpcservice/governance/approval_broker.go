package governance

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrApprovalExpired       = errors.New("approval request expired")
	ErrApprovalRouteMismatch = errors.New("approval route mismatch")
	ErrApprovalResolved      = errors.New("approval request already resolved")
)

const approvalKeyPrefix = "trpc:approval:"

type ApprovalRequest struct {
	TenantID           string
	AppCode            string
	ConfigVersion      uint64
	Channel            string
	BindingID          string
	ConversationID     string
	ConversationScope  string
	ExternalUserID     string
	ProgressMessageID  string
	ProviderReplyToken string
	ToolName           string
	ToolDescription    string
}

type PendingApproval struct {
	Token              string
	TenantID           string
	AppCode            string
	ConfigVersion      uint64
	Channel            string
	BindingID          string
	ConversationID     string
	ConversationScope  string
	ExternalUserID     string
	ProgressMessageID  string
	ProviderReplyToken string
	ToolName           string
	ToolDescription    string
	NotificationID     string
}

type ApprovalResolution struct {
	Token          string
	TenantID       string
	Channel        string
	BindingID      string
	ConversationID string
	ExternalUserID string
	Approved       bool
}

type ApprovalRequester interface {
	Request(context.Context, ApprovalRequest) (bool, error)
}

type ApprovalBroker interface {
	ApprovalRequester
	ListPending(context.Context, int) ([]PendingApproval, error)
	MarkNotified(context.Context, string, string) error
	Resolve(context.Context, ApprovalResolution) (PendingApproval, error)
}

type RedisApprovalBroker struct {
	client redis.UniversalClient
	ttl    time.Duration
}

func NewRedisApprovalBroker(client redis.UniversalClient, ttl time.Duration) (*RedisApprovalBroker, error) {
	if client == nil {
		return nil, errors.New("approval broker Redis client is required")
	}
	if ttl <= 0 {
		return nil, errors.New("approval broker TTL must be positive")
	}
	return &RedisApprovalBroker{client: client, ttl: ttl}, nil
}

func (b *RedisApprovalBroker) Request(ctx context.Context, request ApprovalRequest) (bool, error) {
	if err := validateApprovalRequest(request); err != nil {
		return false, err
	}
	token := uuid.NewString()
	key := approvalKeyPrefix + token
	values := map[string]any{
		"tenant_id": request.TenantID, "app_code": request.AppCode, "config_version": request.ConfigVersion,
		"channel": request.Channel, "binding_id": request.BindingID, "conversation_id": request.ConversationID,
		"conversation_scope": request.ConversationScope, "external_user_id": request.ExternalUserID,
		"progress_message_id": strings.TrimSpace(request.ProgressMessageID), "provider_reply_token": strings.TrimSpace(request.ProviderReplyToken),
		"tool_name": request.ToolName, "tool_description": strings.TrimSpace(request.ToolDescription),
		"status": "pending", "notification_id": "",
	}
	if _, err := b.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, key, values)
		pipe.Expire(ctx, key, b.ttl)
		return nil
	}); err != nil {
		return false, fmt.Errorf("create approval request: %w", err)
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := b.client.HGet(ctx, key, "status").Result()
		switch {
		case errors.Is(err, redis.Nil):
			return false, ErrApprovalExpired
		case err != nil:
			return false, fmt.Errorf("read approval decision: %w", err)
		case status == "approved":
			return true, nil
		case status == "rejected":
			return false, nil
		case status != "pending":
			return false, fmt.Errorf("invalid approval status %q", status)
		}
		select {
		case <-ctx.Done():
			b.cancelPending(key)
			return false, ctx.Err()
		case <-ticker.C:
		}
	}
}

var cancelPendingApprovalScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HGET', KEYS[1], 'status') ~= 'pending' then return 2 end
redis.call('DEL', KEYS[1])
return 1
`)

func (b *RedisApprovalBroker) cancelPending(key string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = cancelPendingApprovalScript.Run(cleanupCtx, b.client, []string{key}).Err()
}

func (b *RedisApprovalBroker) ListPending(ctx context.Context, limit int) ([]PendingApproval, error) {
	if limit <= 0 {
		return nil, errors.New("approval pending limit must be positive")
	}
	results := make([]PendingApproval, 0, limit)
	var cursor uint64
	for len(results) < limit {
		keys, next, err := b.client.Scan(ctx, cursor, approvalKeyPrefix+"*", int64(limit*2)).Result()
		if err != nil {
			return nil, fmt.Errorf("scan pending approvals: %w", err)
		}
		for _, key := range keys {
			fields, err := b.client.HGetAll(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("read pending approval: %w", err)
			}
			if fields["status"] != "pending" || fields["notification_id"] != "" {
				continue
			}
			pending, err := decodePendingApproval(strings.TrimPrefix(key, approvalKeyPrefix), fields)
			if err != nil {
				continue
			}
			results = append(results, pending)
			if len(results) == limit {
				break
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return results, nil
}

var markApprovalNotifiedScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HGET', KEYS[1], 'status') ~= 'pending' then return 2 end
if redis.call('HGET', KEYS[1], 'notification_id') ~= '' then return 3 end
redis.call('HSET', KEYS[1], 'notification_id', ARGV[1])
return 1
`)

func (b *RedisApprovalBroker) MarkNotified(ctx context.Context, token, notificationID string) error {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(notificationID) == "" {
		return errors.New("approval token and notification ID are required")
	}
	result, err := markApprovalNotifiedScript.Run(ctx, b.client, []string{approvalKeyPrefix + token}, notificationID).Int64()
	if err != nil {
		return fmt.Errorf("mark approval notified: %w", err)
	}
	switch result {
	case 1, 2, 3:
		return nil
	case 0:
		return ErrApprovalExpired
	default:
		return fmt.Errorf("unexpected approval notification result %d", result)
	}
}

var resolveApprovalScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HGET', KEYS[1], 'status') ~= 'pending' then return 2 end
if redis.call('HGET', KEYS[1], 'tenant_id') ~= ARGV[1] then return 3 end
if redis.call('HGET', KEYS[1], 'channel') ~= ARGV[2] then return 3 end
if redis.call('HGET', KEYS[1], 'binding_id') ~= ARGV[3] then return 3 end
if redis.call('HGET', KEYS[1], 'conversation_id') ~= ARGV[4] then return 3 end
if redis.call('HGET', KEYS[1], 'external_user_id') ~= ARGV[5] then return 3 end
redis.call('HSET', KEYS[1], 'status', ARGV[6], 'resolved_by', ARGV[5])
return 1
`)

func (b *RedisApprovalBroker) Resolve(ctx context.Context, resolution ApprovalResolution) (PendingApproval, error) {
	if strings.TrimSpace(resolution.Token) == "" || strings.TrimSpace(resolution.TenantID) == "" ||
		strings.TrimSpace(resolution.Channel) == "" || strings.TrimSpace(resolution.BindingID) == "" ||
		strings.TrimSpace(resolution.ConversationID) == "" || strings.TrimSpace(resolution.ExternalUserID) == "" {
		return PendingApproval{}, errors.New("approval resolution identity is incomplete")
	}
	status := "rejected"
	if resolution.Approved {
		status = "approved"
	}
	key := approvalKeyPrefix + resolution.Token
	result, err := resolveApprovalScript.Run(ctx, b.client, []string{key},
		resolution.TenantID, resolution.Channel, resolution.BindingID, resolution.ConversationID, resolution.ExternalUserID, status,
	).Int64()
	if err != nil {
		return PendingApproval{}, fmt.Errorf("resolve approval: %w", err)
	}
	switch result {
	case 0:
		return PendingApproval{}, ErrApprovalExpired
	case 2:
		return PendingApproval{}, ErrApprovalResolved
	case 3:
		return PendingApproval{}, ErrApprovalRouteMismatch
	case 1:
	default:
		return PendingApproval{}, fmt.Errorf("unexpected approval resolution result %d", result)
	}
	fields, err := b.client.HGetAll(ctx, key).Result()
	if err != nil {
		return PendingApproval{}, fmt.Errorf("read resolved approval: %w", err)
	}
	return decodePendingApproval(resolution.Token, fields)
}

func validateApprovalRequest(request ApprovalRequest) error {
	if strings.TrimSpace(request.TenantID) == "" || strings.TrimSpace(request.AppCode) == "" || request.ConfigVersion == 0 ||
		strings.TrimSpace(request.Channel) == "" || strings.TrimSpace(request.BindingID) == "" || strings.TrimSpace(request.ConversationID) == "" ||
		strings.TrimSpace(request.ExternalUserID) == "" || strings.TrimSpace(request.ToolName) == "" {
		return errors.New("approval request identity is incomplete")
	}
	return nil
}

func decodePendingApproval(token string, fields map[string]string) (PendingApproval, error) {
	version, err := strconv.ParseUint(fields["config_version"], 10, 64)
	if err != nil || version == 0 {
		return PendingApproval{}, errors.New("approval config version is invalid")
	}
	approval := PendingApproval{
		Token: token, TenantID: fields["tenant_id"], AppCode: fields["app_code"], ConfigVersion: version,
		Channel: fields["channel"], BindingID: fields["binding_id"], ConversationID: fields["conversation_id"],
		ConversationScope: fields["conversation_scope"], ExternalUserID: fields["external_user_id"],
		ProgressMessageID: fields["progress_message_id"], ProviderReplyToken: fields["provider_reply_token"],
		ToolName: fields["tool_name"], ToolDescription: fields["tool_description"], NotificationID: fields["notification_id"],
	}
	if err := validateApprovalRequest(ApprovalRequest{
		TenantID: approval.TenantID, AppCode: approval.AppCode, ConfigVersion: approval.ConfigVersion,
		Channel: approval.Channel, BindingID: approval.BindingID, ConversationID: approval.ConversationID,
		ConversationScope: approval.ConversationScope, ExternalUserID: approval.ExternalUserID, ToolName: approval.ToolName,
	}); err != nil {
		return PendingApproval{}, err
	}
	return approval, nil
}
