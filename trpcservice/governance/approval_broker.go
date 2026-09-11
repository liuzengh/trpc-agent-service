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

const (
	// Keep the approval hash and pending index in the same Redis Cluster hash
	// slot so the lifecycle scripts remain atomic under clustered deployments.
	approvalKeyPrefix  = "trpc:approval:{approval}:"
	approvalPendingKey = "trpc:approval:{approval}:pending"
)

type ApprovalRequest struct {
	TenantID           string
	AppCode            string
	ConfigVersion      uint64
	RequestID          string
	TraceID            string
	Channel            string
	BindingID          string
	ConversationID     string
	ConversationScope  string
	ExternalUserID     string
	RequesterUserID    string
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
	RequestID          string
	TraceID            string
	Channel            string
	BindingID          string
	ConversationID     string
	ConversationScope  string
	ExternalUserID     string
	RequesterUserID    string
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

// ApprovalNotifier publishes a pending approval to an interactive surface.
// Returning an empty notification ID means the notifier intentionally did not
// handle this approval (for example, an IM approval owned by the Channel node).
type ApprovalNotifier interface {
	NotifyPendingApproval(context.Context, PendingApproval) (string, error)
}

type ApprovalBrokerOption func(*RedisApprovalBroker)

func WithApprovalNotifier(notifier ApprovalNotifier) ApprovalBrokerOption {
	return func(broker *RedisApprovalBroker) {
		broker.notifier = notifier
	}
}

type ApprovalBroker interface {
	ApprovalRequester
	ListPending(context.Context, int) ([]PendingApproval, error)
	MarkNotified(context.Context, PendingApproval, string) error
	Resolve(context.Context, ApprovalResolution) (PendingApproval, error)
}

type RedisApprovalBroker struct {
	client   redis.UniversalClient
	store    ApprovalStore
	ttl      time.Duration
	notifier ApprovalNotifier
}

func NewRedisApprovalBroker(client redis.UniversalClient, store ApprovalStore, ttl time.Duration, options ...ApprovalBrokerOption) (*RedisApprovalBroker, error) {
	if client == nil {
		return nil, errors.New("approval broker Redis client is required")
	}
	if store == nil {
		return nil, errors.New("approval broker durable store is required")
	}
	if ttl <= 0 {
		return nil, errors.New("approval broker TTL must be positive")
	}
	broker := &RedisApprovalBroker{client: client, store: store, ttl: ttl}
	for _, option := range options {
		if option != nil {
			option(broker)
		}
	}
	return broker, nil
}

func (b *RedisApprovalBroker) Request(ctx context.Context, request ApprovalRequest) (bool, error) {
	if err := validateApprovalRequest(request); err != nil {
		return false, err
	}
	token := uuid.NewString()
	now := time.Now().UTC()
	expiresAt := now.Add(b.ttl)
	if err := b.store.Create(ctx, ApprovalRecord{
		Token: token, TenantID: request.TenantID, AppCode: request.AppCode, ConfigVersion: request.ConfigVersion,
		RequestID: strings.TrimSpace(request.RequestID), TraceID: strings.TrimSpace(request.TraceID),
		Channel: request.Channel, BindingID: request.BindingID, ConversationID: request.ConversationID,
		ConversationScope: strings.TrimSpace(request.ConversationScope), ExternalUserID: request.ExternalUserID,
		RequesterUserID: strings.TrimSpace(request.RequesterUserID), ProgressMessageID: strings.TrimSpace(request.ProgressMessageID), ToolName: request.ToolName,
		ToolDescription: strings.TrimSpace(request.ToolDescription), Status: ApprovalPending,
		CreatedAt: now, ExpiresAt: expiresAt,
	}); err != nil {
		return false, fmt.Errorf("persist approval request: %w", err)
	}
	if err := b.restoreRedisApproval(ctx, token, request, "", ApprovalPending, expiresAt); err != nil {
		b.closeDurableApproval(request.TenantID, token, ApprovalCanceled)
		return false, fmt.Errorf("create approval request: %w", err)
	}
	if b.notifier != nil {
		pending := PendingApproval{
			Token: token, TenantID: request.TenantID, AppCode: request.AppCode, ConfigVersion: request.ConfigVersion,
			RequestID: strings.TrimSpace(request.RequestID), TraceID: strings.TrimSpace(request.TraceID),
			Channel: request.Channel, BindingID: request.BindingID, ConversationID: request.ConversationID,
			ConversationScope: strings.TrimSpace(request.ConversationScope), ExternalUserID: request.ExternalUserID,
			RequesterUserID: strings.TrimSpace(request.RequesterUserID), ProgressMessageID: strings.TrimSpace(request.ProgressMessageID),
			ProviderReplyToken: strings.TrimSpace(request.ProviderReplyToken), ToolName: request.ToolName,
			ToolDescription: strings.TrimSpace(request.ToolDescription),
		}
		notificationID, notifyErr := b.notifier.NotifyPendingApproval(ctx, pending)
		if notifyErr != nil {
			b.cancelPending(request.TenantID, approvalKeyPrefix+token)
			return false, fmt.Errorf("notify approval request: %w", notifyErr)
		}
		if strings.TrimSpace(notificationID) != "" {
			pending.NotificationID = notificationID
			if err := b.MarkNotified(ctx, pending, notificationID); err != nil {
				b.cancelPending(request.TenantID, approvalKeyPrefix+token)
				return false, fmt.Errorf("mark approval request notified: %w", err)
			}
		}
	}
	key := approvalKeyPrefix + token

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	polls := 0
	for {
		status, err := b.client.HGet(ctx, key, "status").Result()
		switch {
		case errors.Is(err, redis.Nil):
			resolved, durableErr := b.recoverFromDurable(ctx, token, request)
			if durableErr != nil {
				return false, durableErr
			}
			if resolved != nil {
				return *resolved, nil
			}
			continue
		case err != nil:
			resolved, durableErr := b.readDurableDecision(ctx, request.TenantID, token)
			if durableErr == nil && resolved != nil {
				return *resolved, nil
			}
			return false, fmt.Errorf("read approval decision: %w", err)
		case status == "approved":
			return true, nil
		case status == "rejected":
			return false, nil
		case status == "pending":
			polls++
			if polls%5 == 0 {
				resolved, durableErr := b.readDurableDecision(ctx, request.TenantID, token)
				// Redis is still a healthy low-latency coordination path. A
				// transient PostgreSQL read failure must not manufacture a new
				// approval request through Kafka retry; keep waiting and use the
				// durable read only as stale-Redis reconciliation.
				if durableErr == nil && resolved != nil {
					status := string(ApprovalRejected)
					if *resolved {
						status = string(ApprovalApproved)
					}
					_ = b.client.HSet(ctx, key, "status", status).Err()
					_ = b.client.ZRem(ctx, approvalPendingKey, token).Err()
					return *resolved, nil
				}
			}
		case status != "pending":
			return false, fmt.Errorf("invalid approval status %q", status)
		}
		select {
		case <-ctx.Done():
			b.cancelPending(request.TenantID, key)
			return false, ctx.Err()
		case <-ticker.C:
		}
	}
}

// ResolveForRequester resolves a browser approval without trusting route
// fields supplied by the browser. The durable approval owns the route; the
// authenticated platform user only proves they are the original requester.
func (b *RedisApprovalBroker) ResolveForRequester(ctx context.Context, tenantID, token, requesterUserID string, approved bool) (PendingApproval, error) {
	tenantID, token, requesterUserID = strings.TrimSpace(tenantID), strings.TrimSpace(token), strings.TrimSpace(requesterUserID)
	if tenantID == "" || token == "" || requesterUserID == "" {
		return PendingApproval{}, errors.New("approval requester identity is incomplete")
	}
	record, err := b.store.Get(ctx, tenantID, token)
	if errors.Is(err, ErrApprovalRecordNotFound) {
		return PendingApproval{}, ErrApprovalExpired
	}
	if err != nil {
		return PendingApproval{}, fmt.Errorf("read approval requester: %w", err)
	}
	if record.Channel != "web" || strings.TrimSpace(record.RequesterUserID) != requesterUserID {
		return PendingApproval{}, ErrApprovalRouteMismatch
	}
	if record.Status == ApprovalApproved || record.Status == ApprovalRejected {
		wasApproved := record.Status == ApprovalApproved
		if wasApproved == approved {
			return pendingApprovalFromRecord(record), nil
		}
		return PendingApproval{}, ErrApprovalResolved
	}
	return b.Resolve(ctx, ApprovalResolution{
		Token: record.Token, TenantID: record.TenantID, Channel: record.Channel, BindingID: record.BindingID,
		ConversationID: record.ConversationID, ExternalUserID: record.ExternalUserID, Approved: approved,
	})
}

func (b *RedisApprovalBroker) recoverFromDurable(ctx context.Context, token string, request ApprovalRequest) (*bool, error) {
	record, err := b.store.Get(ctx, request.TenantID, token)
	if err != nil {
		if errors.Is(err, ErrApprovalRecordNotFound) {
			return nil, ErrApprovalExpired
		}
		return nil, fmt.Errorf("read durable approval: %w", err)
	}
	switch record.Status {
	case ApprovalApproved:
		approved := true
		return &approved, nil
	case ApprovalRejected:
		approved := false
		return &approved, nil
	case ApprovalExpired, ApprovalCanceled:
		return nil, ErrApprovalExpired
	case ApprovalPending:
		if !record.ExpiresAt.After(time.Now().UTC()) {
			if err := b.store.Close(ctx, record.TenantID, record.Token, ApprovalExpired); err != nil && !errors.Is(err, ErrApprovalRecordNotFound) {
				return nil, fmt.Errorf("expire durable approval: %w", err)
			}
			return nil, ErrApprovalExpired
		}
		if err := b.restoreRedisApproval(ctx, token, request, record.NotificationID, ApprovalPending, record.ExpiresAt); err != nil {
			return nil, fmt.Errorf("restore approval coordination state: %w", err)
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid durable approval status %q", record.Status)
	}
}

func (b *RedisApprovalBroker) readDurableDecision(ctx context.Context, tenantID, token string) (*bool, error) {
	record, err := b.store.Get(ctx, tenantID, token)
	if err != nil {
		return nil, err
	}
	switch record.Status {
	case ApprovalApproved:
		approved := true
		return &approved, nil
	case ApprovalRejected:
		approved := false
		return &approved, nil
	default:
		return nil, nil
	}
}

func (b *RedisApprovalBroker) restoreRedisApproval(ctx context.Context, token string, request ApprovalRequest, notificationID string, status ApprovalStatus, expiresAt time.Time) error {
	remaining := time.Until(expiresAt)
	if remaining <= 0 {
		return ErrApprovalExpired
	}
	key := approvalKeyPrefix + token
	values := map[string]any{
		"tenant_id": request.TenantID, "app_code": request.AppCode, "config_version": request.ConfigVersion,
		"request_id": strings.TrimSpace(request.RequestID), "trace_id": strings.TrimSpace(request.TraceID),
		"channel": request.Channel, "binding_id": request.BindingID, "conversation_id": request.ConversationID,
		"conversation_scope": request.ConversationScope, "external_user_id": request.ExternalUserID,
		"requester_user_id":   strings.TrimSpace(request.RequesterUserID),
		"progress_message_id": strings.TrimSpace(request.ProgressMessageID), "provider_reply_token": strings.TrimSpace(request.ProviderReplyToken),
		"tool_name": request.ToolName, "tool_description": strings.TrimSpace(request.ToolDescription),
		"status": string(status), "notification_id": strings.TrimSpace(notificationID),
	}
	_, err := b.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, key, values)
		pipe.Expire(ctx, key, remaining)
		if status == ApprovalPending && strings.TrimSpace(notificationID) == "" {
			pipe.ZAdd(ctx, approvalPendingKey, redis.Z{Score: float64(expiresAt.UnixMilli()), Member: token})
		} else {
			pipe.ZRem(ctx, approvalPendingKey, token)
		}
		return nil
	})
	return err
}

func (b *RedisApprovalBroker) closeDurableApproval(tenantID, token string, status ApprovalStatus) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = b.store.Close(cleanupCtx, tenantID, token, status)
}

var cancelPendingApprovalScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HGET', KEYS[1], 'status') ~= 'pending' then return 2 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`)

func (b *RedisApprovalBroker) cancelPending(tenantID, key string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	token := strings.TrimPrefix(key, approvalKeyPrefix)
	_ = b.store.Close(cleanupCtx, tenantID, token, ApprovalCanceled)
	_ = cancelPendingApprovalScript.Run(cleanupCtx, b.client, []string{key, approvalPendingKey}, token).Err()
}

func (b *RedisApprovalBroker) ListPending(ctx context.Context, limit int) ([]PendingApproval, error) {
	if limit <= 0 {
		return nil, errors.New("approval pending limit must be positive")
	}
	nowMillis := time.Now().UnixMilli()
	if err := b.client.ZRemRangeByScore(ctx, approvalPendingKey, "-inf", strconv.FormatInt(nowMillis, 10)).Err(); err != nil {
		return nil, fmt.Errorf("clean expired pending approval index: %w", err)
	}
	tokens, err := b.client.ZRange(ctx, approvalPendingKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("list pending approval index: %w", err)
	}
	results := make([]PendingApproval, 0, limit)
	seen := make(map[string]struct{}, limit)
	for _, token := range tokens {
		fields, err := b.client.HGetAll(ctx, approvalKeyPrefix+token).Result()
		if err != nil {
			return nil, fmt.Errorf("read pending approval: %w", err)
		}
		if len(fields) == 0 || fields["status"] != "pending" || fields["notification_id"] != "" {
			_ = b.client.ZRem(ctx, approvalPendingKey, token).Err()
			continue
		}
		pending, err := decodePendingApproval(token, fields)
		if err != nil {
			_ = b.client.ZRem(ctx, approvalPendingKey, token).Err()
			continue
		}
		record, err := b.store.Get(ctx, pending.TenantID, token)
		if errors.Is(err, ErrApprovalRecordNotFound) {
			_ = b.client.Del(ctx, approvalKeyPrefix+token).Err()
			_ = b.client.ZRem(ctx, approvalPendingKey, token).Err()
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read durable pending approval: %w", err)
		}
		if record.Status == ApprovalPending && !record.ExpiresAt.After(time.Now().UTC()) {
			if err := b.store.Close(ctx, record.TenantID, record.Token, ApprovalExpired); err != nil {
				return nil, fmt.Errorf("expire durable pending approval: %w", err)
			}
			record.Status = ApprovalExpired
		}
		if record.Status != ApprovalPending {
			_ = b.client.HSet(ctx, approvalKeyPrefix+token, "status", string(record.Status), "resolved_by", record.ResolvedBy).Err()
			_ = b.client.ZRem(ctx, approvalPendingKey, token).Err()
			continue
		}
		if strings.TrimSpace(record.NotificationID) != "" {
			_ = b.client.HSet(ctx, approvalKeyPrefix+token, "notification_id", record.NotificationID).Err()
			_ = b.client.ZRem(ctx, approvalPendingKey, token).Err()
			continue
		}
		results = append(results, pending)
		seen[token] = struct{}{}
	}
	if len(results) < limit {
		durablePending, err := b.store.ListPending(ctx, limit)
		if err != nil {
			return nil, fmt.Errorf("list durable pending approvals: %w", err)
		}
		for _, record := range durablePending {
			if _, exists := seen[record.Token]; exists {
				continue
			}
			pending := pendingApprovalFromRecord(record)
			request := approvalRequestFromPending(pending)
			if err := b.restoreRedisApproval(ctx, record.Token, request, "", ApprovalPending, record.ExpiresAt); err != nil {
				if errors.Is(err, ErrApprovalExpired) {
					_ = b.store.Close(ctx, record.TenantID, record.Token, ApprovalExpired)
					continue
				}
				return nil, fmt.Errorf("restore durable pending approval: %w", err)
			}
			results = append(results, pending)
			seen[record.Token] = struct{}{}
			if len(results) == limit {
				break
			}
		}
	}
	return results, nil
}

var markApprovalNotifiedScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HGET', KEYS[1], 'status') ~= 'pending' then return 2 end
if redis.call('HGET', KEYS[1], 'notification_id') ~= '' then return 3 end
redis.call('HSET', KEYS[1], 'notification_id', ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[2])
return 1
`)

func (b *RedisApprovalBroker) MarkNotified(ctx context.Context, approval PendingApproval, notificationID string) error {
	token := strings.TrimSpace(approval.Token)
	if token == "" || strings.TrimSpace(approval.TenantID) == "" || strings.TrimSpace(notificationID) == "" {
		return errors.New("approval token and notification ID are required")
	}
	if err := b.store.MarkNotified(ctx, approval.TenantID, token, notificationID); err != nil {
		if errors.Is(err, ErrApprovalRecordNotFound) {
			return ErrApprovalExpired
		}
		return err
	}
	result, err := markApprovalNotifiedScript.Run(ctx, b.client, []string{approvalKeyPrefix + token, approvalPendingKey}, notificationID, token).Int64()
	if err != nil {
		return fmt.Errorf("mark approval notified: %w", err)
	}
	switch result {
	case 1, 2, 3:
		return nil
	case 0:
		record, readErr := b.store.Get(ctx, approval.TenantID, token)
		if readErr != nil {
			return readErr
		}
		request := approvalRequestFromPending(approval)
		return b.restoreRedisApproval(ctx, token, request, notificationID, ApprovalPending, record.ExpiresAt)
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
redis.call('ZREM', KEYS[2], ARGV[7])
return 1
`)

func (b *RedisApprovalBroker) Resolve(ctx context.Context, resolution ApprovalResolution) (PendingApproval, error) {
	if strings.TrimSpace(resolution.Token) == "" || strings.TrimSpace(resolution.TenantID) == "" ||
		strings.TrimSpace(resolution.Channel) == "" || strings.TrimSpace(resolution.BindingID) == "" ||
		strings.TrimSpace(resolution.ConversationID) == "" || strings.TrimSpace(resolution.ExternalUserID) == "" {
		return PendingApproval{}, errors.New("approval resolution identity is incomplete")
	}
	record, err := b.store.Get(ctx, resolution.TenantID, resolution.Token)
	if errors.Is(err, ErrApprovalRecordNotFound) {
		return PendingApproval{}, ErrApprovalExpired
	}
	if err != nil {
		return PendingApproval{}, fmt.Errorf("read durable approval resolution: %w", err)
	}
	if record.Channel != resolution.Channel || record.BindingID != resolution.BindingID ||
		record.ConversationID != resolution.ConversationID || record.ExternalUserID != resolution.ExternalUserID {
		return PendingApproval{}, ErrApprovalRouteMismatch
	}
	if record.Status != ApprovalPending {
		wanted := ApprovalRejected
		if resolution.Approved {
			wanted = ApprovalApproved
		}
		if record.Status == wanted && record.ResolvedBy == resolution.ExternalUserID {
			return pendingApprovalFromRecord(record), nil
		}
		return PendingApproval{}, ErrApprovalResolved
	}
	if !record.ExpiresAt.After(time.Now().UTC()) {
		_ = b.store.Close(ctx, resolution.TenantID, resolution.Token, ApprovalExpired)
		_ = b.client.ZRem(ctx, approvalPendingKey, resolution.Token).Err()
		return PendingApproval{}, ErrApprovalExpired
	}
	if err := b.store.Resolve(ctx, resolution.TenantID, resolution.Token, resolution.Approved, resolution.ExternalUserID); err != nil {
		if errors.Is(err, ErrApprovalExpired) {
			return PendingApproval{}, ErrApprovalExpired
		}
		if errors.Is(err, ErrApprovalResolved) {
			return PendingApproval{}, ErrApprovalResolved
		}
		return PendingApproval{}, fmt.Errorf("persist approval resolution: %w", err)
	}
	status := string(ApprovalRejected)
	if resolution.Approved {
		status = string(ApprovalApproved)
	}
	key := approvalKeyPrefix + resolution.Token
	result, err := resolveApprovalScript.Run(ctx, b.client, []string{key, approvalPendingKey},
		resolution.TenantID, resolution.Channel, resolution.BindingID, resolution.ConversationID, resolution.ExternalUserID, status, resolution.Token,
	).Int64()
	if err != nil {
		return PendingApproval{}, fmt.Errorf("resolve approval: %w", err)
	}
	switch result {
	case 0:
		resolved := pendingApprovalFromRecord(record)
		resolved.NotificationID = record.NotificationID
		return resolved, nil
	case 2:
		resolved := pendingApprovalFromRecord(record)
		resolved.NotificationID = record.NotificationID
		return resolved, nil
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
	resolved, err := decodePendingApproval(resolution.Token, fields)
	if err != nil {
		return pendingApprovalFromRecord(record), nil
	}
	return resolved, nil
}

func approvalRequestFromPending(approval PendingApproval) ApprovalRequest {
	return ApprovalRequest{
		TenantID: approval.TenantID, AppCode: approval.AppCode, ConfigVersion: approval.ConfigVersion,
		RequestID: approval.RequestID, TraceID: approval.TraceID,
		Channel: approval.Channel, BindingID: approval.BindingID, ConversationID: approval.ConversationID,
		ConversationScope: approval.ConversationScope, ExternalUserID: approval.ExternalUserID,
		RequesterUserID: approval.RequesterUserID, ProgressMessageID: approval.ProgressMessageID,
		ProviderReplyToken: approval.ProviderReplyToken, ToolName: approval.ToolName, ToolDescription: approval.ToolDescription,
	}
}

func pendingApprovalFromRecord(record ApprovalRecord) PendingApproval {
	return PendingApproval{
		Token: record.Token, TenantID: record.TenantID, AppCode: record.AppCode, ConfigVersion: record.ConfigVersion,
		RequestID: record.RequestID, TraceID: record.TraceID,
		Channel: record.Channel, BindingID: record.BindingID, ConversationID: record.ConversationID,
		ConversationScope: record.ConversationScope, ExternalUserID: record.ExternalUserID,
		RequesterUserID: record.RequesterUserID, ProgressMessageID: record.ProgressMessageID,
		ToolName: record.ToolName, ToolDescription: record.ToolDescription,
		NotificationID: record.NotificationID,
	}
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
		RequestID: fields["request_id"], TraceID: fields["trace_id"],
		Channel: fields["channel"], BindingID: fields["binding_id"], ConversationID: fields["conversation_id"],
		ConversationScope: fields["conversation_scope"], ExternalUserID: fields["external_user_id"],
		RequesterUserID:   fields["requester_user_id"],
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
