package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var initializeTenantScript = redis.NewScript(`
local marker_type=redis.call('TYPE',KEYS[1]); local policy_type=redis.call('TYPE',KEYS[2]); local placement_type=redis.call('TYPE',KEYS[3])
if type(marker_type)=='table' then marker_type=marker_type.ok end; if type(policy_type)=='table' then policy_type=policy_type.ok end; if type(placement_type)=='table' then placement_type=placement_type.ok end
if (marker_type~='none' and marker_type~='string') or (policy_type~='none' and policy_type~='string') or (placement_type~='none' and placement_type~='string') then return -9 end
if redis.call('EXISTS',KEYS[1])==1 then
  if redis.call('EXISTS',KEYS[2])==0 then return -2 end
  if redis.call('EXISTS',KEYS[3])==0 then return -3 end
  return 0
end
redis.call('SETNX',KEYS[2],ARGV[1]); redis.call('SETNX',KEYS[3],ARGV[2]); redis.call('SET',KEYS[1],'1'); return 1
`)

var casJSONScript = redis.NewScript(`
local current_type=redis.call('TYPE',KEYS[1]); if type(current_type)=='table' then current_type=current_type.ok end
if current_type~='string' then return current_type=='none' and -1 or -9 end
local ok,current=pcall(cjson.decode,redis.call('GET',KEYS[1])); if not ok or type(current)~='table' then return -9 end
if tonumber(current['revision'] or '0')~=tonumber(ARGV[1]) then return 0 end
redis.call('SET',KEYS[1],ARGV[2]); return 1
`)

var registerNodeScript = redis.NewScript(`
local node_type=redis.call('TYPE',KEYS[1]); local lease_type=redis.call('TYPE',KEYS[2]); local index_type=redis.call('TYPE',KEYS[3])
if type(node_type)=='table' then node_type=node_type.ok end; if type(lease_type)=='table' then lease_type=lease_type.ok end; if type(index_type)=='table' then index_type=index_type.ok end
if (node_type~='none' and node_type~='string') or (lease_type~='none' and lease_type~='string') or (index_type~='none' and index_type~='set') then return -9 end
local owner=redis.call('GET',KEYS[2]); if owner and owner~=ARGV[1] then return -2 end
redis.call('SET',KEYS[2],ARGV[1],'PX',ARGV[2]); redis.call('SET',KEYS[1],ARGV[3]); redis.call('SADD',KEYS[3],ARGV[4]); return 1
`)

var heartbeatNodeScript = redis.NewScript(`
local node_type=redis.call('TYPE',KEYS[1]); local lease_type=redis.call('TYPE',KEYS[2])
if type(node_type)=='table' then node_type=node_type.ok end; if type(lease_type)=='table' then lease_type=lease_type.ok end
if node_type~='string' or lease_type~='string' then return (node_type=='none' or lease_type=='none') and -1 or -9 end
if redis.call('GET',KEYS[2])~=ARGV[1] then return -2 end
redis.call('SET',KEYS[2],ARGV[1],'PX',ARGV[2]); redis.call('SET',KEYS[1],ARGV[3]); return 1
`)

var createAssignmentScript = redis.NewScript(`
local assignment_type=redis.call('TYPE',KEYS[1]); local index_type=redis.call('TYPE',KEYS[2])
if type(assignment_type)=='table' then assignment_type=assignment_type.ok end; if type(index_type)=='table' then index_type=index_type.ok end
if (assignment_type~='none' and assignment_type~='string') or (index_type~='none' and index_type~='set') then return -9 end
local raw=redis.call('GET',KEYS[1]); if raw then
  local ok,current=pcall(cjson.decode,raw); if not ok or type(current)~='table' then return -9 end
  if current['payload_digest']~=ARGV[1] or current['tenant_id']~=ARGV[2] or current['agent_app_id']~=ARGV[3] then return -2 end
  return 0
end
redis.call('SET',KEYS[1],ARGV[4]); redis.call('SADD',KEYS[2],ARGV[5]); return 1
`)

var approveConfirmationScript = redis.NewScript(`
local current_type=redis.call('TYPE',KEYS[1]); if type(current_type)=='table' then current_type=current_type.ok end
if current_type=='none' then return {-1,''} end; if current_type~='string' then return {-9,''} end
local raw=redis.call('GET',KEYS[1]); local ok,current=pcall(cjson.decode,raw); if not ok or type(current)~='table' then return {-9,''} end
if current['tenant_id']~=ARGV[1] or current['actor_user_id_hash']~=ARGV[2] or current['session_id']~=ARGV[3] or current['state']~='requested' then return {-2,''} end
current['state']='approved'; local updated=cjson.encode(current); local ttl=redis.call('PTTL',KEYS[1]); if ttl<=0 then return {-1,''} end
redis.call('SET',KEYS[1],updated,'PX',ttl); return {1,updated}
`)

var consumeConfirmationScript = redis.NewScript(`
local current_type=redis.call('TYPE',KEYS[1]); if type(current_type)=='table' then current_type=current_type.ok end
if current_type=='none' then return -1 end; if current_type~='string' then return -9 end
local ok,current=pcall(cjson.decode,redis.call('GET',KEYS[1])); if not ok or type(current)~='table' then return -9 end
if current['state']~='approved' or current['tenant_id']~=ARGV[1] or current['actor_user_id_hash']~=ARGV[2] or current['session_id']~=ARGV[3] or current['tool_name']~=ARGV[4] or current['args_digest']~=ARGV[5] then return -2 end
redis.call('DEL',KEYS[1]); return 1
`)

var consumeConfirmationMatchScript = redis.NewScript(`
local match_type=redis.call('TYPE',KEYS[1]); if type(match_type)=='table' then match_type=match_type.ok end
if match_type=='none' then return -1 end; if match_type~='string' then return -9 end
local nonce=redis.call('GET',KEYS[1]); local confirmation_key=KEYS[2]..nonce
local current_type=redis.call('TYPE',confirmation_key); if type(current_type)=='table' then current_type=current_type.ok end
if current_type=='none' then redis.call('DEL',KEYS[1]); return -1 end; if current_type~='string' then return -9 end
local ok,current=pcall(cjson.decode,redis.call('GET',confirmation_key)); if not ok or type(current)~='table' then return -9 end
if current['state']~='approved' or current['tenant_id']~=ARGV[1] or current['actor_user_id_hash']~=ARGV[2] or current['session_id']~=ARGV[3] or current['tool_name']~=ARGV[4] or current['args_digest']~=ARGV[5] then return -2 end
redis.call('DEL',confirmation_key,KEYS[1]); return 1
`)

var acquireLeaseScript = redis.NewScript(`
local current_type=redis.call('TYPE',KEYS[1]); if type(current_type)=='table' then current_type=current_type.ok end
if current_type~='none' and current_type~='string' then return -9 end
local owner=redis.call('GET',KEYS[1]); if owner and owner~=ARGV[1] then return 0 end
redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2]); return 1
`)

var releaseLeaseScript = redis.NewScript(`
local current_type=redis.call('TYPE',KEYS[1]); if type(current_type)=='table' then current_type=current_type.ok end
if current_type=='none' then return 0 end; if current_type~='string' then return -9 end
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
redis.call('DEL',KEYS[1]); return 1
`)

var appendAuditScript = redis.NewScript(`
local audit_type=redis.call('TYPE',KEYS[1]); local ids_type=redis.call('TYPE',KEYS[2])
if type(audit_type)=='table' then audit_type=audit_type.ok end; if type(ids_type)=='table' then ids_type=ids_type.ok end
if (audit_type~='none' and audit_type~='zset') or (ids_type~='none' and ids_type~='zset') then return -9 end
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',ARGV[3]); redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',ARGV[3])
if tonumber(ARGV[1])<=tonumber(ARGV[3]) then return 0 end
if redis.call('ZADD',KEYS[2],'NX',ARGV[1],ARGV[2])==0 then return 0 end
redis.call('ZADD',KEYS[1],ARGV[1],ARGV[4])
redis.call('PEXPIRE',KEYS[1],ARGV[5]); redis.call('PEXPIRE',KEYS[2],ARGV[5]); return 1
`)

type RedisRepository struct {
	client *redis.Client
	config config.ControlPlaneConfig
	prefix string

	closeOnce sync.Once
	closeErr  error
}

func NewRedisRepository(cfg config.ControlPlaneConfig) (*RedisRepository, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	options, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, errors.New("parse control plane Redis URL")
	}
	return &RedisRepository{
		client: redis.NewClient(options), config: cfg,
		prefix: strings.TrimRight(cfg.KeyPrefix, ":") + ":control-v1",
	}, nil
}

func (r *RedisRepository) Ready(ctx context.Context) error {
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("%w: Redis ping", ErrUnavailable)
	}
	if r.config.DevelopmentAllowSharedRedis {
		return nil
	}
	values, err := r.client.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return fmt.Errorf("%w: Redis maxmemory policy unavailable", ErrUnavailable)
	}
	if !strings.EqualFold(values["maxmemory-policy"], "noeviction") {
		return fmt.Errorf("%w: control plane Redis must use maxmemory-policy=noeviction", ErrUnavailable)
	}
	return nil
}

func (r *RedisRepository) InitializeTenants(ctx context.Context, tenants []tenant.Tenant, defaultTools []string) error {
	for _, current := range tenants {
		if !current.Enabled {
			continue
		}
		if err := tenant.ValidateID("tenant id", current.ID); err != nil {
			return err
		}
		now := time.Now().UTC()
		policy := DefaultTenantPolicy(current.ID, defaultTools, now)
		placement := DefaultTenantPlacement(current.ID, now)
		policyRaw, _ := json.Marshal(policy)
		placementRaw, _ := json.Marshal(placement)
		result, err := initializeTenantScript.Run(ctx, r.client, []string{r.initializedKey(current.ID), r.policyKey(current.ID), r.placementKey(current.ID)}, string(policyRaw), string(placementRaw)).Int()
		if err != nil {
			return fmt.Errorf("%w: initialize tenant control state", ErrUnavailable)
		}
		switch result {
		case -9:
			return fmt.Errorf("%w: incompatible tenant control key", ErrUnavailable)
		case -2:
			return ErrPolicyMissing
		case -3:
			return ErrPlacementMissing
		}
	}
	return nil
}

func (r *RedisRepository) GetPolicy(ctx context.Context, tenantID string) (TenantPolicy, error) {
	if exists, err := r.client.Exists(ctx, r.initializedKey(tenantID)).Result(); err != nil {
		return TenantPolicy{}, fmt.Errorf("%w: read tenant initialization", ErrUnavailable)
	} else if exists == 0 {
		return TenantPolicy{}, ErrPolicyMissing
	}
	var policy TenantPolicy
	if err := r.getJSON(ctx, r.policyKey(tenantID), &policy); err != nil {
		if errors.Is(err, redis.Nil) {
			return TenantPolicy{}, ErrPolicyMissing
		}
		return TenantPolicy{}, err
	}
	if err := policy.Validate(); err != nil || policy.TenantID != tenantID {
		return TenantPolicy{}, fmt.Errorf("%w: invalid tenant policy", ErrUnavailable)
	}
	return policy, nil
}

func (r *RedisRepository) PutPolicy(ctx context.Context, policy TenantPolicy, expectedRevision int64) (TenantPolicy, error) {
	current, err := r.GetPolicy(ctx, policy.TenantID)
	if err != nil {
		return TenantPolicy{}, err
	}
	if current.Revision != expectedRevision || !containsAll(policy.RedactionPatterns, current.RedactionPatterns) {
		return TenantPolicy{}, ErrRevisionConflict
	}
	policy.ActorAllowlistHashes = normalizedNames(policy.ActorAllowlistHashes)
	policy.ToolAllowlist = normalizedNames(policy.ToolAllowlist)
	policy.DangerousTools = normalizedNames(policy.DangerousTools)
	policy.RedactionPatterns = normalizedNames(policy.RedactionPatterns)
	policy.Revision = expectedRevision + 1
	policy.CreatedAt = current.CreatedAt
	policy.UpdatedAt = time.Now().UTC()
	if err := policy.Validate(); err != nil {
		return TenantPolicy{}, err
	}
	if err := r.casJSON(ctx, r.policyKey(policy.TenantID), expectedRevision, policy); err != nil {
		return TenantPolicy{}, err
	}
	return policy, nil
}

func (r *RedisRepository) GetPlacement(ctx context.Context, tenantID string) (TenantPlacement, error) {
	if exists, err := r.client.Exists(ctx, r.initializedKey(tenantID)).Result(); err != nil {
		return TenantPlacement{}, fmt.Errorf("%w: read tenant initialization", ErrUnavailable)
	} else if exists == 0 {
		return TenantPlacement{}, ErrPlacementMissing
	}
	var placement TenantPlacement
	if err := r.getJSON(ctx, r.placementKey(tenantID), &placement); err != nil {
		if errors.Is(err, redis.Nil) {
			return TenantPlacement{}, ErrPlacementMissing
		}
		return TenantPlacement{}, err
	}
	if err := placement.Validate(); err != nil || placement.TenantID != tenantID {
		return TenantPlacement{}, fmt.Errorf("%w: invalid tenant placement", ErrUnavailable)
	}
	return placement, nil
}

func (r *RedisRepository) PutPlacement(ctx context.Context, placement TenantPlacement, expectedRevision int64) (TenantPlacement, error) {
	current, err := r.GetPlacement(ctx, placement.TenantID)
	if err != nil {
		return TenantPlacement{}, err
	}
	if current.Revision != expectedRevision {
		return TenantPlacement{}, ErrRevisionConflict
	}
	placement.Revision = expectedRevision + 1
	placement.CreatedAt = current.CreatedAt
	placement.UpdatedAt = time.Now().UTC()
	if err := placement.Validate(); err != nil {
		return TenantPlacement{}, err
	}
	if err := r.casJSON(ctx, r.placementKey(placement.TenantID), expectedRevision, placement); err != nil {
		return TenantPlacement{}, err
	}
	return placement, nil
}

func (r *RedisRepository) RegisterNode(ctx context.Context, node NodeRecord) error {
	if err := node.Validate(); err != nil {
		return err
	}
	ttl := time.Until(node.LeaseUntil)
	if ttl <= 0 {
		return errors.New("node lease must be in the future")
	}
	raw, _ := json.Marshal(node)
	result, err := registerNodeScript.Run(ctx, r.client, []string{r.nodeKey(node.NodeID), r.nodeLeaseKey(node.NodeID), r.nodesKey()}, node.BootID, ttl.Milliseconds(), string(raw), node.NodeID).Int()
	if err != nil {
		return fmt.Errorf("%w: register node", ErrUnavailable)
	}
	if result == -2 {
		return ErrNodeConflict
	}
	if result != 1 {
		return fmt.Errorf("%w: incompatible node key", ErrUnavailable)
	}
	return nil
}

func (r *RedisRepository) HeartbeatNode(ctx context.Context, nodeID, bootID string, state NodeState, inflight int, leaseUntil time.Time) error {
	node, err := r.GetNode(ctx, nodeID)
	if err != nil && !errors.Is(err, ErrNodeConflict) {
		return err
	}
	if node.BootID != bootID {
		return ErrNodeConflict
	}
	node.State = state
	node.Inflight = inflight
	node.LastHeartbeat = time.Now().UTC()
	node.LeaseUntil = leaseUntil.UTC()
	if err := node.Validate(); err != nil {
		return err
	}
	ttl := time.Until(node.LeaseUntil)
	if ttl <= 0 {
		return errors.New("node lease must be in the future")
	}
	raw, _ := json.Marshal(node)
	result, err := heartbeatNodeScript.Run(ctx, r.client, []string{r.nodeKey(nodeID), r.nodeLeaseKey(nodeID)}, bootID, ttl.Milliseconds(), string(raw)).Int()
	if err != nil {
		return fmt.Errorf("%w: heartbeat node", ErrUnavailable)
	}
	switch result {
	case 1:
		return nil
	case -1:
		return ErrNodeNotFound
	case -2:
		return ErrNodeConflict
	default:
		return fmt.Errorf("%w: incompatible node key", ErrUnavailable)
	}
}

func (r *RedisRepository) GetNode(ctx context.Context, nodeID string) (NodeRecord, error) {
	var node NodeRecord
	if err := r.getJSON(ctx, r.nodeKey(nodeID), &node); err != nil {
		if errors.Is(err, redis.Nil) {
			return NodeRecord{}, ErrNodeNotFound
		}
		return NodeRecord{}, err
	}
	if err := node.Validate(); err != nil || node.NodeID != nodeID {
		return NodeRecord{}, fmt.Errorf("%w: invalid node record", ErrUnavailable)
	}
	owner, err := r.client.Get(ctx, r.nodeLeaseKey(nodeID)).Result()
	if errors.Is(err, redis.Nil) {
		node.State = NodeOffline
		return node, nil
	}
	if err != nil {
		return NodeRecord{}, fmt.Errorf("%w: read node lease", ErrUnavailable)
	}
	if owner != node.BootID {
		return node, ErrNodeConflict
	}
	return node, nil
}

func (r *RedisRepository) ListNodes(ctx context.Context) ([]NodeRecord, error) {
	ids, err := r.client.SMembers(ctx, r.nodesKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("%w: list nodes", ErrUnavailable)
	}
	sort.Strings(ids)
	result := make([]NodeRecord, 0, len(ids))
	for _, id := range ids {
		node, getErr := r.GetNode(ctx, id)
		if errors.Is(getErr, ErrNodeNotFound) {
			continue
		}
		if getErr != nil && !errors.Is(getErr, ErrNodeConflict) {
			return nil, getErr
		}
		result = append(result, node)
	}
	return result, nil
}

func (r *RedisRepository) CreateAssignment(ctx context.Context, assignment NodeAssignment) (NodeAssignment, bool, error) {
	if err := assignment.Validate(); err != nil {
		return NodeAssignment{}, false, err
	}
	raw, _ := json.Marshal(assignment)
	result, err := createAssignmentScript.Run(ctx, r.client, []string{r.assignmentKey(assignment.InboxID), r.assignmentsKey()}, assignment.PayloadDigest, assignment.TenantID, assignment.AgentAppID, string(raw), assignment.InboxID).Int()
	if err != nil {
		return NodeAssignment{}, false, fmt.Errorf("%w: create assignment", ErrUnavailable)
	}
	if result == -2 {
		return NodeAssignment{}, false, ErrAssignmentConflict
	}
	if result == -9 {
		return NodeAssignment{}, false, fmt.Errorf("%w: incompatible assignment key", ErrUnavailable)
	}
	if result == 0 {
		existing, getErr := r.GetAssignment(ctx, assignment.InboxID)
		return existing, false, getErr
	}
	return assignment, true, nil
}

func (r *RedisRepository) GetAssignment(ctx context.Context, inboxID string) (NodeAssignment, error) {
	var assignment NodeAssignment
	if err := r.getJSON(ctx, r.assignmentKey(inboxID), &assignment); err != nil {
		if errors.Is(err, redis.Nil) {
			return NodeAssignment{}, ErrAssignmentNotFound
		}
		return NodeAssignment{}, err
	}
	if err := assignment.Validate(); err != nil || assignment.InboxID != inboxID {
		return NodeAssignment{}, fmt.Errorf("%w: invalid assignment", ErrUnavailable)
	}
	return assignment, nil
}

func (r *RedisRepository) ListAssignments(ctx context.Context) ([]NodeAssignment, error) {
	ids, err := r.client.SMembers(ctx, r.assignmentsKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("%w: list assignments", ErrUnavailable)
	}
	sort.Strings(ids)
	result := make([]NodeAssignment, 0, len(ids))
	for _, id := range ids {
		assignment, getErr := r.GetAssignment(ctx, id)
		if errors.Is(getErr, ErrAssignmentNotFound) {
			continue
		}
		if getErr != nil {
			return nil, getErr
		}
		result = append(result, assignment)
	}
	return result, nil
}

func (r *RedisRepository) PutAssignment(ctx context.Context, assignment NodeAssignment, expectedRevision int64) (NodeAssignment, error) {
	current, err := r.GetAssignment(ctx, assignment.InboxID)
	if err != nil {
		return NodeAssignment{}, err
	}
	if current.Revision != expectedRevision || current.PayloadDigest != assignment.PayloadDigest || current.TenantID != assignment.TenantID || current.AgentAppID != assignment.AgentAppID {
		return NodeAssignment{}, ErrRevisionConflict
	}
	assignment.Revision = expectedRevision + 1
	assignment.CreatedAt = current.CreatedAt
	assignment.UpdatedAt = time.Now().UTC()
	if err := assignment.Validate(); err != nil {
		return NodeAssignment{}, err
	}
	if err := r.casJSON(ctx, r.assignmentKey(assignment.InboxID), expectedRevision, assignment); err != nil {
		return NodeAssignment{}, err
	}
	return assignment, nil
}

// AppendAudit retains the first valid record for an audit ID. Replays of that
// ID within the retention window are successful no-ops and never overwrite
// the original event.
func (r *RedisRepository) AppendAudit(ctx context.Context, record AuditRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-r.config.AuditRetention).UnixMilli()
	result, err := appendAuditScript.Run(
		ctx,
		r.client,
		[]string{r.auditKey(), r.auditIDsKey()},
		record.OccurredAt.UnixMilli(),
		record.ID,
		cutoff,
		string(raw),
		r.config.AuditRetention.Milliseconds(),
	).Int()
	if err != nil {
		return fmt.Errorf("%w: append retained audit event", ErrUnavailable)
	}
	if result == -9 {
		return fmt.Errorf("%w: incompatible audit index", ErrUnavailable)
	}
	return nil
}

func (r *RedisRepository) QueryAudit(ctx context.Context, query AuditQuery) ([]AuditRecord, string, error) {
	limit, offset, err := queryWindow(query.Limit, query.Cursor)
	if err != nil {
		return nil, "", err
	}
	var records []AuditRecord
	next, err := r.queryTimed(ctx, r.auditKey(), query.From, query.To, offset, limit, func(raw string) (bool, error) {
		var record AuditRecord
		if err := decodeStrict(raw, &record); err != nil {
			return false, err
		}
		if query.TenantID != "" && record.TenantID != query.TenantID || query.Decision != "" && record.Decision != query.Decision || query.ToolName != "" && record.ToolName != query.ToolName || query.ErrorType != "" && record.ErrorType != query.ErrorType {
			return false, nil
		}
		records = append(records, record)
		return true, nil
	})
	return records, next, err
}

func (r *RedisRepository) AppendMetric(ctx context.Context, event MetricEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	return r.appendTimed(ctx, r.metricKey(), event.OccurredAt, event, r.config.MetricRetention)
}

func (r *RedisRepository) QueryMetrics(ctx context.Context, query MetricQuery) ([]MetricEvent, string, error) {
	limit, offset, err := queryWindow(query.Limit, query.Cursor)
	if err != nil {
		return nil, "", err
	}
	var events []MetricEvent
	next, err := r.queryTimed(ctx, r.metricKey(), query.From, query.To, offset, limit, func(raw string) (bool, error) {
		var event MetricEvent
		if err := decodeStrict(raw, &event); err != nil {
			return false, err
		}
		if query.TenantID != "" && event.TenantID != query.TenantID || query.AgentAppID != "" && event.AgentAppID != query.AgentAppID {
			return false, nil
		}
		events = append(events, event)
		return true, nil
	})
	return events, next, err
}

func (r *RedisRepository) SetTenantDegraded(ctx context.Context, tenantID, reason string) error {
	if err := tenant.ValidateID("degraded tenant id", tenantID); err != nil {
		return err
	}
	if reason == "" || len(reason) > tenant.MaxIDBytes {
		return errors.New("tenant degraded reason is invalid")
	}
	return r.client.HSet(ctx, r.degradedKey(), tenantID, reason).Err()
}

func (r *RedisRepository) ClearTenantDegraded(ctx context.Context, tenantID string) error {
	return r.client.HDel(ctx, r.degradedKey(), tenantID).Err()
}

func (r *RedisRepository) CreateConfirmation(ctx context.Context, confirmation Confirmation, ttl time.Duration) error {
	if err := confirmation.Validate(); err != nil {
		return err
	}
	if ttl <= 0 {
		return errors.New("confirmation TTL must be positive")
	}
	raw, _ := json.Marshal(confirmation)
	created, err := r.client.SetNX(ctx, r.confirmationKey(confirmation.Nonce), string(raw), ttl).Result()
	if err != nil {
		return fmt.Errorf("%w: create confirmation", ErrUnavailable)
	}
	if !created {
		return ErrConfirmationMismatch
	}
	matchKey := r.confirmationMatchKey(confirmation)
	if ok, err := r.client.SetNX(ctx, matchKey, confirmation.Nonce, ttl).Result(); err != nil || !ok {
		_ = r.client.Del(ctx, r.confirmationKey(confirmation.Nonce)).Err()
		if err != nil {
			return fmt.Errorf("%w: create confirmation index", ErrUnavailable)
		}
		return ErrConfirmationMismatch
	}
	return nil
}

func (r *RedisRepository) ApproveConfirmation(ctx context.Context, tenantID, actorHash, sessionID, nonce string) (Confirmation, error) {
	result, err := approveConfirmationScript.Run(ctx, r.client, []string{r.confirmationKey(nonce)}, tenantID, actorHash, sessionID).Slice()
	if err != nil {
		return Confirmation{}, fmt.Errorf("%w: approve confirmation", ErrUnavailable)
	}
	if len(result) != 2 {
		return Confirmation{}, ErrConfirmationNotFound
	}
	switch asInt64(result[0]) {
	case -1:
		return Confirmation{}, ErrConfirmationNotFound
	case -2:
		return Confirmation{}, ErrConfirmationMismatch
	case -9:
		return Confirmation{}, fmt.Errorf("%w: invalid confirmation record", ErrUnavailable)
	}
	var confirmation Confirmation
	if err := decodeStrict(fmt.Sprint(result[1]), &confirmation); err != nil {
		return Confirmation{}, fmt.Errorf("%w: invalid confirmation record", ErrUnavailable)
	}
	return confirmation, nil
}

func (r *RedisRepository) ConsumeConfirmation(ctx context.Context, confirmation Confirmation) error {
	result, err := consumeConfirmationScript.Run(ctx, r.client, []string{r.confirmationKey(confirmation.Nonce)}, confirmation.TenantID, confirmation.ActorUserIDHash, confirmation.SessionID, confirmation.ToolName, confirmation.ArgsDigest).Int()
	if err != nil {
		return fmt.Errorf("%w: consume confirmation", ErrUnavailable)
	}
	switch result {
	case 1:
		_ = r.client.Del(ctx, r.confirmationMatchKey(confirmation)).Err()
		return nil
	case -1:
		return ErrConfirmationNotFound
	case -2:
		return ErrConfirmationMismatch
	default:
		return fmt.Errorf("%w: invalid confirmation record", ErrUnavailable)
	}
}

// ConfirmationMatcher supports cross-worker consumption without exposing the
// nonce index to callers. Implementations must compare all tuple fields in one
// Redis script and delete both the approved record and tuple index.
func (r *RedisRepository) ConsumeConfirmationMatch(ctx context.Context, tenantID, actorHash, sessionID, toolName, argsDigest string) error {
	match := Confirmation{TenantID: tenantID, ActorUserIDHash: actorHash, SessionID: sessionID, ToolName: toolName, ArgsDigest: argsDigest}
	result, err := consumeConfirmationMatchScript.Run(ctx, r.client, []string{r.confirmationMatchKey(match), r.prefix + ":confirmation:"}, tenantID, actorHash, sessionID, toolName, argsDigest).Int()
	if err != nil {
		return fmt.Errorf("%w: consume confirmation match", ErrUnavailable)
	}
	switch result {
	case 1:
		return nil
	case -1:
		return ErrConfirmationNotFound
	case -2:
		return ErrConfirmationMismatch
	default:
		return fmt.Errorf("%w: invalid confirmation record", ErrUnavailable)
	}
}

func (r *RedisRepository) AcquireLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	return r.setLease(ctx, name, owner, ttl)
}

func (r *RedisRepository) RenewLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	return r.setLease(ctx, name, owner, ttl)
}

func (r *RedisRepository) ReleaseLease(ctx context.Context, name, owner string) error {
	result, err := releaseLeaseScript.Run(ctx, r.client, []string{r.leaseKey(name)}, owner).Int()
	if err != nil {
		return fmt.Errorf("%w: release lease", ErrUnavailable)
	}
	if result == -9 {
		return fmt.Errorf("%w: incompatible lease key", ErrUnavailable)
	}
	return nil
}

func (r *RedisRepository) Close() error {
	r.closeOnce.Do(func() { r.closeErr = r.client.Close() })
	return r.closeErr
}

func (r *RedisRepository) setLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(owner) == "" || ttl <= 0 {
		return false, errors.New("lease name, owner, and positive TTL are required")
	}
	result, err := acquireLeaseScript.Run(ctx, r.client, []string{r.leaseKey(name)}, owner, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("%w: acquire lease", ErrUnavailable)
	}
	if result == -9 {
		return false, fmt.Errorf("%w: incompatible lease key", ErrUnavailable)
	}
	return result == 1, nil
}

func (r *RedisRepository) getJSON(ctx context.Context, key string, target any) error {
	raw, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	if err := decodeStrict(raw, target); err != nil {
		return fmt.Errorf("%w: invalid control plane record", ErrUnavailable)
	}
	return nil
}

func (r *RedisRepository) casJSON(ctx context.Context, key string, expected int64, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	result, err := casJSONScript.Run(ctx, r.client, []string{key}, expected, string(raw)).Int()
	if err != nil {
		return fmt.Errorf("%w: update control plane record", ErrUnavailable)
	}
	switch result {
	case 1:
		return nil
	case 0:
		return ErrRevisionConflict
	case -1:
		return ErrRevisionConflict
	default:
		return fmt.Errorf("%w: incompatible control plane record", ErrUnavailable)
	}
}

func (r *RedisRepository) appendTimed(ctx context.Context, key string, at time.Time, value any, retention time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(at.UnixMilli()), Member: string(raw)})
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(time.Now().Add(-retention).UnixMilli(), 10))
	pipe.Expire(ctx, key, retention)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("%w: append retained control event", ErrUnavailable)
	}
	return nil
}

func (r *RedisRepository) queryTimed(ctx context.Context, key string, from, to time.Time, offset, limit int, accept func(string) (bool, error)) (string, error) {
	if from.IsZero() {
		from = time.Unix(0, 0)
	}
	if to.IsZero() {
		to = time.Now().UTC()
	}
	raw, err := r.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: strconv.FormatInt(from.UnixMilli(), 10), Max: strconv.FormatInt(to.UnixMilli(), 10)}).Result()
	if err != nil {
		return "", fmt.Errorf("%w: query retained control events", ErrUnavailable)
	}
	matched := 0
	consumed := 0
	for index, current := range raw {
		if index < offset {
			continue
		}
		accepted, acceptErr := accept(current)
		if acceptErr != nil {
			return "", fmt.Errorf("%w: invalid retained control event", ErrUnavailable)
		}
		consumed++
		if accepted {
			matched++
			if matched == limit {
				if offset+consumed < len(raw) {
					return encodeCursor(offset + consumed), nil
				}
				return "", nil
			}
		}
	}
	return "", nil
}

func queryWindow(limit int, cursor string) (int, int, error) {
	if limit == 0 {
		limit = MaxQueryLimit
	}
	if limit < 1 || limit > MaxQueryLimit {
		return 0, 0, fmt.Errorf("query limit must be between 1 and %d", MaxQueryLimit)
	}
	offset := 0
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return 0, 0, errors.New("query cursor is invalid")
		}
		offset, err = strconv.Atoi(string(raw))
		if err != nil || offset < 0 {
			return 0, 0, errors.New("query cursor is invalid")
		}
	}
	return limit, offset, nil
}

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeStrict(raw string, target any) error {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("record must contain one JSON value")
	}
	return nil
}

func containsAll(values, required []string) bool {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	for _, value := range required {
		if _, exists := set[value]; !exists {
			return false
		}
	}
	return true
}

func asInt64(value any) int64 {
	switch current := value.(type) {
	case int64:
		return current
	case string:
		parsed, _ := strconv.ParseInt(current, 10, 64)
		return parsed
	default:
		parsed, _ := strconv.ParseInt(fmt.Sprint(current), 10, 64)
		return parsed
	}
}

func (r *RedisRepository) initializedKey(tenantID string) string {
	return r.prefix + ":tenant:" + tenantID + ":initialized"
}
func (r *RedisRepository) policyKey(tenantID string) string {
	return r.prefix + ":tenant:" + tenantID + ":policy"
}
func (r *RedisRepository) placementKey(tenantID string) string {
	return r.prefix + ":tenant:" + tenantID + ":placement"
}
func (r *RedisRepository) nodesKey() string             { return r.prefix + ":nodes" }
func (r *RedisRepository) nodeKey(nodeID string) string { return r.prefix + ":node:" + nodeID }
func (r *RedisRepository) nodeLeaseKey(nodeID string) string {
	return r.prefix + ":node:" + nodeID + ":lease"
}
func (r *RedisRepository) assignmentsKey() string { return r.prefix + ":assignments" }
func (r *RedisRepository) assignmentKey(inboxID string) string {
	return r.prefix + ":assignment:" + inboxID
}
func (r *RedisRepository) auditKey() string    { return r.prefix + ":audit" }
func (r *RedisRepository) auditIDsKey() string { return r.prefix + ":audit-ids" }
func (r *RedisRepository) metricKey() string   { return r.prefix + ":metrics" }
func (r *RedisRepository) degradedKey() string { return r.prefix + ":degraded" }
func (r *RedisRepository) confirmationKey(nonce string) string {
	return r.prefix + ":confirmation:" + nonce
}
func (r *RedisRepository) confirmationMatchKey(c Confirmation) string {
	return r.prefix + ":confirmation-match:" + digestConfirmationTuple(c.TenantID, c.ActorUserIDHash, c.SessionID, c.ToolName, c.ArgsDigest)
}
func digestConfirmationTuple(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
func (r *RedisRepository) leaseKey(name string) string { return r.prefix + ":lease:" + name }

var _ Repository = (*RedisRepository)(nil)
