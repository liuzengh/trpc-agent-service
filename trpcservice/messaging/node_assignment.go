package messaging

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
)

var deferNodeScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local wait_type=redis.call('TYPE',KEYS[2]); local stream_type=redis.call('TYPE',KEYS[3])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(wait_type)=='table' then wait_type=wait_type.ok end; if type(stream_type)=='table' then stream_type=stream_type.ok end
if inbox_type~='hash' or (wait_type~='none' and wait_type~='zset') or (stream_type~='none' and stream_type~='stream') then return -9 end
if redis.call('HGET',KEYS[1],'state')~='queued' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'digest')~=ARGV[2] or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[3] or redis.call('HGET',KEYS[1],'node_id')~=ARGV[4] then return 0 end
local member=KEYS[1]..'|'..ARGV[3]..'|'..ARGV[4]
redis.call('ZADD',KEYS[2],ARGV[5],member)
redis.call('HSET',KEYS[1],'state','node_wait','node_wait_until',ARGV[5],'node_wait_reason',ARGV[6],'node_wait_member',member)
redis.call('HDEL',KEYS[1],'stream_id','owner','lease_until')
redis.call('XDEL',KEYS[3],ARGV[3]); redis.call('XACK',KEYS[3],ARGV[7],ARGV[3]); return 1
`)

var promoteNodeScript = redis.NewScript(`
local wait_type=redis.call('TYPE',KEYS[1]); local inbox_type=redis.call('TYPE',KEYS[2]); local stream_type=redis.call('TYPE',KEYS[3])
if type(wait_type)=='table' then wait_type=wait_type.ok end; if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(stream_type)=='table' then stream_type=stream_type.ok end
if wait_type~='zset' or inbox_type~='hash' or (stream_type~='none' and stream_type~='stream') then return -9 end
if redis.call('ZSCORE',KEYS[1],ARGV[1])==false then return 0 end
if redis.call('HGET',KEYS[2],'state')~='node_wait' or redis.call('HGET',KEYS[2],'node_id')~=ARGV[3] or tonumber(redis.call('HGET',KEYS[2],'node_wait_until') or '0')>tonumber(ARGV[2]) then return 0 end
local payload=redis.call('HGET',KEYS[2],'payload'); local inbox_id=redis.call('HGET',KEYS[2],'inbox_id'); local digest=redis.call('HGET',KEYS[2],'digest')
if not payload or not inbox_id or not digest then redis.call('ZREM',KEYS[1],ARGV[1]); return 0 end
local stream_id=redis.call('XADD',KEYS[3],'*','payload',payload,'inbox_id',inbox_id,'node_id',ARGV[3])
redis.call('HSET',KEYS[2],'state','queued','stream_id',stream_id,'assignment_state','planned')
redis.call('HDEL',KEYS[2],'node_wait_until','node_wait_reason','node_wait_member')
redis.call('ZREM',KEYS[1],ARGV[1]); return 1
`)

var updateNodeAssignmentScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local wait_type=redis.call('TYPE',KEYS[2]); if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(wait_type)=='table' then wait_type=wait_type.ok end
if inbox_type~='hash' or (wait_type~='none' and wait_type~='zset') then return inbox_type=='none' and -1 or -9 end
if redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'digest')~=ARGV[2] then return -2 end
if tonumber(redis.call('HGET',KEYS[1],'assignment_revision') or '0')~=tonumber(ARGV[3]) then return 0 end
local state=redis.call('HGET',KEYS[1],'assignment_state') or 'planned'; if state=='completed' then return -3 end
if state=='admitted' or state=='running' then
  local inbox_state=redis.call('HGET',KEYS[1],'state'); local lease_until=tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')
  if (inbox_state=='processing' or inbox_state=='persisting') and lease_until>tonumber(ARGV[9]) then return -3 end
end
redis.call('HSET',KEYS[1],'node_id',ARGV[4],'assignment_revision',ARGV[5],'assignment_mode',ARGV[6],'assignment_state',ARGV[7],'assignment_payload_digest',ARGV[2])
if ARGV[7]=='blocked' then redis.call('HSET',KEYS[1],'blocked_reason',ARGV[8]) else redis.call('HDEL',KEYS[1],'blocked_reason') end
if redis.call('HGET',KEYS[1],'state')=='node_wait' then
  local old_member=redis.call('HGET',KEYS[1],'node_wait_member'); if old_member then
    local score=redis.call('ZSCORE',KEYS[2],old_member); redis.call('ZREM',KEYS[2],old_member)
    local first=string.find(old_member,'|'); local second=first and string.find(old_member,'|',first+1)
    if second then local new_member=string.sub(old_member,1,second)..ARGV[4]; redis.call('ZADD',KEYS[2],score or ARGV[9],new_member); redis.call('HSET',KEYS[1],'node_wait_member',new_member) end
  end
end
return 1
`)

var advanceNodeAssignmentScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); if type(inbox_type)=='table' then inbox_type=inbox_type.ok end
if inbox_type~='hash' then return -9 end
if redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'digest')~=ARGV[2] or redis.call('HGET',KEYS[1],'node_id')~=ARGV[3] then return 0 end
local current=redis.call('HGET',KEYS[1],'assignment_state')
if ARGV[4]=='running' and current~='admitted' then return 0 end
if ARGV[4]=='completed' and current~='running' and current~='admitted' then return 0 end
redis.call('HSET',KEYS[1],'assignment_state',ARGV[4]); return 1
`)

var backfillNodeAssignmentScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); if type(inbox_type)=='table' then inbox_type=inbox_type.ok end
if inbox_type~='hash' then return inbox_type=='none' and -1 or -9 end
if redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'digest')~=ARGV[2] then return -2 end
if redis.call('HGET',KEYS[1],'assignment_revision') then return 0 end
if redis.call('HGET',KEYS[1],'state')~='queued' then return -3 end
redis.call('HSET',KEYS[1],'node_id',ARGV[3],'assignment_revision',ARGV[4],'assignment_mode',ARGV[5],'assignment_state',ARGV[6],'assignment_payload_digest',ARGV[2]); return 1
`)

var admitNodeAssignmentScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); if type(inbox_type)=='table' then inbox_type=inbox_type.ok end
if inbox_type~='hash' then return -9 end
if redis.call('HGET',KEYS[1],'state')~='queued' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'digest')~=ARGV[2] or redis.call('HGET',KEYS[1],'node_id')~=ARGV[3] or redis.call('HGET',KEYS[1],'assignment_payload_digest')~=ARGV[2] then return 0 end
redis.call('HSET',KEYS[1],'assignment_state','admitted'); return 1
`)

func (s *Store) DeferNode(ctx context.Context, delivery Delivery, nodeID string, due time.Time, reason string) error {
	if due.IsZero() {
		due = time.Now().UTC()
	}
	result, err := deferNodeScript.Run(ctx, s.client, []string{s.inboxKey(delivery.InboxID), s.nodeWaitKey, s.taskStream}, delivery.Task.TaskID, delivery.Task.PayloadDigest, delivery.StreamID, nodeID, due.UnixMilli(), reason, workerGroup).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) PromoteNodeWait(ctx context.Context, nodeID string, limit int) (int, error) {
	if strings.TrimSpace(nodeID) == "" {
		return 0, errors.New("node id is required")
	}
	if limit <= 0 {
		limit = 32
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return 0, err
	}
	members, err := s.client.ZRangeByScore(ctx, s.nodeWaitKey, &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Count: int64(limit)}).Result()
	if err != nil {
		return 0, err
	}
	promoted := 0
	for _, member := range members {
		parts := strings.Split(member, "|")
		if len(parts) != 3 {
			_ = s.client.ZRem(ctx, s.nodeWaitKey, member).Err()
			continue
		}
		result, runErr := promoteNodeScript.Run(ctx, s.client, []string{s.nodeWaitKey, parts[0], s.taskStream}, member, now.UnixMilli(), nodeID).Int()
		if runErr != nil {
			return promoted, runErr
		}
		if result == -9 {
			return promoted, ErrKeyType
		}
		if result == 1 {
			promoted++
		}
	}
	return promoted, nil
}

func (s *Store) UpdateNodeAssignment(ctx context.Context, inboxID, taskID string, assignment control.NodeAssignment, expectedRevision int64) error {
	if err := assignment.Validate(); err != nil {
		return err
	}
	if assignment.InboxID != inboxID {
		return control.ErrAssignmentConflict
	}
	result, err := updateNodeAssignmentScript.Run(ctx, s.client, []string{s.inboxKey(inboxID), s.nodeWaitKey}, taskID, assignment.PayloadDigest, expectedRevision, assignment.NodeID, assignment.Revision, string(assignment.Mode), string(assignment.State), assignment.BlockedReason, time.Now().UnixMilli()).Int()
	if err != nil {
		return err
	}
	switch result {
	case 1:
		return nil
	case 0:
		return control.ErrRevisionConflict
	case -1:
		return ErrInboxMissing
	case -2:
		return control.ErrAssignmentConflict
	case -3:
		return control.ErrAssignmentNotOverridable
	default:
		return ErrKeyType
	}
}

func (s *Store) AdvanceNodeAssignment(ctx context.Context, delivery Delivery, nodeID string, state control.AssignmentState) error {
	if state != control.AssignmentRunning && state != control.AssignmentCompleted {
		return errors.New("invalid internal assignment transition")
	}
	result, err := advanceNodeAssignmentScript.Run(ctx, s.client, []string{s.inboxKey(delivery.InboxID)}, delivery.Task.TaskID, delivery.Task.PayloadDigest, nodeID, string(state)).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) AdmitNodeAssignment(ctx context.Context, delivery Delivery, nodeID string) error {
	result, err := admitNodeAssignmentScript.Run(ctx, s.client, []string{s.inboxKey(delivery.InboxID)}, delivery.Task.TaskID, delivery.Task.PayloadDigest, nodeID).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) BackfillSharedAssignment(ctx context.Context, delivery Delivery, assignment control.NodeAssignment) error {
	if err := assignment.Validate(); err != nil {
		return err
	}
	if assignment.InboxID != delivery.InboxID || assignment.PayloadDigest != delivery.Task.PayloadDigest {
		return control.ErrAssignmentConflict
	}
	result, err := backfillNodeAssignmentScript.Run(ctx, s.client, []string{s.inboxKey(delivery.InboxID)}, delivery.Task.TaskID, delivery.Task.PayloadDigest, assignment.NodeID, assignment.Revision, string(assignment.Mode), string(assignment.State)).Int()
	if err != nil {
		return err
	}
	switch result {
	case 1, 0:
		return nil
	case -1:
		return ErrInboxMissing
	case -2:
		return control.ErrAssignmentConflict
	case -3:
		return ErrLeaseLost
	default:
		return ErrKeyType
	}
}
