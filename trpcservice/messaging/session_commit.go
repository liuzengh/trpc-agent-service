package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/keyspace"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
)

var commitTurnScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1]); local lock_type = redis.call('TYPE', KEYS[2]); local state_type = redis.call('TYPE', KEYS[3])
local events_type = redis.call('TYPE', KEYS[4]); local meta_type = redis.call('TYPE', KEYS[5]); local index_type = redis.call('TYPE', KEYS[6]); local task_type = redis.call('TYPE', KEYS[7]); local reply_type = redis.call('TYPE', KEYS[8])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end
if type(events_type)=='table' then events_type=events_type.ok end; if type(meta_type)=='table' then meta_type=meta_type.ok end; if type(index_type)=='table' then index_type=index_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end; if type(reply_type)=='table' then reply_type=reply_type.ok end
if inbox_type~='hash' or (lock_type~='none' and lock_type~='hash') or state_type~='hash' or
   (events_type~='none' and events_type~='list') or (meta_type~='none' and meta_type~='hash') or
   (index_type~='none' and index_type~='hash') or (task_type~='none' and task_type~='stream') or
   (reply_type~='none' and reply_type~='stream') then return -9 end
if redis.call('HGET',KEYS[1],'state')~='processing' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[3]) or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] then return 0 end
if redis.call('HGET',KEYS[1],'session_coord')~=ARGV[12] or tonumber(redis.call('HGET',KEYS[1],'session_seq') or '0')~=tonumber(ARGV[6]) or redis.call('HGET',KEYS[1],'digest')~=ARGV[21] or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')<=tonumber(ARGV[22]) then return 0 end
if redis.call('HGET',KEYS[2],'token')~=ARGV[5] or redis.call('HGET',KEYS[2],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[2],'lease_epoch') or '0')~=tonumber(ARGV[3]) or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')<=tonumber(ARGV[22]) then return -3 end
local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if tonumber(ARGV[6])~=last+1 then return -2 end
local ok, events=pcall(cjson.decode,ARGV[7]); if not ok or type(events)~='table' or #events~=tonumber(ARGV[17]) or #ARGV[7]>tonumber(ARGV[20]) or tonumber(ARGV[17])>tonumber(ARGV[19]) then return -4 end
local state_ok,state=pcall(cjson.decode,ARGV[8]); if not state_ok or type(state)~='table' then return -4 end
local result_ok,result=pcall(cjson.decode,ARGV[14]); if not result_ok or type(result)~='table' or result['task_id']~=ARGV[1] then return -4 end
for _,e in ipairs(events) do if type(e)~='table' then return -4 end end
for _,e in ipairs(events) do redis.call('RPUSH',KEYS[4],cjson.encode(e)) end
local created = redis.call('HGET',KEYS[5],'created_at'); if not created then created=ARGV[13] end
redis.call('HSET',KEYS[5],'app_name',ARGV[9],'user_id',ARGV[10],'session_id',ARGV[11],'session_coord',ARGV[12],'state_json',ARGV[8],'updated_at',ARGV[13],'created_at',created,'last_committed_seq',ARGV[6])
redis.call('HSET',KEYS[6],ARGV[11],ARGV[12]); redis.call('HSET',KEYS[3],'last_completed_seq',ARGV[6],'updated_at_ms',ARGV[22])
redis.call('HSET',KEYS[1],'state','succeeded','result',ARGV[14],'error_code',''); redis.call('HDEL',KEYS[1],'payload','owner','lease_until','stream_id'); redis.call('EXPIRE',KEYS[1],ARGV[15])
redis.call('XADD',KEYS[8],'*','payload',ARGV[14]); redis.call('XDEL',KEYS[7],ARGV[4]); redis.call('XACK',KEYS[7],ARGV[16],ARGV[4]); redis.call('DEL',KEYS[2]); return 1
`)

func (s *Store) CompleteTurn(ctx context.Context, lease Lease, reply message.OutboundMessage, commit sessionfence.TurnCommit) error {
	task := lease.Delivery.Task
	expectedApp := tenant.AppName(task.TenantID, task.AgentAppID)
	expectedUserCoord := keyspace.UserCoord(commit.AppName, commit.UserID)
	if commit.SessionCoord != lease.SessionCoord || commit.SessionSeq != lease.SessionSeq ||
		commit.AppName != expectedApp || commit.UserID != task.RunnerUserID || commit.SessionID != task.SessionID ||
		(commit.UserCoord != "" && commit.UserCoord != expectedUserCoord) {
		return ErrLeaseLost
	}
	if reply.Channel == "" {
		reply.Channel = lease.Delivery.Task.Channel
	}
	if reply.BindingID == "" {
		reply.BindingID = lease.Delivery.Task.ChannelBindingID
	}
	target := lease.Delivery.Task.DeliveryTarget()
	reply = target.Apply(reply)
	result := message.TaskResult{
		SchemaVersion: message.TaskSchemaVersion, TaskID: task.TaskID, Succeeded: true,
		Channel: target.Channel, BindingID: target.ChannelBindingID, Reply: reply,
		TraceID: task.TraceID, TraceParent: task.TraceParent, DigestVersion: task.DigestVersion,
	}
	if target.Valid() {
		result.Target = target
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	eventList := commit.Events
	if eventList == nil {
		eventList = []event.Event{}
	}
	events, err := json.Marshal(eventList)
	if err != nil {
		return err
	}
	state, err := json.Marshal(commit.FinalState)
	if err != nil {
		return err
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	userCoord := expectedUserCoord
	resultCode, err := commitTurnScript.Run(ctx, s.client, []string{lease.InboxKey, s.sessionLockKey(lease.SessionCoord), s.sessionStateKey(lease.SessionCoord), s.fencedEventsKey(commit.SessionCoord), s.fencedMetaKey(commit.SessionCoord), s.fencedIndexKey(userCoord), s.taskStream, s.replyStream}, lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID, lease.LockToken, commit.SessionSeq, string(events), string(state), commit.AppName, commit.UserID, commit.SessionID, commit.SessionCoord, now.UTC().Format(time.RFC3339Nano), string(payload), int64(s.config.InboxRetention/time.Second), workerGroup, len(eventList), len(events), s.config.MaxTurnEvents, s.config.MaxTurnBytes, lease.Delivery.Task.PayloadDigest, now.UnixMilli()).Int()
	if err != nil {
		return err
	}
	if resultCode == -9 {
		return ErrKeyType
	}
	if resultCode != 1 {
		return fmt.Errorf("%w: commit code %d", ErrLeaseLost, resultCode)
	}
	return nil
}

func (s *Store) fencedEventsKey(coord string) string {
	return keyspace.FencedEvents(s.config.KeyPrefix, coord)
}
func (s *Store) fencedMetaKey(coord string) string {
	return keyspace.FencedMeta(s.config.KeyPrefix, coord)
}
func (s *Store) fencedIndexKey(userCoord string) string {
	return keyspace.FencedUserIndex(s.config.KeyPrefix, userCoord)
}
