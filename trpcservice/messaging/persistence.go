package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
)

var preparePersistenceScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local state_type=redis.call('TYPE',KEYS[3]); local task_type=redis.call('TYPE',KEYS[4])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end
if inbox_type~='hash' or lock_type~='hash' or state_type~='hash' or task_type~='stream' then return -9 end
if redis.call('HGET',KEYS[1],'state')~='processing' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[3]) or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] then return 0 end
if redis.call('HGET',KEYS[1],'session_coord')~=ARGV[15] or tonumber(redis.call('HGET',KEYS[1],'session_seq') or '0')~=tonumber(ARGV[6]) or redis.call('HGET',KEYS[1],'digest')~=ARGV[7] or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')<=tonumber(ARGV[8]) then return 0 end
if redis.call('HGET',KEYS[2],'token')~=ARGV[5] or redis.call('HGET',KEYS[2],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[2],'lease_epoch') or '0')~=tonumber(ARGV[3]) or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')<=tonumber(ARGV[8]) then return -3 end
local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if tonumber(ARGV[6])~=last+1 then return -2 end
if #ARGV[9]>tonumber(ARGV[12]) then return -4 end
local ok,envelope=pcall(cjson.decode,ARGV[9]); if not ok or type(envelope)~='table' or envelope['task_id']~=ARGV[1] or envelope['payload_digest']~=ARGV[7] or envelope['session_coord']~=ARGV[15] or tonumber(envelope['session_seq'] or '0')~=tonumber(ARGV[6]) or envelope['envelope_digest']~=ARGV[10] or tonumber(envelope['persist_attempt'] or '0')~=tonumber(ARGV[11]) or envelope['backend_kind']~=ARGV[13] or envelope['storage_profile_id']~=ARGV[14] then return -4 end
redis.call('HSET',KEYS[1],'state','persisting','persistence_envelope',ARGV[9],'envelope_digest',ARGV[10],'persist_attempt',ARGV[11],'backend_kind',ARGV[13],'storage_profile_id',ARGV[14])
return 1
`)

var beginPersistenceScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local state_type=redis.call('TYPE',KEYS[3])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end
if inbox_type~='hash' or (lock_type~='none' and lock_type~='hash') or state_type~='hash' then return {-9,0} end
if redis.call('HGET',KEYS[1],'state')~='persisting' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'digest')~=ARGV[2] or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] or redis.call('HGET',KEYS[1],'session_coord')~=ARGV[9] or tonumber(redis.call('HGET',KEYS[1],'session_seq') or '0')~=tonumber(ARGV[10]) or redis.call('HGET',KEYS[1],'envelope_digest')~=ARGV[11] then return {0,0} end
if redis.call('HGET',KEYS[1],'owner') and tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')>tonumber(ARGV[12]) then return {-3,0} end
if lock_type=='hash' and tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')>tonumber(ARGV[12]) then return {-3,0} end
local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if tonumber(ARGV[10])~=last+1 then return {-4,0} end
local epoch=redis.call('HINCRBY',KEYS[1],'lease_epoch',1); redis.call('HSET',KEYS[1],'owner',ARGV[3],'lease_until',ARGV[6])
redis.call('HSET',KEYS[2],'token',ARGV[5],'task_id',ARGV[1],'owner',ARGV[3],'lease_epoch',epoch,'session_seq',ARGV[10],'expires_at_ms',ARGV[8]); redis.call('PEXPIRE',KEYS[2],ARGV[7])
return {1,epoch}
`)

var finalizePersistenceScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local state_type=redis.call('TYPE',KEYS[3]); local reply_type=redis.call('TYPE',KEYS[4]); local task_type=redis.call('TYPE',KEYS[5])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end; if type(reply_type)=='table' then reply_type=reply_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end
if inbox_type~='hash' or lock_type~='hash' or state_type~='hash' or (reply_type~='none' and reply_type~='stream') or task_type~='stream' then return -9 end
if redis.call('HGET',KEYS[1],'state')~='persisting' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[3]) or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] or redis.call('HGET',KEYS[1],'digest')~=ARGV[7] or redis.call('HGET',KEYS[1],'envelope_digest')~=ARGV[8] or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')<=tonumber(ARGV[12]) then return 0 end
if redis.call('HGET',KEYS[2],'token')~=ARGV[5] or redis.call('HGET',KEYS[2],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[2],'lease_epoch') or '0')~=tonumber(ARGV[3]) or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')<=tonumber(ARGV[12]) then return -3 end
local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if tonumber(ARGV[6])~=last+1 then return -2 end
local ok,result=pcall(cjson.decode,ARGV[9]); if not ok or type(result)~='table' or result['task_id']~=ARGV[1] then return -4 end
redis.call('HSET',KEYS[3],'last_completed_seq',ARGV[6],'updated_at_ms',ARGV[12]); redis.call('HSET',KEYS[1],'state','succeeded','result',ARGV[9],'error_code','')
redis.call('HDEL',KEYS[1],'payload','owner','lease_until','stream_id','next_attempt_at','persistence_envelope','envelope_digest','persist_attempt','backend_kind','storage_profile_id','last_error'); redis.call('EXPIRE',KEYS[1],ARGV[10])
redis.call('XADD',KEYS[4],'*','payload',ARGV[9]); redis.call('XDEL',KEYS[5],ARGV[4]); redis.call('XACK',KEYS[5],ARGV[11],ARGV[4]); redis.call('DEL',KEYS[2]); return 1
`)

var deferPersistenceScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local retry_type=redis.call('TYPE',KEYS[3]); local task_type=redis.call('TYPE',KEYS[4])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(retry_type)=='table' then retry_type=retry_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end
if inbox_type~='hash' or lock_type~='hash' or (retry_type~='none' and retry_type~='zset') or task_type~='stream' then return -9 end
if redis.call('HGET',KEYS[1],'state')~='persisting' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[3]) or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] or redis.call('HGET',KEYS[1],'digest')~=ARGV[7] or redis.call('HGET',KEYS[1],'envelope_digest')~=ARGV[8] or tonumber(redis.call('HGET',KEYS[1],'persist_attempt') or '0')~=tonumber(ARGV[9]) or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')<=tonumber(ARGV[15]) then return 0 end
if redis.call('HGET',KEYS[2],'token')~=ARGV[5] or redis.call('HGET',KEYS[2],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[2],'lease_epoch') or '0')~=tonumber(ARGV[3]) or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')<=tonumber(ARGV[15]) then return -3 end
local ok,envelope=pcall(cjson.decode,ARGV[10]); if not ok or type(envelope)~='table' or envelope['task_id']~=ARGV[1] or envelope['envelope_digest']~=ARGV[8] or tonumber(envelope['persist_attempt'] or '0')~=tonumber(ARGV[11]) then return -4 end
redis.call('HSET',KEYS[1],'state','persist_retry_wait','persistence_envelope',ARGV[10],'persist_attempt',ARGV[11],'next_attempt_at',ARGV[12],'last_error',ARGV[13]); redis.call('HDEL',KEYS[1],'owner','lease_until','stream_id')
redis.call('ZADD',KEYS[3],ARGV[12],KEYS[1]); redis.call('XDEL',KEYS[4],ARGV[4]); redis.call('XACK',KEYS[4],ARGV[14],ARGV[4]); redis.call('DEL',KEYS[2]); return 1
`)

var promotePersistenceScript = redis.NewScript(`
local retry_type=redis.call('TYPE',KEYS[1]); local inbox_type=redis.call('TYPE',KEYS[2]); local task_type=redis.call('TYPE',KEYS[3])
if type(retry_type)=='table' then retry_type=retry_type.ok end; if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end
if retry_type~='zset' or inbox_type~='hash' or (task_type~='none' and task_type~='stream') then return -9 end
local score=redis.call('ZSCORE',KEYS[1],KEYS[2]); if not score then return 0 end
if tonumber(score)>tonumber(ARGV[1]) or redis.call('HGET',KEYS[2],'state')~='persist_retry_wait' or redis.call('HGET',KEYS[2],'stream_id') then return 0 end
local payload=redis.call('HGET',KEYS[2],'payload'); local inboxID=redis.call('HGET',KEYS[2],'inbox_id'); if not payload or not inboxID then return -4 end
local streamID=redis.call('XADD',KEYS[3],'*','payload',payload,'inbox_id',inboxID); redis.call('HSET',KEYS[2],'state','persisting','stream_id',streamID); redis.call('HDEL',KEYS[2],'next_attempt_at'); redis.call('ZREM',KEYS[1],KEYS[2]); return 1
`)

var failPersistenceScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local state_type=redis.call('TYPE',KEYS[3]); local reply_type=redis.call('TYPE',KEYS[4]); local task_type=redis.call('TYPE',KEYS[5]); local retry_type=redis.call('TYPE',KEYS[6])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end; if type(reply_type)=='table' then reply_type=reply_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end; if type(retry_type)=='table' then retry_type=retry_type.ok end
if inbox_type~='hash' or lock_type~='hash' or state_type~='hash' or (reply_type~='none' and reply_type~='stream') or task_type~='stream' or (retry_type~='none' and retry_type~='zset') then return -9 end
if redis.call('HGET',KEYS[1],'state')~='persisting' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[3]) or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] or redis.call('HGET',KEYS[1],'digest')~=ARGV[7] or redis.call('HGET',KEYS[1],'envelope_digest')~=ARGV[8] or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')<=tonumber(ARGV[13]) then return 0 end
if redis.call('HGET',KEYS[2],'token')~=ARGV[5] or redis.call('HGET',KEYS[2],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[2],'lease_epoch') or '0')~=tonumber(ARGV[3]) or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')<=tonumber(ARGV[13]) then return -3 end
local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if tonumber(ARGV[6])~=last+1 then return -2 end
local ok,result=pcall(cjson.decode,ARGV[9]); if not ok or type(result)~='table' or result['task_id']~=ARGV[1] then return -4 end
redis.call('HSET',KEYS[3],'last_completed_seq',ARGV[6],'updated_at_ms',ARGV[13]); redis.call('HSET',KEYS[1],'state','failed_terminal','result',ARGV[9],'error_code',ARGV[10])
redis.call('HDEL',KEYS[1],'payload','owner','lease_until','stream_id','next_attempt_at','persistence_envelope','envelope_digest','persist_attempt','backend_kind','storage_profile_id','last_error'); redis.call('EXPIRE',KEYS[1],ARGV[11]); redis.call('ZREM',KEYS[6],KEYS[1])
redis.call('XADD',KEYS[4],'*','payload',ARGV[9]); redis.call('XDEL',KEYS[5],ARGV[4]); redis.call('XACK',KEYS[5],ARGV[12],ARGV[4]); redis.call('DEL',KEYS[2]); return 1
`)

var failCorruptPersistenceScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local state_type=redis.call('TYPE',KEYS[3]); local reply_type=redis.call('TYPE',KEYS[4]); local task_type=redis.call('TYPE',KEYS[5]); local retry_type=redis.call('TYPE',KEYS[6])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end; if type(reply_type)=='table' then reply_type=reply_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end; if type(retry_type)=='table' then retry_type=retry_type.ok end
if inbox_type~='hash' or lock_type~='hash' or state_type~='hash' or (reply_type~='none' and reply_type~='stream') or task_type~='stream' or (retry_type~='none' and retry_type~='zset') then return -9 end
if redis.call('HGET',KEYS[1],'state')~='persisting' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[3]) or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] or redis.call('HGET',KEYS[1],'digest')~=ARGV[7] or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')<=tonumber(ARGV[12]) then return 0 end
if redis.call('HGET',KEYS[1],'session_coord')~=ARGV[8] or tonumber(redis.call('HGET',KEYS[1],'session_seq') or '0')~=tonumber(ARGV[6]) then return 0 end
if redis.call('HGET',KEYS[2],'token')~=ARGV[5] or redis.call('HGET',KEYS[2],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[2],'lease_epoch') or '0')~=tonumber(ARGV[3]) or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')<=tonumber(ARGV[12]) then return -3 end
local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if tonumber(ARGV[6])~=last+1 then return -2 end
local ok,result=pcall(cjson.decode,ARGV[9]); if not ok or type(result)~='table' or result['task_id']~=ARGV[1] then return -4 end
redis.call('HSET',KEYS[3],'last_completed_seq',ARGV[6],'updated_at_ms',ARGV[12]); redis.call('HSET',KEYS[1],'state','failed_terminal','result',ARGV[9],'error_code',ARGV[10])
redis.call('HDEL',KEYS[1],'payload','owner','lease_until','stream_id','next_attempt_at','persistence_envelope','envelope_digest','persist_attempt','backend_kind','storage_profile_id','last_error'); redis.call('EXPIRE',KEYS[1],ARGV[11]); redis.call('ZREM',KEYS[6],KEYS[1])
redis.call('XADD',KEYS[4],'*','payload',ARGV[9]); redis.call('XDEL',KEYS[5],ARGV[4]); redis.call('XACK',KEYS[5],ARGV[13],ARGV[4]); redis.call('DEL',KEYS[2]); return 1
`)

var recoverPersistenceScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local task_type=redis.call('TYPE',KEYS[3])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end
if inbox_type~='hash' or (lock_type~='none' and lock_type~='hash') or task_type~='stream' then return -9 end
if redis.call('HGET',KEYS[1],'state')~='persisting' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[2] or redis.call('HGET',KEYS[1],'digest')~=ARGV[3] or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')>tonumber(ARGV[4]) then return 0 end
if lock_type=='hash' and tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')>tonumber(ARGV[4]) then return -3 end
local payload=redis.call('HGET',KEYS[1],'payload'); local inboxID=redis.call('HGET',KEYS[1],'inbox_id'); if not payload or not inboxID then return -4 end
local claimed=redis.call('XCLAIM',KEYS[3],ARGV[5],ARGV[6],0,ARGV[2],'JUSTID'); if not claimed or #claimed~=1 then return 0 end
local newID=redis.call('XADD',KEYS[3],'*','payload',payload,'inbox_id',inboxID); redis.call('HSET',KEYS[1],'state','persisting','stream_id',newID); redis.call('HDEL',KEYS[1],'owner','lease_until'); redis.call('XDEL',KEYS[3],ARGV[2]); redis.call('XACK',KEYS[3],ARGV[5],ARGV[2]); if lock_type=='hash' then redis.call('DEL',KEYS[2]) end; return 1
`)

func (s *Store) PreparePersistence(ctx context.Context, lease Lease, route persistence.Route, reply message.OutboundMessage, commit sessionfence.TurnCommit) (persistence.Envelope, error) {
	if !route.IsSQL() || route.TenantID != lease.Delivery.Task.TenantID || route.AgentAppID != lease.Delivery.Task.AgentAppID {
		return persistence.Envelope{}, persistence.ErrInvalidEnvelope
	}
	if err := validateTurnCommit(lease, commit); err != nil {
		return persistence.Envelope{}, err
	}
	reply = lease.Delivery.Task.DeliveryTarget().Apply(reply)
	now, err := s.redisTime(ctx)
	if err != nil {
		return persistence.Envelope{}, err
	}
	envelope, err := persistence.NewEnvelope(lease.Delivery.Task, route, commit, reply, now)
	if err != nil {
		return persistence.Envelope{}, err
	}
	raw, err := json.Marshal(envelope)
	if err != nil || len(raw) > s.config.PersistencePayloadMaxBytes {
		return persistence.Envelope{}, persistence.ErrInvalidEnvelope
	}
	result, err := preparePersistenceScript.Run(ctx, s.client, []string{lease.InboxKey, s.sessionLockKey(lease.SessionCoord), s.sessionStateKey(lease.SessionCoord), s.taskStream},
		lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID, lease.LockToken, lease.SessionSeq,
		lease.Delivery.Task.PayloadDigest, now.UnixMilli(), string(raw), envelope.EnvelopeDigest, envelope.PersistAttempt,
		s.config.PersistencePayloadMaxBytes, string(envelope.BackendKind), envelope.StorageProfileID, envelope.SessionCoord).Int()
	if err != nil {
		return persistence.Envelope{}, err
	}
	if result == -9 {
		return persistence.Envelope{}, ErrKeyType
	}
	if result != 1 {
		return persistence.Envelope{}, fmt.Errorf("%w: prepare persistence code %d", ErrLeaseLost, result)
	}
	return envelope, nil
}

func validateTurnCommit(lease Lease, commit sessionfence.TurnCommit) error {
	task := lease.Delivery.Task
	if commit.SessionCoord != lease.SessionCoord || commit.SessionSeq != lease.SessionSeq || commit.AppName != tenant.AppName(task.TenantID, task.AgentAppID) || commit.UserID != task.RunnerUserID || commit.SessionID != task.SessionID {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) BeginPersistence(ctx context.Context, delivery Delivery, consumer string) (Lease, persistence.Envelope, error) {
	snapshot, err := s.Snapshot(ctx, delivery.InboxID)
	if err != nil {
		return Lease{}, persistence.Envelope{}, err
	}
	if snapshot.SessionCoord != sessionCoord(delivery.Task) || snapshot.SessionSeq < 1 {
		return Lease{}, persistence.Envelope{}, persistence.ErrInvalidEnvelope
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return Lease{}, persistence.Envelope{}, err
	}
	token := randomToken()
	values, err := beginPersistenceScript.Run(ctx, s.client, []string{s.inboxKey(delivery.InboxID), s.sessionLockKey(snapshot.SessionCoord), s.sessionStateKey(snapshot.SessionCoord)},
		delivery.Task.TaskID, delivery.Task.PayloadDigest, consumer, delivery.StreamID, token,
		now.Add(s.config.LeaseDuration).UnixMilli(), s.config.SessionLockDuration.Milliseconds(), now.Add(s.config.SessionLockDuration).UnixMilli(),
		snapshot.SessionCoord, snapshot.SessionSeq, snapshot.EnvelopeDigest, now.UnixMilli()).Slice()
	if err != nil {
		return Lease{}, persistence.Envelope{}, err
	}
	if len(values) != 2 {
		return Lease{}, persistence.Envelope{}, ErrLeaseLost
	}
	code := asInt64(values[0])
	if code == -9 {
		return Lease{}, persistence.Envelope{}, ErrKeyType
	}
	if code == -3 {
		return Lease{}, persistence.Envelope{}, ErrSessionBusy
	}
	if code != 1 {
		return Lease{}, persistence.Envelope{}, ErrLeaseLost
	}
	lease := Lease{Delivery: delivery, InboxKey: s.inboxKey(delivery.InboxID), Owner: consumer, Epoch: asInt64(values[1]), SessionCoord: snapshot.SessionCoord, SessionSeq: snapshot.SessionSeq, LockToken: token}
	var envelope persistence.Envelope
	if decodeStrictJSON(snapshot.RawEnvelope, &envelope) != nil || envelope.Validate() != nil || envelope.EnvelopeDigest != snapshot.EnvelopeDigest || envelope.PersistAttempt != snapshot.PersistAttempt || envelope.TaskID != delivery.Task.TaskID || envelope.PayloadDigest != delivery.Task.PayloadDigest {
		if failErr := s.FailCorruptPersistence(ctx, lease, "persistence_envelope_invalid"); failErr != nil {
			return Lease{}, persistence.Envelope{}, failErr
		}
		return Lease{}, persistence.Envelope{}, persistence.ErrInvalidEnvelope
	}
	return lease, envelope, nil
}

func (s *Store) FinalizePersistence(ctx context.Context, lease Lease, envelope persistence.Envelope) error {
	if envelope.Validate() != nil || envelope.TaskID != lease.Delivery.Task.TaskID || envelope.SessionCoord != lease.SessionCoord || envelope.SessionSeq != lease.SessionSeq {
		return persistence.ErrInvalidEnvelope
	}
	target := lease.Delivery.Task.DeliveryTarget()
	reply := target.Apply(envelope.Reply)
	result := message.TaskResult{SchemaVersion: message.TaskSchemaVersion, TaskID: envelope.TaskID, Succeeded: true, Channel: target.Channel, BindingID: target.ChannelBindingID, Reply: reply, TraceID: lease.Delivery.Task.TraceID, TraceParent: lease.Delivery.Task.TraceParent, DigestVersion: lease.Delivery.Task.DigestVersion}
	if target.Valid() {
		result.Target = target
	}
	rawResult, err := json.Marshal(result)
	if err != nil {
		return err
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	code, err := finalizePersistenceScript.Run(ctx, s.client, []string{lease.InboxKey, s.sessionLockKey(lease.SessionCoord), s.sessionStateKey(lease.SessionCoord), s.replyStream, s.taskStream},
		envelope.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID, lease.LockToken, lease.SessionSeq,
		envelope.PayloadDigest, envelope.EnvelopeDigest, string(rawResult), int64(s.config.InboxRetention/time.Second), workerGroup, now.UnixMilli()).Int()
	if err != nil {
		return err
	}
	if code == -9 {
		return ErrKeyType
	}
	if code != 1 {
		return fmt.Errorf("%w: finalize persistence code %d", ErrLeaseLost, code)
	}
	return nil
}

func (s *Store) DeferPersistence(ctx context.Context, lease Lease, envelope persistence.Envelope, errorCode string) (persistence.Envelope, error) {
	if envelope.Validate() != nil || envelope.PersistAttempt < 1 {
		return persistence.Envelope{}, persistence.ErrInvalidEnvelope
	}
	oldAttempt := envelope.PersistAttempt
	envelope.PersistAttempt++
	raw, err := json.Marshal(envelope)
	if err != nil || len(raw) > s.config.PersistencePayloadMaxBytes {
		return persistence.Envelope{}, persistence.ErrInvalidEnvelope
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return persistence.Envelope{}, err
	}
	due := now.Add(s.persistenceBackoff(oldAttempt))
	code, err := deferPersistenceScript.Run(ctx, s.client, []string{lease.InboxKey, s.sessionLockKey(lease.SessionCoord), s.persistenceRetryKey, s.taskStream},
		envelope.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID, lease.LockToken, lease.SessionSeq,
		envelope.PayloadDigest, envelope.EnvelopeDigest, oldAttempt, string(raw), envelope.PersistAttempt, due.UnixMilli(), errorCode, workerGroup, now.UnixMilli()).Int()
	if err != nil {
		return persistence.Envelope{}, err
	}
	if code == -9 {
		return persistence.Envelope{}, ErrKeyType
	}
	if code != 1 {
		return persistence.Envelope{}, ErrLeaseLost
	}
	return envelope, nil
}

func (s *Store) PromotePersistenceRetries(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 32
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return 0, err
	}
	members, err := s.client.ZRangeByScore(ctx, s.persistenceRetryKey, &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Count: int64(limit)}).Result()
	if err != nil {
		return 0, err
	}
	promoted := 0
	for _, inboxKey := range members {
		code, scriptErr := promotePersistenceScript.Run(ctx, s.client, []string{s.persistenceRetryKey, inboxKey, s.taskStream}, now.UnixMilli()).Int()
		if scriptErr != nil {
			return promoted, scriptErr
		}
		if code == -9 {
			return promoted, ErrKeyType
		}
		if code == -4 {
			return promoted, persistence.ErrInvalidEnvelope
		}
		if code == 1 {
			promoted++
		}
	}
	return promoted, nil
}

func (s *Store) FailPersistence(ctx context.Context, lease Lease, envelope persistence.Envelope, errorCode string) error {
	if envelope.Validate() != nil || errorCode == "" {
		return persistence.ErrInvalidEnvelope
	}
	target := lease.Delivery.Task.DeliveryTarget()
	result := message.TaskResult{SchemaVersion: message.TaskSchemaVersion, TaskID: envelope.TaskID, Channel: target.Channel, BindingID: target.ChannelBindingID, ErrorCode: errorCode, TraceID: lease.Delivery.Task.TraceID, TraceParent: lease.Delivery.Task.TraceParent, DigestVersion: lease.Delivery.Task.DigestVersion}
	if target.Valid() {
		result.Target = target
	}
	rawResult, _ := json.Marshal(result)
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	code, err := failPersistenceScript.Run(ctx, s.client, []string{lease.InboxKey, s.sessionLockKey(lease.SessionCoord), s.sessionStateKey(lease.SessionCoord), s.replyStream, s.taskStream, s.persistenceRetryKey},
		envelope.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID, lease.LockToken, lease.SessionSeq, envelope.PayloadDigest, envelope.EnvelopeDigest,
		string(rawResult), errorCode, int64(s.config.InboxRetention/time.Second), workerGroup, now.UnixMilli()).Int()
	if err != nil {
		return err
	}
	if code == -9 {
		return ErrKeyType
	}
	if code != 1 {
		return ErrLeaseLost
	}
	return nil
}

// FailCorruptPersistence terminalizes an owned persisting turn whose complete
// envelope can no longer be trusted. It deliberately relies only on the
// original task and Redis fencing coordinates established before corruption.
func (s *Store) FailCorruptPersistence(ctx context.Context, lease Lease, errorCode string) error {
	if lease.Delivery.Task.Validate() != nil || lease.SessionCoord != sessionCoord(lease.Delivery.Task) || lease.SessionSeq < 1 || errorCode == "" {
		return persistence.ErrInvalidEnvelope
	}
	target := lease.Delivery.Task.DeliveryTarget()
	result := message.TaskResult{SchemaVersion: message.TaskSchemaVersion, TaskID: lease.Delivery.Task.TaskID, Channel: target.Channel, BindingID: target.ChannelBindingID, ErrorCode: errorCode, TraceID: lease.Delivery.Task.TraceID, TraceParent: lease.Delivery.Task.TraceParent, DigestVersion: lease.Delivery.Task.DigestVersion}
	if target.Valid() {
		result.Target = target
	}
	rawResult, err := json.Marshal(result)
	if err != nil {
		return err
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	code, err := failCorruptPersistenceScript.Run(ctx, s.client, []string{lease.InboxKey, s.sessionLockKey(lease.SessionCoord), s.sessionStateKey(lease.SessionCoord), s.replyStream, s.taskStream, s.persistenceRetryKey},
		lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID, lease.LockToken, lease.SessionSeq, lease.Delivery.Task.PayloadDigest, lease.SessionCoord,
		string(rawResult), errorCode, int64(s.config.InboxRetention/time.Second), now.UnixMilli(), workerGroup).Int()
	if err != nil {
		return err
	}
	if code == -9 {
		return ErrKeyType
	}
	if code != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) recoverPersistence(ctx context.Context, delivery Delivery, consumer string, now time.Time) error {
	code, err := recoverPersistenceScript.Run(ctx, s.client, []string{s.inboxKey(delivery.InboxID), s.sessionLockKey(sessionCoord(delivery.Task)), s.taskStream},
		delivery.Task.TaskID, delivery.StreamID, delivery.Task.PayloadDigest, now.UnixMilli(), workerGroup, consumer).Int()
	if err != nil {
		return err
	}
	if code == -9 {
		return ErrKeyType
	}
	if code == -3 {
		return ErrSessionLockActive
	}
	if code == -4 {
		return persistence.ErrInvalidEnvelope
	}
	return nil
}

func decodePersistenceEnvelope(raw string) (persistence.Envelope, error) {
	var envelope persistence.Envelope
	if err := decodeStrictJSON(raw, &envelope); err != nil || envelope.Validate() != nil {
		return persistence.Envelope{}, persistence.ErrInvalidEnvelope
	}
	return envelope, nil
}
