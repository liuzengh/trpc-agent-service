package messaging

import "github.com/redis/go-redis/v9"

var submitWithSessionScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
local seq_type = redis.call('TYPE', KEYS[3])
local state_type = redis.call('TYPE', KEYS[4])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if type(seq_type) == 'table' then seq_type = seq_type.ok end
if type(state_type) == 'table' then state_type = state_type.ok end
if (inbox_type ~= 'none' and inbox_type ~= 'hash') or
   (stream_type ~= 'none' and stream_type ~= 'stream') or
   (seq_type ~= 'none' and seq_type ~= 'string') or
   (state_type ~= 'none' and state_type ~= 'hash') then return {-9} end
local existing = redis.call('HGET', KEYS[1], 'task_id')
if existing then
  if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return {-1} end
  return {0, tonumber(redis.call('HGET', KEYS[1], 'session_seq') or '0'), redis.call('HGET', KEYS[1], 'stream_id') or ''}
end
local payload_ok, payload = pcall(cjson.decode, ARGV[3])
if not payload_ok or type(payload) ~= 'table' or payload['task_id'] ~= ARGV[1] or payload['payload_digest'] ~= ARGV[2] or tonumber(payload['attempt'] or '0') ~= tonumber(ARGV[4]) then return {-2} end
local seq = redis.call('INCR', KEYS[3])
redis.call('HSETNX', KEYS[4], 'last_completed_seq', 0)
redis.call('HSET', KEYS[4], 'updated_at_ms', ARGV[8])
redis.call('HSET', KEYS[1],
  'task_id', ARGV[1], 'digest', ARGV[2], 'payload', ARGV[3],
  'state', 'queued', 'attempt', ARGV[4], 'trace_id', ARGV[5], 'inbox_id', ARGV[6],
  'session_coord', ARGV[7], 'session_seq', seq, 'received_at_ms', ARGV[8], 'request_id', ARGV[9],
  'tenant_id', ARGV[10], 'agent_app_id', ARGV[11], 'channel', ARGV[12], 'binding_id', ARGV[13], 'platform_message_id', ARGV[14])
local stream_id = redis.call('XADD', KEYS[2], '*', 'payload', ARGV[3], 'inbox_id', ARGV[6],
  'session_coord', ARGV[7], 'session_seq', seq)
redis.call('HSET', KEYS[1], 'stream_id', stream_id)
if ARGV[15] and ARGV[15] ~= '' then
  redis.call('HSET', KEYS[1], 'node_id', ARGV[15], 'assignment_revision', ARGV[16], 'assignment_mode', ARGV[17], 'assignment_state', ARGV[18], 'assignment_payload_digest', ARGV[19])
end
return {1, seq, stream_id}
`)

// legacySubmitScript preserves the Phase 3 key and Stream contract. Strong
// Session ordering keys are never created while fencing is disabled.
var legacySubmitScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if (inbox_type ~= 'none' and inbox_type ~= 'hash') or
   (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
local existing = redis.call('HGET', KEYS[1], 'task_id')
if existing then
  if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return -1 end
  return 0
end
local payload_ok, payload = pcall(cjson.decode, ARGV[3])
if not payload_ok or type(payload) ~= 'table' or payload['task_id'] ~= ARGV[1] or payload['payload_digest'] ~= ARGV[2] or tonumber(payload['attempt'] or '0') ~= tonumber(ARGV[4]) then return -2 end
redis.call('HSET', KEYS[1],
  'task_id', ARGV[1], 'digest', ARGV[2], 'payload', ARGV[3],
  'state', 'queued', 'attempt', ARGV[4], 'trace_id', ARGV[5], 'inbox_id', ARGV[6], 'request_id', ARGV[7], 'received_at_ms', ARGV[8],
  'tenant_id', ARGV[9], 'agent_app_id', ARGV[10], 'channel', ARGV[11], 'binding_id', ARGV[12], 'platform_message_id', ARGV[13])
if ARGV[14] and ARGV[14] ~= '' then
  redis.call('HSET', KEYS[1], 'node_id', ARGV[14], 'assignment_revision', ARGV[15], 'assignment_mode', ARGV[16], 'assignment_state', ARGV[17], 'assignment_payload_digest', ARGV[18])
end
local stream_id = redis.call('XADD', KEYS[2], '*', 'payload', ARGV[3], 'inbox_id', ARGV[6])
redis.call('HSET', KEYS[1], 'stream_id', stream_id)
return 1
`)

var beginScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local lock_type = redis.call('TYPE', KEYS[2])
local state_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(lock_type) == 'table' then lock_type = lock_type.ok end
if type(state_type) == 'table' then state_type = state_type.ok end
if inbox_type ~= 'hash' or (lock_type ~= 'none' and lock_type ~= 'hash') or state_type ~= 'hash' then return {-9, 0} end
if redis.call('HGET', KEYS[1], 'state') ~= 'queued' then return {-1, 0} end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return {-2, 0} end
if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return {-2, 0} end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return {-2, 0} end
if tonumber(redis.call('HGET', KEYS[1], 'attempt')) ~= tonumber(ARGV[5]) then return {-2, 0} end
if redis.call('HGET', KEYS[1], 'session_coord') ~= ARGV[9] then return {-2, 0} end
if tonumber(redis.call('HGET', KEYS[1], 'session_seq') or '0') ~= tonumber(ARGV[10]) then return {-2, 0} end
if redis.call('EXISTS', KEYS[2]) == 1 then return {-3, 0} end
local last = tonumber(redis.call('HGET', KEYS[3], 'last_completed_seq') or '0')
local seq = tonumber(redis.call('HGET', KEYS[1], 'session_seq') or '0')
if seq > last + 1 then return {-4, 0} end
if seq <= last then return {-5, 0} end
local epoch = redis.call('HINCRBY', KEYS[1], 'lease_epoch', 1)
redis.call('HSET', KEYS[1], 'state', 'processing', 'owner', ARGV[3],
  'lease_until', ARGV[6], 'last_error', '')
redis.call('HDEL', KEYS[1], 'wait_count', 'wait_reason')
redis.call('HSET', KEYS[2], 'token', ARGV[7], 'task_id', ARGV[1], 'owner', ARGV[3], 'lease_epoch', epoch, 'expires_at_ms', ARGV[11])
redis.call('PEXPIRE', KEYS[2], ARGV[8])
return {1, epoch}
`)

var legacyBeginScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if inbox_type ~= 'hash' then return {-9, 0} end
if redis.call('HGET', KEYS[1], 'state') ~= 'queued' then return {-1, 0} end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return {-2, 0} end
if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return {-2, 0} end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return {-2, 0} end
if tonumber(redis.call('HGET', KEYS[1], 'attempt')) ~= tonumber(ARGV[5]) then return {-2, 0} end
local epoch = redis.call('HINCRBY', KEYS[1], 'lease_epoch', 1)
redis.call('HSET', KEYS[1], 'state', 'processing', 'owner', ARGV[3], 'lease_until', ARGV[6], 'last_error', '')
return {1, epoch}
`)

var beginFailureScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if inbox_type ~= 'hash' or (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'queued' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return 0 end
local current_stream = redis.call('HGET', KEYS[1], 'stream_id')
if current_stream == ARGV[3] then
  local payload = redis.call('HGET', KEYS[1], 'payload')
  local inbox_id = redis.call('HGET', KEYS[1], 'inbox_id')
  if not payload or not inbox_id then return 0 end
  local new_stream = redis.call('XADD', KEYS[2], '*', 'payload', payload, 'inbox_id', inbox_id)
  redis.call('HSET', KEYS[1], 'stream_id', new_stream)
end
redis.call('XDEL', KEYS[2], ARGV[3])
redis.call('XACK', KEYS[2], ARGV[4], ARGV[3])
return 1
`)

// taskHeartbeatScript renews the task lease and the Redis Stream Pending idle
// time. It deliberately does not touch the Session lock.
var taskHeartbeatScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if inbox_type ~= 'hash' or (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch')) ~= tonumber(ARGV[3]) then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
redis.call('HSET', KEYS[1], 'lease_until', ARGV[5])
redis.call('XCLAIM', KEYS[2], ARGV[6], ARGV[2], 0, ARGV[4], 'JUSTID')
return 1
`)

// strongTaskHeartbeatScript additionally prevents an already-expired lease
// from being resurrected by a delayed heartbeat.
var strongTaskHeartbeatScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if inbox_type ~= 'hash' or (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
local state=redis.call('HGET', KEYS[1], 'state')
if (state ~= 'processing' and state ~= 'persisting') or
   redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] or
   tonumber(redis.call('HGET', KEYS[1], 'lease_epoch') or '0') ~= tonumber(ARGV[3]) or
   redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] or
   redis.call('HGET', KEYS[1], 'digest') ~= ARGV[7] or
   tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') <= tonumber(ARGV[8]) then return 0 end
local claimed = redis.call('XCLAIM', KEYS[2], ARGV[6], ARGV[2], 0, ARGV[4], 'JUSTID')
if #claimed ~= 1 then return 0 end
redis.call('HSET', KEYS[1], 'lease_until', ARGV[5])
return 1
`)

// sessionHeartbeatScript renews only the Session lock. In particular it must
// not call XCLAIM: doing so would keep task Pending entries alive and prevent
// XAUTOCLAIM from recovering a task whose task heartbeat has stopped.
var sessionHeartbeatScript = redis.NewScript(`
local lock_type = redis.call('TYPE', KEYS[1])
local inbox_type = redis.call('TYPE', KEYS[2])
if type(lock_type) == 'table' then lock_type = lock_type.ok end
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if (lock_type ~= 'none' and lock_type ~= 'hash') or inbox_type ~= 'hash' then return -9 end
if lock_type == 'none' then return 0 end
if redis.call('HGET', KEYS[1], 'token') ~= ARGV[4] then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch') or '0') ~= tonumber(ARGV[3]) then return 0 end
local state=redis.call('HGET', KEYS[2], 'state')
if state ~= 'processing' and state ~= 'persisting' then return 0 end
if redis.call('HGET', KEYS[2], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[2], 'owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[2], 'lease_epoch') or '0') ~= tonumber(ARGV[3]) then return 0 end
if redis.call('HGET', KEYS[2], 'digest') ~= ARGV[7] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0') <= tonumber(ARGV[8]) then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[5])
redis.call('HSET', KEYS[1], 'expires_at_ms', ARGV[6])
return 1
`)

var deferSessionScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local wait_type = redis.call('TYPE', KEYS[2])
local stream_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(wait_type) == 'table' then wait_type = wait_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if inbox_type ~= 'hash' or (wait_type ~= 'none' and wait_type ~= 'zset') or (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'queued' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] or redis.call('HGET', KEYS[1], 'digest') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[3] then return 0 end
local payload_ok,payload=pcall(cjson.decode,redis.call('HGET',KEYS[1],'payload') or ''); if not payload_ok or type(payload)~='table' or payload['task_id']~=ARGV[1] or payload['payload_digest']~=ARGV[2] then return 0 end
local count=tonumber(redis.call('HGET', KEYS[1], 'wait_count') or '0'); local delay=tonumber(ARGV[7]); local maxdelay=tonumber(ARGV[8]); local power=1
for i=1,count do if delay>=maxdelay then delay=maxdelay; break end; power=power*2; if delay*power>=maxdelay then delay=maxdelay; break end end
if delay<maxdelay then delay=delay*power end
local due=tonumber(ARGV[9])+delay
redis.call('ZADD', KEYS[2], due, KEYS[1] .. '|' .. ARGV[3])
redis.call('HSET', KEYS[1], 'wait_count', count+1, 'wait_reason', ARGV[6])
redis.call('HDEL', KEYS[1], 'stream_id')
redis.call('XDEL', KEYS[3], ARGV[3])
redis.call('XACK', KEYS[3], ARGV[5], ARGV[3])
return 1
`)

var promoteSessionScript = redis.NewScript(`
local wait_type = redis.call('TYPE', KEYS[1])
local inbox_type = redis.call('TYPE', KEYS[2])
local stream_type = redis.call('TYPE', KEYS[3])
local state_type = redis.call('TYPE', KEYS[4])
local lock_type = redis.call('TYPE', KEYS[5])
if type(wait_type) == 'table' then wait_type = wait_type.ok end
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if type(state_type) == 'table' then state_type = state_type.ok end
if type(lock_type) == 'table' then lock_type = lock_type.ok end
if wait_type ~= 'zset' or inbox_type ~= 'hash' or (stream_type ~= 'none' and stream_type ~= 'stream') or state_type ~= 'hash' or (lock_type ~= 'none' and lock_type ~= 'hash') then return -9 end
if redis.call('ZSCORE', KEYS[1], ARGV[1]) == false then return 0 end
if redis.call('HGET', KEYS[2], 'state') ~= 'queued' then redis.call('ZREM', KEYS[1], ARGV[1]); return 0 end
if redis.call('HGET', KEYS[2], 'stream_id') then redis.call('ZREM', KEYS[1], ARGV[1]); return 0 end
local last = tonumber(redis.call('HGET', KEYS[4], 'last_completed_seq') or '0')
local seq = tonumber(redis.call('HGET', KEYS[2], 'session_seq') or '0')
if redis.call('EXISTS', KEYS[5]) == 1 or seq > last + 1 then
  local count=tonumber(redis.call('HGET', KEYS[2], 'wait_count') or '0'); local delay=tonumber(ARGV[3]); local maxdelay=tonumber(ARGV[4]); local power=1
  for i=1,count do if delay>=maxdelay then delay=maxdelay; break end; power=power*2; if delay*power>=maxdelay then delay=maxdelay; break end end
  if delay<maxdelay then delay=delay*power end
  redis.call('HSET', KEYS[2], 'wait_count', count+1)
  redis.call('ZADD', KEYS[1], tonumber(ARGV[2])+delay, ARGV[1])
  return 2
end
local payload = redis.call('HGET', KEYS[2], 'payload')
local inbox_id = redis.call('HGET', KEYS[2], 'inbox_id')
local coord = redis.call('HGET', KEYS[2], 'session_coord')
if not payload or not inbox_id or not coord then redis.call('ZREM', KEYS[1], ARGV[1]); return 0 end
local payload_ok,payload_value=pcall(cjson.decode,payload); if not payload_ok or type(payload_value)~='table' then return -1 end
local stream_id = redis.call('XADD', KEYS[3], '*', 'payload', payload, 'inbox_id', inbox_id, 'session_coord', coord, 'session_seq', seq)
redis.call('HSET', KEYS[2], 'stream_id', stream_id)
redis.call('HDEL', KEYS[2], 'wait_reason')
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`)

var retryScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local lock_type = redis.call('TYPE', KEYS[2])
local retry_type = redis.call('TYPE', KEYS[3])
local stream_type = redis.call('TYPE', KEYS[4])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(lock_type) == 'table' then lock_type = lock_type.ok end
if type(retry_type) == 'table' then retry_type = retry_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if inbox_type ~= 'hash' or
   (lock_type ~= 'none' and lock_type ~= 'hash') or
   (retry_type ~= 'none' and retry_type ~= 'zset') or
   (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch')) ~= tonumber(ARGV[3]) then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[5] then return 0 end
if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[11] or tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') <= tonumber(ARGV[12]) then return 0 end
if redis.call('HGET', KEYS[2], 'token') ~= ARGV[4] then return 0 end
if redis.call('HGET', KEYS[2], 'task_id') ~= ARGV[1] then return 0 end
if tonumber(redis.call('HGET', KEYS[2], 'lease_epoch') or '0') ~= tonumber(ARGV[3]) then return 0 end
if tonumber(redis.call('HGET', KEYS[2], 'expires_at_ms') or '0') <= tonumber(ARGV[12]) then return 0 end
local payload_ok, payload = pcall(cjson.decode, ARGV[7])
if not payload_ok or type(payload) ~= 'table' or payload['task_id'] ~= ARGV[1] or payload['payload_digest'] ~= ARGV[11] or tonumber(payload['attempt'] or '0') ~= tonumber(ARGV[6]) then return 0 end
redis.call('HSET', KEYS[1], 'state', 'retry_wait', 'attempt', ARGV[6],
	'payload', ARGV[7], 'next_attempt_at', ARGV[8], 'last_error', ARGV[9])
redis.call('HDEL', KEYS[1], 'owner', 'lease_until', 'stream_id')
redis.call('HDEL', KEYS[1], 'wait_count', 'wait_reason')
redis.call('ZADD', KEYS[3], ARGV[8], KEYS[1])
redis.call('XDEL', KEYS[4], ARGV[5])
redis.call('XACK', KEYS[4], ARGV[10], ARGV[5])
redis.call('DEL', KEYS[2])
return 1
`)

var legacyRetryScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1]); local retry_type = redis.call('TYPE', KEYS[2]); local stream_type = redis.call('TYPE', KEYS[3])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(retry_type)=='table' then retry_type=retry_type.ok end; if type(stream_type)=='table' then stream_type=stream_type.ok end
if inbox_type~='hash' or (retry_type~='none' and retry_type~='zset') or (stream_type~='none' and stream_type~='stream') then return -9 end
if redis.call('HGET',KEYS[1],'state')~='processing' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch'))~=tonumber(ARGV[3]) or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] then return 0 end
redis.call('HSET',KEYS[1],'state','retry_wait','attempt',ARGV[5],'payload',ARGV[6],'next_attempt_at',ARGV[7],'last_error',ARGV[8]); redis.call('HDEL',KEYS[1],'owner','lease_until','stream_id'); redis.call('ZADD',KEYS[2],ARGV[7],KEYS[1]); redis.call('XDEL',KEYS[3],ARGV[4]); redis.call('XACK',KEYS[3],ARGV[9],ARGV[4]); return 1
`)

var promoteScript = redis.NewScript(`
local retry_type = redis.call('TYPE', KEYS[1])
local stream_type = redis.call('TYPE', KEYS[2])
if type(retry_type) == 'table' then retry_type = retry_type.ok end
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if (retry_type ~= 'none' and retry_type ~= 'zset') or
   (stream_type ~= 'none' and stream_type ~= 'stream') then return -9 end
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, inbox in ipairs(due) do
  local inbox_type = redis.call('TYPE', inbox)
  if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
  if inbox_type ~= 'hash' then return -9 end
end
local promoted = 0
for _, inbox in ipairs(due) do
  local state = redis.call('HGET', inbox, 'state')
  local next_at = tonumber(redis.call('HGET', inbox, 'next_attempt_at') or '0')
  local payload = redis.call('HGET', inbox, 'payload')
  local inbox_id = redis.call('HGET', inbox, 'inbox_id')
  if state == 'retry_wait' and next_at <= tonumber(ARGV[1]) and payload and inbox_id then
    local stream_id = redis.call('XADD', KEYS[2], '*', 'payload', payload, 'inbox_id', inbox_id)
    redis.call('HSET', inbox, 'state', 'queued', 'stream_id', stream_id)
    redis.call('HDEL', inbox, 'next_attempt_at')
    promoted = promoted + 1
  end
  redis.call('ZREM', KEYS[1], inbox)
end
return promoted
`)

var completeScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local reply_type = redis.call('TYPE', KEYS[2])
local task_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(reply_type) == 'table' then reply_type = reply_type.ok end
if type(task_type) == 'table' then task_type = task_type.ok end
if inbox_type ~= 'hash' or
   (reply_type ~= 'none' and reply_type ~= 'stream') or
   (task_type ~= 'none' and task_type ~= 'stream') then return -9 end
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return 0 end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch')) ~= tonumber(ARGV[3]) then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
redis.call('HSET', KEYS[1], 'state', 'succeeded', 'result', ARGV[5], 'error_code', '')
redis.call('HDEL', KEYS[1], 'payload', 'owner', 'lease_until', 'next_attempt_at')
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[5])
redis.call('XDEL', KEYS[3], ARGV[4])
redis.call('XACK', KEYS[3], ARGV[7], ARGV[4])
return 1
`)

var failScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local reply_type = redis.call('TYPE', KEYS[2])
local task_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(reply_type) == 'table' then reply_type = reply_type.ok end
if type(task_type) == 'table' then task_type = task_type.ok end
if inbox_type ~= 'hash' or
   (reply_type ~= 'none' and reply_type ~= 'stream') or
   (task_type ~= 'none' and task_type ~= 'stream') then return -9 end
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'processing' then
  if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
  if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
  if tonumber(redis.call('HGET', KEYS[1], 'lease_epoch')) ~= tonumber(ARGV[3]) then return 0 end
  if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
elseif state == 'queued' then
  if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return 0 end
  if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[4] then return 0 end
else
  return 0
end
redis.call('HSET', KEYS[1], 'state', 'failed_terminal', 'result', ARGV[5], 'error_code', ARGV[6])
redis.call('HDEL', KEYS[1], 'payload', 'owner', 'lease_until', 'next_attempt_at')
redis.call('EXPIRE', KEYS[1], ARGV[7])
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[5])
redis.call('XDEL', KEYS[3], ARGV[4])
redis.call('XACK', KEYS[3], ARGV[8], ARGV[4])
return 1
`)

var rejectScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local reply_type = redis.call('TYPE', KEYS[2])
local task_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(reply_type) == 'table' then reply_type = reply_type.ok end
if type(task_type) == 'table' then task_type = task_type.ok end
if inbox_type ~= 'hash' or
   (reply_type ~= 'none' and reply_type ~= 'stream') or
   (task_type ~= 'none' and task_type ~= 'stream') then return -9 end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'processing' and state ~= 'queued' then return 0 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'state', 'failed_terminal', 'result', ARGV[2], 'error_code', ARGV[3])
redis.call('HDEL', KEYS[1], 'payload', 'owner', 'lease_until', 'next_attempt_at')
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[2])
redis.call('XDEL', KEYS[3], ARGV[1])
redis.call('XACK', KEYS[3], ARGV[5], ARGV[1])
return 1
`)

var recoverScript = redis.NewScript(`
local inbox_type = redis.call('TYPE', KEYS[1])
local reply_type = redis.call('TYPE', KEYS[2])
local task_type = redis.call('TYPE', KEYS[3])
if type(inbox_type) == 'table' then inbox_type = inbox_type.ok end
if type(reply_type) == 'table' then reply_type = reply_type.ok end
if type(task_type) == 'table' then task_type = task_type.ok end
if inbox_type ~= 'hash' or
   (reply_type ~= 'none' and reply_type ~= 'stream') or
   (task_type ~= 'none' and task_type ~= 'stream') then return -9 end
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'processing' and state ~= 'queued' then
  redis.call('XDEL', KEYS[3], ARGV[2])
  redis.call('XACK', KEYS[3], ARGV[8], ARGV[2])
  return 0
end
if redis.call('HGET', KEYS[1], 'task_id') ~= ARGV[1] then return -1 end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[2] then return -1 end
if redis.call('HGET', KEYS[1], 'digest') ~= ARGV[10] then return -1 end
if tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0') ~= tonumber(ARGV[11]) then return -1 end
if state == 'processing' and tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') > tonumber(ARGV[3]) then return -2 end
if tonumber(ARGV[4]) > tonumber(ARGV[5]) then
  redis.call('HSET', KEYS[1], 'state', 'failed_terminal', 'result', ARGV[7], 'error_code', 'worker_lost')
  redis.call('HDEL', KEYS[1], 'payload', 'owner', 'lease_until', 'next_attempt_at')
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  redis.call('XADD', KEYS[2], '*', 'payload', ARGV[7])
else
  local new_id = redis.call('XADD', KEYS[3], '*', 'payload', ARGV[9], 'inbox_id', ARGV[12])
  redis.call('HSET', KEYS[1], 'state', 'queued', 'attempt', ARGV[4], 'payload', ARGV[9], 'stream_id', new_id, 'last_error', 'worker_lost')
  redis.call('HDEL', KEYS[1], 'owner', 'lease_until', 'next_attempt_at')
end
redis.call('XDEL', KEYS[3], ARGV[2])
redis.call('XACK', KEYS[3], ARGV[8], ARGV[2])
return 1
`)

var ackReplyScript = redis.NewScript(`
local stream_type = redis.call('TYPE', KEYS[1])
if type(stream_type) == 'table' then stream_type = stream_type.ok end
if stream_type ~= 'none' and stream_type ~= 'stream' then return -9 end
redis.call('XDEL', KEYS[1], ARGV[2])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
`)
