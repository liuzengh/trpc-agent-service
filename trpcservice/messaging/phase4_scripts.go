package messaging

import "github.com/redis/go-redis/v9"

// failWithSessionScript advances the Session cursor as part of a terminal
// failure. It is deliberately separate from the Phase 3 fail script.
var failWithSessionScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local state_type=redis.call('TYPE',KEYS[3]); local reply_type=redis.call('TYPE',KEYS[4]); local task_type=redis.call('TYPE',KEYS[5]); local wait_type=redis.call('TYPE',KEYS[6])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end; if type(reply_type)=='table' then reply_type=reply_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end; if type(wait_type)=='table' then wait_type=wait_type.ok end
if inbox_type~='hash' or (lock_type~='none' and lock_type~='hash') or state_type~='hash' or (reply_type~='none' and reply_type~='stream') or (task_type~='none' and task_type~='stream') or (wait_type~='none' and wait_type~='zset') then return -9 end
if redis.call('HGET',KEYS[1],'state')~='processing' or redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[3]) or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[4] then return 0 end
if tonumber(redis.call('HGET',KEYS[1],'session_seq') or '0')~=tonumber(ARGV[6]) or redis.call('HGET',KEYS[1],'digest')~=ARGV[12] or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')<=tonumber(ARGV[11]) then return 0 end
if redis.call('HGET',KEYS[2],'token')~=ARGV[5] or redis.call('HGET',KEYS[2],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[2],'lease_epoch') or '0')~=tonumber(ARGV[3]) or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')<=tonumber(ARGV[11]) then return -3 end
local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if tonumber(ARGV[6])~=last+1 then return -2 end
local result_ok,result=pcall(cjson.decode,ARGV[7]); if not result_ok or type(result)~='table' or result['task_id']~=ARGV[1] then return -4 end
redis.call('HSET',KEYS[1],'state','failed_terminal','result',ARGV[7],'error_code',ARGV[8])
redis.call('HDEL',KEYS[1],'payload','owner','lease_until','stream_id','next_attempt_at','wait_count','wait_reason')
redis.call('HSET',KEYS[3],'last_completed_seq',ARGV[6],'updated_at_ms',ARGV[11])
if wait_type=='zset' then redis.call('ZREM',KEYS[6],KEYS[1]..'|'..ARGV[4]) end
redis.call('EXPIRE',KEYS[1],ARGV[9]); redis.call('XADD',KEYS[4],'*','payload',ARGV[7]); redis.call('XDEL',KEYS[5],ARGV[4]); redis.call('XACK',KEYS[5],ARGV[10],ARGV[4]); redis.call('DEL',KEYS[2]); return 1
`)

// rejectWithSessionScript handles queued/session-stale deliveries without
// allowing a stale sequence to advance the cursor twice.
var rejectWithSessionScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local state_type=redis.call('TYPE',KEYS[2]); local reply_type=redis.call('TYPE',KEYS[3]); local task_type=redis.call('TYPE',KEYS[4]); local wait_type=redis.call('TYPE',KEYS[5]); local lock_type=redis.call('TYPE',KEYS[6])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end; if type(reply_type)=='table' then reply_type=reply_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end; if type(wait_type)=='table' then wait_type=wait_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end
if inbox_type~='hash' or state_type~='hash' or (reply_type~='none' and reply_type~='stream') or (task_type~='none' and task_type~='stream') or (wait_type~='none' and wait_type~='zset') or (lock_type~='none' and lock_type~='hash') then return -9 end
local inbox_state=redis.call('HGET',KEYS[1],'state'); if inbox_state~='queued' and inbox_state~='processing' then return 0 end
if redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[2] or redis.call('HGET',KEYS[1],'digest')~=ARGV[10] or redis.call('HGET',KEYS[1],'session_coord')~=ARGV[11] or tonumber(redis.call('HGET',KEYS[1],'session_seq') or '0')~=tonumber(ARGV[12]) then return 0 end
if inbox_state=='processing' then
  if redis.call('HGET',KEYS[1],'owner')~=ARGV[13] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[14]) or tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')>tonumber(ARGV[7]) then return -3 end
  if lock_type~='hash' or redis.call('HGET',KEYS[6],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[6],'lease_epoch') or '0')~=tonumber(ARGV[14]) or tonumber(redis.call('HGET',KEYS[6],'expires_at_ms') or '0')>tonumber(ARGV[7]) then return -3 end
end
local result_ok,result=pcall(cjson.decode,ARGV[4]); if not result_ok or type(result)~='table' or result['task_id']~=ARGV[1] then return -4 end
local seq=tonumber(redis.call('HGET',KEYS[1],'session_seq') or '0'); local last=tonumber(redis.call('HGET',KEYS[2],'last_completed_seq') or '0')
if seq<=last then redis.call('ZREM',KEYS[5],KEYS[1]..'|'..ARGV[2]); redis.call('XDEL',KEYS[4],ARGV[2]); redis.call('XACK',KEYS[4],ARGV[5],ARGV[2]); return 2 end
if seq>last+1 then
  local count=tonumber(redis.call('HGET',KEYS[1],'wait_count') or '0'); local delay=tonumber(ARGV[8]); local maxdelay=tonumber(ARGV[9]); local power=1
  for i=1,count do if delay>=maxdelay then delay=maxdelay; break end; power=power*2; if delay*power>=maxdelay then delay=maxdelay; break end end
  if delay<maxdelay then delay=delay*power end
  redis.call('HSET',KEYS[1],'wait_count',count+1,'wait_reason',ARGV[3]); redis.call('HDEL',KEYS[1],'stream_id'); redis.call('ZADD',KEYS[5],tonumber(ARGV[7])+delay,KEYS[1]..'|'..ARGV[2]); redis.call('XDEL',KEYS[4],ARGV[2]); redis.call('XACK',KEYS[4],ARGV[5],ARGV[2]); return 3
end
if inbox_state=='queued' and lock_type=='hash' then return -3 end
redis.call('HSET',KEYS[1],'state','failed_terminal','result',ARGV[4],'error_code',ARGV[3])
redis.call('HDEL',KEYS[1],'payload','stream_id','wait_count','wait_reason','owner','lease_until')
redis.call('HSET',KEYS[2],'last_completed_seq',seq,'updated_at_ms',ARGV[7]); redis.call('EXPIRE',KEYS[1],ARGV[6]); redis.call('XADD',KEYS[3],'*','payload',ARGV[4]); redis.call('XDEL',KEYS[4],ARGV[2]); redis.call('XACK',KEYS[4],ARGV[5],ARGV[2]); if inbox_state=='processing' then redis.call('DEL',KEYS[6]) end; return 1
`)

// releaseSessionLockScript is only used by cancellation/shutdown cleanup.
var releaseSessionLockScript = redis.NewScript(`
local t=redis.call('TYPE',KEYS[1]); if type(t)=='table' then t=t.ok end; if t=='none' then return 0 end; if t~='hash' then return -9 end
if redis.call('HGET',KEYS[1],'token')~=ARGV[1] or redis.call('HGET',KEYS[1],'task_id')~=ARGV[2] or tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0')~=tonumber(ARGV[3]) then return 0 end
redis.call('DEL',KEYS[1]); return 1
`)

// recoverWithSessionScript checks the Session lock before changing Pending
// ownership. A live lock returns before XCLAIM/XACK/XDEL, preserving the old
// consumer's Pending entry.
var recoverWithSessionScript = redis.NewScript(`
local inbox_type=redis.call('TYPE',KEYS[1]); local lock_type=redis.call('TYPE',KEYS[2]); local state_type=redis.call('TYPE',KEYS[3]); local reply_type=redis.call('TYPE',KEYS[4]); local task_type=redis.call('TYPE',KEYS[5]); local wait_type=redis.call('TYPE',KEYS[6])
if type(inbox_type)=='table' then inbox_type=inbox_type.ok end; if type(lock_type)=='table' then lock_type=lock_type.ok end; if type(state_type)=='table' then state_type=state_type.ok end; if type(reply_type)=='table' then reply_type=reply_type.ok end; if type(task_type)=='table' then task_type=task_type.ok end; if type(wait_type)=='table' then wait_type=wait_type.ok end
if inbox_type~='hash' or (lock_type~='none' and lock_type~='hash') or state_type~='hash' or (reply_type~='none' and reply_type~='stream') or (task_type~='none' and task_type~='stream') or (wait_type~='none' and wait_type~='zset') then return -9 end
local state=redis.call('HGET',KEYS[1],'state'); if state~='processing' and state~='queued' then return 0 end
if redis.call('HGET',KEYS[1],'task_id')~=ARGV[1] or redis.call('HGET',KEYS[1],'stream_id')~=ARGV[2] or redis.call('HGET',KEYS[1],'digest')~=ARGV[10] or tonumber(redis.call('HGET',KEYS[1],'attempt') or '0')~=tonumber(ARGV[11]) then return -1 end
if state=='processing' and tonumber(redis.call('HGET',KEYS[1],'lease_until') or '0')>tonumber(ARGV[3]) then return -2 end
if lock_type=='hash' and (redis.call('HGET',KEYS[2],'task_id')~=ARGV[1] or tonumber(redis.call('HGET',KEYS[2],'lease_epoch') or '0')~=tonumber(redis.call('HGET',KEYS[1],'lease_epoch') or '0') or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms') or '0')>tonumber(ARGV[3])) then return -3 end
local payload=redis.call('HGET',KEYS[1],'payload'); local inboxID=redis.call('HGET',KEYS[1],'inbox_id'); local coord=redis.call('HGET',KEYS[1],'session_coord'); local seq=redis.call('HGET',KEYS[1],'session_seq'); if not payload or not inboxID or not coord or not seq then return -1 end
local stored_ok,stored=pcall(cjson.decode,payload); if not stored_ok or type(stored)~='table' or stored['task_id']~=ARGV[1] or stored['payload_digest']~=ARGV[10] or tonumber(stored['attempt'] or '0')~=tonumber(ARGV[11]) then return -1 end
local failed_ok,failed=pcall(cjson.decode,ARGV[7]); if not failed_ok or type(failed)~='table' or failed['task_id']~=ARGV[1] then return -1 end
local nextAttempt=tonumber(ARGV[4]); if state=='processing' then nextAttempt=nextAttempt+1 end
local next_ok,next_payload=pcall(cjson.decode,ARGV[12]); if not next_ok or type(next_payload)~='table' or next_payload['task_id']~=ARGV[1] or next_payload['payload_digest']~=ARGV[10] or tonumber(next_payload['attempt'] or '0')~=nextAttempt then return -1 end
local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if state=='processing' and nextAttempt>tonumber(ARGV[5]) and tonumber(seq)>last+1 then return -4 end
-- All checks above happen before ownership changes.
local claimed=redis.call('XCLAIM',KEYS[5],ARGV[8],ARGV[9],0,ARGV[2],'JUSTID'); if not claimed then return 0 end
if #claimed~=1 then return 0 end
if state=='processing' and nextAttempt>tonumber(ARGV[5]) then
  redis.call('HSET',KEYS[1],'state','failed_terminal','result',ARGV[7],'error_code','worker_lost'); redis.call('HDEL',KEYS[1],'payload','owner','lease_until','stream_id','wait_count','wait_reason'); local seq=tonumber(redis.call('HGET',KEYS[1],'session_seq') or '0'); local last=tonumber(redis.call('HGET',KEYS[3],'last_completed_seq') or '0'); if seq==last+1 then redis.call('HSET',KEYS[3],'last_completed_seq',seq,'updated_at_ms',ARGV[3]) end; redis.call('EXPIRE',KEYS[1],ARGV[6]); redis.call('XADD',KEYS[4],'*','payload',ARGV[7]); redis.call('XDEL',KEYS[5],ARGV[2]); redis.call('XACK',KEYS[5],ARGV[8],ARGV[2]); if lock_type=='hash' then redis.call('DEL',KEYS[2]) end; return 2
end
local nextPayload=ARGV[12]; local newID=redis.call('XADD',KEYS[5],'*','payload',nextPayload,'inbox_id',inboxID,'session_coord',coord,'session_seq',seq)
redis.call('HSET',KEYS[1],'state','queued','attempt',nextAttempt,'payload',nextPayload,'stream_id',newID,'last_error','worker_lost'); redis.call('HDEL',KEYS[1],'owner','lease_until','next_attempt_at'); redis.call('XDEL',KEYS[5],ARGV[2]); redis.call('XACK',KEYS[5],ARGV[8],ARGV[2]); if lock_type=='hash' then redis.call('DEL',KEYS[2]) end; return 1
`)
