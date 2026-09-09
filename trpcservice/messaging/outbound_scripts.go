package messaging

import "github.com/redis/go-redis/v9"

var beginOutboundScript = redis.NewScript(`
local t=redis.call('TYPE',KEYS[1]); if type(t)=='table' then t=t.ok end
if t~='none' and t~='hash' then return {-9,0,0} end
if redis.call('HGET',KEYS[1],'terminal')=='1' then return {2,tonumber(redis.call('HGET',KEYS[1],'attempts') or '0'),0} end
local due=tonumber(redis.call('HGET',KEYS[1],'next_attempt_at') or '0')
if due>tonumber(ARGV[1]) then return {3,tonumber(redis.call('HGET',KEYS[1],'attempts') or '0'),due} end
local attempts=redis.call('HINCRBY',KEYS[1],'attempts',1)
redis.call('HSET',KEYS[1],'status','sending','updated_at',ARGV[1],'terminal','0','last_error','')
redis.call('HDEL',KEYS[1],'next_attempt_at')
redis.call('EXPIRE',KEYS[1],ARGV[2])
return {1,attempts,0}
`)

var retryOutboundScript = redis.NewScript(`
local t=redis.call('TYPE',KEYS[1]); if type(t)=='table' then t=t.ok end
if t~='hash' then return -9 end
if redis.call('HGET',KEYS[1],'terminal')=='1' then return 0 end
redis.call('HSET',KEYS[1],'status','retry_wait','last_error',ARGV[1],'next_attempt_at',ARGV[2],'updated_at',ARGV[3],'terminal','0')
redis.call('EXPIRE',KEYS[1],ARGV[4]); return 1
`)

var finishOutboundScript = redis.NewScript(`
local out_type=redis.call('TYPE',KEYS[1]); local stream_type=redis.call('TYPE',KEYS[2])
if type(out_type)=='table' then out_type=out_type.ok end; if type(stream_type)=='table' then stream_type=stream_type.ok end
if out_type~='hash' or (stream_type~='none' and stream_type~='stream') then return -9 end
if redis.call('HGET',KEYS[1],'terminal')=='1' then return 0 end
redis.call('HSET',KEYS[1],'status',ARGV[1],'last_error',ARGV[2],'terminal','1','updated_at',ARGV[3])
redis.call('HDEL',KEYS[1],'next_attempt_at')
if ARGV[1]=='succeeded' then redis.call('HSET',KEYS[1],'acked_at',ARGV[3]) end
redis.call('EXPIRE',KEYS[1],ARGV[4])
redis.call('XACK',KEYS[2],ARGV[5],ARGV[6]); redis.call('XDEL',KEYS[2],ARGV[6]); return 1
`)
