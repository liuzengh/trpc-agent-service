package workqueue

import "github.com/redis/go-redis/v9"

// A platform stream belongs to exactly one consumer group. Check on every
// destructive operation as well as startup; never delete another group's work.
const checkGroup = `
local groups=redis.call('XINFO','GROUPS',KEYS[1])
if #groups~=1 then return redis.error_reply('QUEUE_GROUP_CONFLICT') end
local group=nil
local last='0-0'
for i=1,#groups[1],2 do
 if groups[1][i]=='name' then group=groups[1][i+1] end
 if groups[1][i]=='last-delivered-id' then last=groups[1][i+1] end
end
if group~=ARGV[1] then return redis.error_reply('QUEUE_GROUP_CONFLICT') end
`

var initializeStream = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==0 then
 redis.call('XGROUP','CREATE',KEYS[1],ARGV[1],'0','MKSTREAM')
 return 1
end
local groups=redis.call('XINFO','GROUPS',KEYS[1])
if #groups==0 then
 redis.call('XGROUP','CREATE',KEYS[1],ARGV[1],'0')
 return 1
end
` + checkGroup + `return 1`)

// Prune only a confirmed prefix. A low pending ID deliberately keeps newer
// history until that task finishes. Unread and pending records are never cut.
var publishTask = redis.NewScript(checkGroup + `
local pending=redis.call('XPENDING',KEYS[1],ARGV[1],'-','+',1)
local keep=last
if #pending>0 then keep=pending[1][1] end
if keep~='0-0' then redis.call('XTRIM',KEYS[1],'MINID',keep) end
if redis.call('XLEN',KEYS[1])>=tonumber(ARGV[2]) then return 0 end
redis.call('XADD',KEYS[1],'*','task',ARGV[3])
return 1
`)

const checkOwner = `
local pending=redis.call('XPENDING',KEYS[1],ARGV[1],ARGV[3],ARGV[3],1)
if #pending==0 or pending[1][2]~=ARGV[2] then return 0 end
`

var renewDelivery = redis.NewScript(checkGroup + checkOwner + `
local claimed=redis.call('XCLAIM',KEYS[1],ARGV[1],ARGV[2],0,ARGV[3],'JUSTID')
return #claimed
`)

var acknowledgeDelivery = redis.NewScript(checkGroup + checkOwner + `
redis.call('XACK',KEYS[1],ARGV[1],ARGV[3])
redis.call('XDEL',KEYS[1],ARGV[3])
return 1
`)

// Publish replacement before acknowledging the old task in the same script.
// The replacement uses the old record's capacity slot. OOM on XADD leaves the
// original pending record intact, rather than losing it between two commands.
var retryDelivery = redis.NewScript(checkGroup + checkOwner + `
redis.call('XADD',KEYS[1],'*','task',ARGV[4])
redis.call('XACK',KEYS[1],ARGV[1],ARGV[3])
redis.call('XDEL',KEYS[1],ARGV[3])
return 1
`)
