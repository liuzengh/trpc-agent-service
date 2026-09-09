local now = redis.call('TIME')
local seconds = tonumber(now[1])
local window = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local tenant_limit = tonumber(ARGV[3])
local binding_limit = tonumber(ARGV[4])
local chat_limit = tonumber(ARGV[5])
local limits = {tenant_limit, binding_limit, chat_limit}
local remaining = limits[1]
local retry_after = window - (seconds % window)

for i = 1, 3 do
  local current = tonumber(redis.call('GET', KEYS[i]) or '0')
  if current + cost > limits[i] then
    local available = limits[i] - current
    if available < remaining then
      remaining = available
    end
    return {0, retry_after, remaining, i}
  end
end

for i = 1, 3 do
  local current = redis.call('INCRBY', KEYS[i], cost)
  if current == cost then
    redis.call('EXPIRE', KEYS[i], window)
  end
  local available = limits[i] - current
  if available < remaining then
    remaining = available
  end
end

return {1, 0, remaining, 0}
