local now_ms    = tonumber(ARGV[1])
local requested = tonumber(ARGV[2])
local need      = math.max(requested, 1)

local n = #KEYS
local refilled = {}
local ttls = {}
local allowed = 1
local max_wait = 0

for i = 1, n do
  local capacity = tonumber(ARGV[1 + 2 * i])
  local rate     = tonumber(ARGV[2 + 2 * i])

  local ttl
  if rate <= 0 then
    ttl = 3600
  else
    ttl = math.ceil(capacity / rate) + 10
  end
  ttls[i] = ttl

  local data    = redis.call('HMGET', KEYS[i], 'tokens', 'last_ms')
  local tokens  = tonumber(data[1])
  local last_ms = tonumber(data[2])

  if tokens == nil then tokens = capacity end
  if last_ms == nil then last_ms = now_ms end

  local elapsed = math.max(0, now_ms - last_ms) / 1000
  tokens = math.min(capacity, tokens + elapsed * rate)

  refilled[i] = tokens

  if tokens < need then
    allowed = 0
    local wait
    if rate <= 0 then
      wait = ttl * 1000
    else
      wait = math.ceil((need - tokens) / rate * 1000)
    end
    if wait > max_wait then max_wait = wait end
  end
end

if allowed == 1 and requested > 0 then
  for i = 1, n do
    redis.call('HMSET', KEYS[i], 'tokens', refilled[i] - requested, 'last_ms', now_ms)
    redis.call('EXPIRE', KEYS[i], ttls[i])
  end
end

return { allowed, max_wait }
