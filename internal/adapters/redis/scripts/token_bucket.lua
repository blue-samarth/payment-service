local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local now_ms    = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local ttl
if rate <= 0 then
  ttl = 3600
else
  ttl = math.ceil(capacity / rate) + 10
end

local n = #KEYS
local refilled = {}
local allowed = 1
local max_wait = 0

for i = 1, n do
  local data    = redis.call('HMGET', KEYS[i], 'tokens', 'last_ms')
  local tokens  = tonumber(data[1])
  local last_ms = tonumber(data[2])

  if tokens == nil then tokens = capacity end
  if last_ms == nil then last_ms = now_ms end

  local elapsed = math.max(0, now_ms - last_ms) / 1000
  tokens = math.min(capacity, tokens + elapsed * rate)

  refilled[i] = tokens

  if tokens < requested then
    allowed = 0
    local wait
    if rate <= 0 then
      wait = ttl * 1000
    else
      wait = math.ceil((requested - tokens) / rate * 1000)
    end
    if wait > max_wait then max_wait = wait end
  end
end

for i = 1, n do
  local tokens = refilled[i]
  if allowed == 1 then
    tokens = tokens - requested
  end
  redis.call('HMSET', KEYS[i], 'tokens', tokens, 'last_ms', now_ms)
  redis.call('EXPIRE', KEYS[i], ttl)
end

return { allowed, max_wait }
