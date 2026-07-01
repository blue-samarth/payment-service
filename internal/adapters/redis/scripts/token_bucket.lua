local key        = KEYS[1]
local legacy_key = KEYS[2]
local capacity   = tonumber(ARGV[1])
local rate       = tonumber(ARGV[2])
local now_ms     = tonumber(ARGV[3])
local requested  = tonumber(ARGV[4])

local data    = redis.call('HMGET', key, 'tokens', 'last_ms')
local tokens  = tonumber(data[1])
local last_ms = tonumber(data[2])
local used_legacy = 0

if tokens == nil and legacy_key and legacy_key ~= '' then
  local legacy = redis.call('HMGET', legacy_key, 'tokens', 'last_ms')
  tokens  = tonumber(legacy[1])
  last_ms = tonumber(legacy[2])
  if tokens ~= nil then
    used_legacy = 1
  end
end

if tokens == nil then tokens = capacity end
if last_ms == nil then last_ms = now_ms end

local elapsed = math.max(0, now_ms - last_ms) / 1000
tokens = math.min(capacity, tokens + elapsed * rate)

local ttl = math.ceil(capacity / rate) + 10

if tokens < requested then
  local wait_sec = (requested - tokens) / rate
  redis.call('HMSET', key, 'tokens', tokens, 'last_ms', now_ms)
  redis.call('EXPIRE', key, ttl)
  if used_legacy == 1 then redis.call('DEL', legacy_key) end
  return {0, math.ceil(wait_sec * 1000)}
end

tokens = tokens - requested
redis.call('HMSET', key, 'tokens', tokens, 'last_ms', now_ms)
redis.call('EXPIRE', key, ttl)
if used_legacy == 1 then redis.call('DEL', legacy_key) end
return {1, 0}
