local key       = KEYS[1]
local probe     = KEYS[2]
local now_epoch = tonumber(ARGV[1])
local ttl_ms    = tonumber(ARGV[2])

local raw = redis.call('GET', key)
if not raw then
  return 1
end

local cb       = cjson.decode(raw)
local state    = cb.state or 'CLOSED'
local cooldown = tonumber(cb.cooldown_until) or 0

if state == 'CLOSED' then
  return 1
end

local probing = state == 'HALF_OPEN'
             or (state == 'OPEN' and cooldown > 0 and now_epoch >= cooldown)
if not probing then
  return 0
end

if redis.call('SET', probe, '1', 'NX', 'PX', ttl_ms) then
  return 1
end
return 0
