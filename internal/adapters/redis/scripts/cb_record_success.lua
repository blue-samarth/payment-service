local key       = KEYS[1]
local probe     = KEYS[2]
local now_epoch = tonumber(ARGV[1])

local raw = redis.call('GET', key)
if not raw then
  return 'CLOSED'
end

local cb       = cjson.decode(raw)
local state    = cb.state or 'CLOSED'
local cooldown = tonumber(cb.cooldown_until) or 0

if state == 'OPEN' and (cooldown == 0 or now_epoch < cooldown) then
  return state
end

if state ~= 'CLOSED' then
  redis.call('DEL', probe)
end

cb.state = 'CLOSED'
cb.consecutive_failures = 0
cb.cooldown_until = 0
cb.trips = 0
redis.call('SET', key, cjson.encode(cb))
return 'CLOSED'
