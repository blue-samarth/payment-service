local key = KEYS[1]

local raw   = redis.call('GET', key)
local cb    = raw and cjson.decode(raw) or {}
local state = cb.state or 'CLOSED'

if state == 'HALF_OPEN' then
  state = 'CLOSED'
end

cb.state = state
cb.consecutive_failures = 0
cb.cooldown_until = 0
redis.call('SET', key, cjson.encode(cb))
return cb.state
