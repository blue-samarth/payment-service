local key            = KEYS[1]
local threshold      = tonumber(ARGV[1])

local raw   = redis.call('GET', key)
local cb    = raw and cjson.decode(raw) or {}
local state = cb.state or 'CLOSED'
local fails = (cb.consecutive_failures or 0) + 1
local opened = 0

if state == 'HALF_OPEN' or (state == 'CLOSED' and fails >= threshold) then
  state = 'OPEN'
  cb.cooldown_until = ''
  opened = 1
end

cb.state = state
cb.consecutive_failures = fails
redis.call('SET', key, cjson.encode(cb))
return {opened, fails}
