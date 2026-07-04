local key       = KEYS[1]
local threshold = tonumber(ARGV[1])
local now_epoch = tonumber(ARGV[2])

local raw   = redis.call('GET', key)
local cb    = raw and cjson.decode(raw) or {}
local state = cb.state or 'CLOSED'
local fails = (cb.consecutive_failures or 0) + 1
local opened = 0

if state == 'HALF_OPEN' or (state == 'CLOSED' and fails >= threshold) then
  state = 'OPEN'
  local secs = math.floor(60 * (2 ^ (fails - 1)))
  if secs > 240 then secs = 240 end
  cb.cooldown_until = now_epoch + secs
  opened = 1
end

cb.state = state
cb.consecutive_failures = fails
redis.call('SET', key, cjson.encode(cb))
return {opened, fails}
