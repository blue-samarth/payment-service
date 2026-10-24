local key       = KEYS[1]
local probe     = KEYS[2]
local threshold = tonumber(ARGV[1])
local now_epoch = tonumber(ARGV[2])
local base_secs = tonumber(ARGV[3])
local max_secs  = tonumber(ARGV[4])

local raw      = redis.call('GET', key)
local cb       = raw and cjson.decode(raw) or {}
local state    = cb.state or 'CLOSED'
local cooldown = tonumber(cb.cooldown_until) or 0
local trips    = tonumber(cb.trips) or 0
local fails    = (tonumber(cb.consecutive_failures) or 0) + 1
local opened   = 0

local probing = state == 'HALF_OPEN'
             or (state == 'OPEN' and cooldown > 0 and now_epoch >= cooldown)

if probing or (state == 'CLOSED' and fails >= threshold) then
  trips = trips + 1
  local secs = base_secs * (2 ^ (trips - 1))
  if secs > max_secs then secs = max_secs end
  state = 'OPEN'
  cooldown = now_epoch + math.floor(secs)
  opened = 1
  redis.call('DEL', probe)
end

cb.state = state
cb.cooldown_until = cooldown
cb.trips = trips
cb.consecutive_failures = fails
redis.call('SET', key, cjson.encode(cb))
return {opened, fails}
