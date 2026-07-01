local key      = KEYS[1]
local to       = ARGV[1]
local until_ts = ARGV[2]
local fails    = tonumber(ARGV[3])

local raw  = redis.call('GET', key)
local cb   = raw and cjson.decode(raw) or {}
local from = cb.state or 'CLOSED'

local ok = (from == 'CLOSED'    and to == 'OPEN')
        or (from == 'OPEN'      and to == 'HALF_OPEN')
        or (from == 'HALF_OPEN' and (to == 'CLOSED' or to == 'OPEN'))

if not ok then return 0 end

cb.state = to
cb.cooldown_until = until_ts
cb.consecutive_failures = fails
redis.call('SET', key, cjson.encode(cb))
return 1
