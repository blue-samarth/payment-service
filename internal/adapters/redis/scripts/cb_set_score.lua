local key   = KEYS[1]
local score = tonumber(ARGV[1])

local raw = redis.call('GET', key)
local cb  = raw and cjson.decode(raw) or {}

cb.last_known_score = score
cb.state = cb.state or 'CLOSED'
redis.call('SET', key, cjson.encode(cb))
return cb.state
