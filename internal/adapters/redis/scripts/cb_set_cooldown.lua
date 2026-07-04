local key            = KEYS[1]
local cooldown_until = ARGV[1]

local raw = redis.call('GET', key)
if not raw then return 0 end

local cb = cjson.decode(raw)
if cb.state ~= 'OPEN' then return 0 end

cb.cooldown_until = cooldown_until
redis.call('SET', key, cjson.encode(cb))
return 1
