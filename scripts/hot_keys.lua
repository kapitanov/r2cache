-- List unexpired hot keys in oldest-access-first order without touching access times.
-- KEYS: hot index.
-- ARGV: optional maximum number of keys; when present the reply is bounded.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

if ARGV[1] then
    return redis.call('ZRANGEBYSCORE', KEYS[1], '('..now, '+inf', 'LIMIT', 0, ARGV[1])
end

return redis.call('ZRANGEBYSCORE', KEYS[1], '('..now, '+inf')
