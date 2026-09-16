-- List unexpired hot keys in oldest-access-first order without touching access times.
-- KEYS: hot index.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

return redis.call('ZRANGEBYSCORE', KEYS[1], '('..now, '+inf')
