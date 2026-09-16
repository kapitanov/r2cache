-- Count currently eligible hot keys without mutating the shared index.
-- KEYS: hot index.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

return redis.call('ZCOUNT', KEYS[1], '('..now, '+inf')
