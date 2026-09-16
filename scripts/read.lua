-- Record an external access, schedule a new hot key, and return its cached value.
-- KEYS: value, hot index, due index. ARGV: user key, hot lifetime ms, refresh interval ms.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

if tonumber(ARGV[2]) > 0 then
 redis.call('ZADD', KEYS[2], now+tonumber(ARGV[2]), ARGV[1])
 redis.call('ZADD', KEYS[3], 'NX', now+tonumber(ARGV[3]), ARGV[1])
 redis.call('PEXPIRE', KEYS[2], ARGV[2])
 redis.call('PEXPIRE', KEYS[3], ARGV[2])
end
return redis.call('GET', KEYS[1])
