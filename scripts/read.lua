-- Return a cached value and, only when one exists, record the external access.
-- KEYS: value, hot index, due index. ARGV: user key, hot lifetime ms, refresh interval ms.
-- A miss leaves hot and due untouched so a failed fetch cannot be refresh-eligible.

local value = redis.call('GET', KEYS[1])
if not value then return false end

if tonumber(ARGV[2]) > 0 then
 local t = redis.call('TIME')
 local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
 redis.call('ZADD', KEYS[2], now+tonumber(ARGV[2]), ARGV[1])
 redis.call('ZADD', KEYS[3], 'NX', now+tonumber(ARGV[3]), ARGV[1])
 redis.call('PEXPIRE', KEYS[2], ARGV[2])
 redis.call('PEXPIRE', KEYS[3], ARGV[2])
end
return value
