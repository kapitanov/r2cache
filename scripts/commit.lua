-- Store a result only while its owner and permit remain valid, then wake waiters.
-- KEYS: value, lock, permits, hot index, due index, events channel.
-- ARGV: token, encoded value, TTL ms, concurrency limit, user key, refresh interval ms.
-- Returns 1 on commit, 0 on lost ownership.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
if tonumber(ARGV[4]) > 0 then
 local score = redis.call('ZSCORE', KEYS[3], ARGV[1])
 if not score or tonumber(score) <= now then return 0 end
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
if tonumber(ARGV[6]) > 0 then
 local hot = redis.call('ZSCORE', KEYS[4], ARGV[5])
 if hot and tonumber(hot) > now then
  redis.call('ZADD', KEYS[5], now+tonumber(ARGV[6]), ARGV[5])
 end
end
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('PUBLISH', KEYS[6], ARGV[5])
return 1
