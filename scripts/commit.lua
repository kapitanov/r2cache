-- Store a result only while its owner and permit remain valid, then wake waiters.
-- Idempotent for retries: when ownership is gone but the stored value already
-- equals the encoded payload, an earlier execution committed and the response
-- was lost, so report success without side effects. A different payload with
-- lost ownership returns 0 and must not overwrite the stored value.
-- KEYS: value, lock, permits, due index, events channel, hot index.
-- ARGV: token, encoded value, TTL ms, concurrency limit, user key, refresh interval ms, hot lifetime ms.
-- Returns 1 on commit or on replay of an already stored payload, 0 on lost ownership.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

if redis.call('GET', KEYS[2]) ~= ARGV[1] then
 if redis.call('GET', KEYS[1]) == ARGV[2] then return 1 end
 return 0
end
if tonumber(ARGV[4]) > 0 then
 local score = redis.call('ZSCORE', KEYS[3], ARGV[1])
 if not score or tonumber(score) <= now then return 0 end
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
local interval = tonumber(ARGV[6])
if interval > 0 then
 -- Only a key that is hot at commit time is eligible for background refresh.
 -- A cold commit, such as the fetch after a one-off miss, must not grow the due
 -- index: the later read that makes the key hot schedules its first refresh.
 local hot = redis.call('ZSCORE', KEYS[6], ARGV[5])
 if hot and tonumber(hot) > now then
  redis.call('ZADD', KEYS[4], now+interval, ARGV[5])
  redis.call('PEXPIRE', KEYS[4], tonumber(ARGV[7]) + interval)
 end
end
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('PUBLISH', KEYS[5], ARGV[5])
return 1
