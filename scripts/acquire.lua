-- Acquire per-key ownership and, when limited, a global permit atomically.
-- Idempotent for retries: if the lock already holds this token, an earlier
-- execution acquired it, so renew the lease (and permit) and report acquired
-- instead of busy. A lock held by another token is still busy.
-- KEYS: value, lock, permits, hot index. ARGV: token, lease ms, limit, refresh flag, user key.
-- Returns: {0} busy, {1, value} cached, {2} acquired, {3} no longer hot.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

if redis.call('GET', KEYS[2]) == ARGV[1] then
 redis.call('PEXPIRE', KEYS[2], ARGV[2])
 if tonumber(ARGV[3]) > 0 then
  redis.call('ZADD', KEYS[3], now+tonumber(ARGV[2]), ARGV[1])
  redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[2])*2)
 end
 return {2}
end

if ARGV[4] == '0' then
 local v = redis.call('GET', KEYS[1])
 if v then return {1,v} end
else
 local hot = redis.call('ZSCORE', KEYS[4], ARGV[5])
 if not hot or tonumber(hot) <= now then return {3} end
end
if redis.call('EXISTS', KEYS[2]) == 1 then return {0} end
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
if tonumber(ARGV[3]) > 0 and redis.call('ZCARD', KEYS[3]) >= tonumber(ARGV[3]) then return {0} end
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[2])
if tonumber(ARGV[3]) > 0 then
 redis.call('ZADD', KEYS[3], now+tonumber(ARGV[2]), ARGV[1])
 redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[2])*2)
end
return {2}
