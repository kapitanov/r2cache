-- Renew only a still-valid owner and permit; never resurrect expired ownership.
-- KEYS: lock, permits. ARGV: token, lease ms, concurrency limit. Returns 1 on renewal, 0 on loss.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if tonumber(ARGV[3]) > 0 then
 local score = redis.call('ZSCORE', KEYS[2], ARGV[1])
 if not score or tonumber(score) <= now then return 0 end
 redis.call('ZADD', KEYS[2], now+tonumber(ARGV[2]), ARGV[1])
 redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2])*2)
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
