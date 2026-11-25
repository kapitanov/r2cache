-- Defer a hot key after a failed refresh, measured from the failure rather than
-- the attempt start, so a slow failed fetch cannot retry almost immediately.
-- KEYS: hot index, due index. ARGV: user key, refresh interval ms.
-- Returns 1 when the due time moved, 0 when the key is no longer eligible.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

local hot = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not hot or tonumber(hot) <= now then return 0 end
-- The due index expires with the hot index; do not create it without a TTL.
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
redis.call('ZADD', KEYS[2], now+tonumber(ARGV[2]), ARGV[1])
return 1
