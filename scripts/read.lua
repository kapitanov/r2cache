-- Return a cached value and, when refresh is enabled, sample an external access.
-- KEYS: value, hot index. ARGV: user key, hot lifetime ms, refresh interval ms.
-- A miss leaves the hot index untouched; only reads that find a value make a key hot.
-- Hotness is recorded at most once per sample window and is therefore best-effort.

local value = redis.call('GET', KEYS[1])
if not value then return false end

local hot_ttl = tonumber(ARGV[2])
if hot_ttl > 0 then
 local t = redis.call('TIME')
 local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
 -- At most one hotness write per key per window. The window stays below the
 -- hot lifetime, so continuously read keys never leave the hot set.
 local sample = math.min(tonumber(ARGV[3]), math.floor(hot_ttl/2))
 if sample < 1 then sample = 1 end
 if sample >= hot_ttl then sample = 0 end
 -- Scores hold the last sampled access plus the hot lifetime. The 1ms lifetime
 -- cannot contain a shorter window, so it records every access.
 local score = redis.call('ZSCORE', KEYS[2], ARGV[1])
 if not score or tonumber(score) - hot_ttl <= now - sample then
  redis.call('ZADD', KEYS[2], now + hot_ttl, ARGV[1])
  redis.call('PEXPIRE', KEYS[2], ARGV[2])
 end
end
return value
