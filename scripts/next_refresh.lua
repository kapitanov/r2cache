-- Select at most one due key per global pacing interval, without extending hot time.
-- Prune in bounded batches so a burst of cold keys cannot monopolize Redis.
-- KEYS: hot index, due index, pacing timestamp. ARGV: refresh interval ms.
-- Returns {delay ms}, or {delay ms, user key} when a key was scheduled.

local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 128)
for _,k in ipairs(expired) do
 redis.call('ZREM', KEYS[1], k)
 redis.call('ZREM', KEYS[2], k)
end
local count = redis.call('ZCOUNT', KEYS[1], '('..now, '+inf')
if count == 0 then return {0} end
local next = tonumber(redis.call('GET', KEYS[3]) or '0')
if next > now then return {next-now} end
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 1)
if #due == 0 then return {0} end
local k = due[1]
local hot = redis.call('ZSCORE', KEYS[1], k)
if not hot or tonumber(hot) <= now then
 redis.call('ZREM', KEYS[2], k)
 return {0}
end
local pace = math.max(1, math.floor(tonumber(ARGV[1])/count))
redis.call('SET', KEYS[3], now+pace, 'PX', pace)
redis.call('ZADD', KEYS[2], now+tonumber(ARGV[1]), k)
return {pace,k}
