-- Delete a cached value only when it still matches the bytes that failed to decode.
-- KEYS: value. ARGV: exact bytes returned by the failed read.
-- Returns 1 when deleted, 0 when the value was replaced or already removed.

if redis.call('GET', KEYS[1]) == ARGV[1] then
 return redis.call('DEL', KEYS[1])
end
return 0
