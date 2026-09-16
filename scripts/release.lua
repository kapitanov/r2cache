-- Release only this token; an old owner must not remove a replacement lock.
-- KEYS: lock, permits, events channel. ARGV: token, user key.

if redis.call('GET', KEYS[1]) == ARGV[1] then
 redis.call('DEL', KEYS[1])
 redis.call('PUBLISH', KEYS[3], ARGV[2])
end
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
