package r2cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	require.Eventually(t, fn, timeout, 10*time.Millisecond)
}
func testCache(t *testing.T, builder *Builder[string, int]) *Cache[string, int] {
	t.Helper()
	c, err := builder.Build()
	require.NoError(t, err)
	t.Cleanup(c.Shutdown)
	return c
}
func mustGet(t *testing.T, c *Cache[string, int], key string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	value, err := c.GetOrFetch(ctx, key)
	require.NoError(t, err)
	return value
}

// wakeWindow bounds how long a notification-triggered attempt may take. It must
// stay below the 100ms polling interval, otherwise an attempt could be explained
// by the backstop timer instead of the notification.
const wakeWindow = 80 * time.Millisecond

// acquireGate is a go-redis Hook that counts acquire-script invocations per user
// key and can hold them before they reach Redis. Holding attempts keeps waiters
// registered while other keys are notified, so the test observes wakeups rather
// than the polling backstop.
type acquireGate struct {
	mu      sync.Mutex
	labels  map[string]string
	gates   map[string]chan struct{}
	waiting map[string]int
	counts  map[string]int
}

func newAcquireGate() *acquireGate {
	return &acquireGate{
		labels:  make(map[string]string),
		gates:   make(map[string]chan struct{}),
		waiting: make(map[string]int),
		counts:  make(map[string]int),
	}
}

func (g *acquireGate) register(lockKey, label string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.labels[lockKey] = label
}

func (g *acquireGate) hold(label string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gates[label] == nil {
		g.gates[label] = make(chan struct{})
	}
}

func (g *acquireGate) release(label string) {
	g.mu.Lock()
	gate := g.gates[label]
	g.gates[label] = nil
	g.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

func (g *acquireGate) attempts(label string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.counts[label]
}

func (g *acquireGate) blocked(label string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waiting[label]
}

func (g *acquireGate) DialHook(next redis.DialHook) redis.DialHook { return next }

func (g *acquireGate) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if len(args) < 5 || !strings.EqualFold(fmt.Sprint(args[0]), "evalsha") || fmt.Sprint(args[1]) != acquireScript.Hash() {
			return next(ctx, cmd)
		}

		g.mu.Lock()
		label, known := g.labels[fmt.Sprint(args[4])]
		if !known {
			g.mu.Unlock()
			return next(ctx, cmd)
		}
		gate := g.gates[label]
		if gate == nil {
			g.counts[label]++
			g.mu.Unlock()
			return next(ctx, cmd)
		}
		g.waiting[label]++
		g.mu.Unlock()

		select {
		case <-gate:
		case <-ctx.Done():
			g.mu.Lock()
			g.waiting[label]--
			g.mu.Unlock()
			return ctx.Err()
		}

		g.mu.Lock()
		g.waiting[label]--
		g.counts[label]++
		g.mu.Unlock()

		return next(ctx, cmd)
	}
}

func (g *acquireGate) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func testCacheWithGate(t *testing.T, options *redis.Options, prefix string) (*Cache[string, int], *acquireGate) {
	t.Helper()
	gate := newAcquireGate()
	client := redis.NewClient(options)
	client.AddHook(gate)
	t.Cleanup(func() { _ = client.Close() })
	c, err := NewBuilder[string, int]().
		UseRedisClient(client).
		KeyPrefix(prefix).
		Fetch(func(context.Context, string) (int, error) { return 1, nil }).
		Build()
	require.NoError(t, err)
	t.Cleanup(c.Shutdown)
	return c, gate
}

func waitForSubscribers(t *testing.T, admin *redis.Client, channel string) {
	t.Helper()
	eventually(t, 2*time.Second, func() bool {
		counts, err := admin.PubSubNumSub(context.Background(), channel).Result()
		return err == nil && counts[channel] >= 1
	})
}

func waiterCount(c *Cache[string, int]) int {
	c.waitMu.Lock()
	defer c.waitMu.Unlock()
	return len(c.waiters)
}

// commandCalls returns the cumulative execution count of one Redis command,
// including calls made from Lua scripts. INFO resets nothing, so callers
// compare snapshots taken around the operation under test.
func commandCalls(t *testing.T, client *redis.Client, command string) int64 {
	t.Helper()
	info, err := client.Do(context.Background(), "INFO", "commandstats").Text()
	require.NoError(t, err)
	for _, line := range strings.Split(info, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "cmdstat_"+command+":")
		if !ok {
			continue
		}
		for _, field := range strings.Split(rest, ",") {
			if calls, ok := strings.CutPrefix(field, "calls="); ok {
				n, err := strconv.ParseInt(calls, 10, 64)
				require.NoError(t, err)
				return n
			}
		}
	}
	return 0
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration tests require Docker")
	}
	ctx := context.Background()
	image := os.Getenv("R2CACHE_TEST_IMAGE")
	if image == "" {
		image = "redis:7.4.2-alpine"
	}
	container, err := tcredis.Run(ctx, image)
	require.NoError(t, err, "start Redis container (Docker must be running)")
	testcontainers.CleanupContainer(t, container)
	url, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	options, err := redis.ParseURL(url)
	require.NoError(t, err)
	admin := redis.NewClient(options)
	t.Cleanup(func() { _ = admin.Close() })

	t.Run("builder snapshots and independent clients", func(t *testing.T) {
		policy := AutoRefreshPolicy{HotKeyLifetime: time.Minute, UpdateInterval: 30 * time.Second}
		builder := NewBuilder[string, int]().RedisURL(url).
			KeyPrefix(t.Name() + ":first").TTL(time.Minute).
			Fetch(func(context.Context, string) (int, error) { return 1, nil }).EnableAutoRefresh(policy)
		first := testCache(t, builder)
		policy.HotKeyLifetime = 0
		builder.KeyPrefix(t.Name() + ":second").TTL(time.Hour).DisableAutoRefresh().
			Fetch(func(context.Context, string) (int, error) { return 2, nil })
		second := testCache(t, builder)
		require.NotSame(t, first.client, second.client)
		require.True(t, first.owned)
		require.True(t, second.owned)
		require.Equal(t, 1, mustGet(t, first, "product"))
		require.Equal(t, 2, mustGet(t, second, "product"))
		// The fetch above was a miss; a miss is not hot, so read again to hit.
		require.Equal(t, 1, mustGet(t, first, "product"))
		require.Equal(t, time.Minute, first.cfg.refresh.HotKeyLifetime)
		require.ElementsMatch(t, []string{"product"}, first.HotKeys(ctx))
		require.Empty(t, second.HotKeys(ctx))
		firstTTL, err := admin.PTTL(ctx, first.keys("product")[0]).Result()
		require.NoError(t, err)
		secondTTL, err := admin.PTTL(ctx, second.keys("product")[0]).Result()
		require.NoError(t, err)
		require.Positive(t, firstTTL)
		require.LessOrEqual(t, firstTTL, time.Minute)
		require.Greater(t, secondTTL, 59*time.Minute)
		first.Shutdown()
		require.Equal(t, 2, mustGet(t, second, "another-product"))
	})

	t.Run("literal key prefixes", func(t *testing.T) {
		for _, tt := range []struct {
			name, prefix, valueKey, hotKey string
		}{
			{"empty", "", "value:bXkta2V5", "hot"},
			{"plain", "my-cache", "my-cache:value:bXkta2V5", "my-cache:hot"},
			{"trailing colon", "my-cache:", "my-cache::value:bXkta2V5", "my-cache::hot"},
			{"verbatim", " My 缓存:{products} ", " My 缓存:{products} :value:bXkta2V5", " My 缓存:{products} :hot"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				cache := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(tt.prefix).Fetch(func(context.Context, string) (int, error) {
					return 42, nil
				}).EnableAutoRefresh(AutoRefreshPolicy{
					HotKeyLifetime: time.Minute,
					UpdateInterval: 30 * time.Second,
				}))
				require.Equal(t, 42, mustGet(t, cache, "my-key"))
				// The fetch above was a miss; a hit is what registers the key as hot.
				require.Equal(t, 42, mustGet(t, cache, "my-key"))
				// Inspect actual Redis keys, independent of the cache's key builder.
				stored, err := admin.Get(ctx, tt.valueKey).Result()
				require.NoError(t, err)
				require.Equal(t, "42", stored)
				_, err = admin.ZScore(ctx, tt.hotKey, "my-key").Result()
				require.NoError(t, err)
			})
		}
	})

	t.Run("read TTL and namespace isolation", func(t *testing.T) {
		var calls atomic.Int64
		fetch := func(context.Context, string) (int, error) { return int(calls.Add(1)), nil }
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch).TTL(120*time.Millisecond))
		first, second := mustGet(t, c, "{key}:a\x00"), mustGet(t, c, "{key}:a\x00")
		require.Equal(t, 1, first)
		require.Equal(t, 1, second, "cache hit changed value")
		eventually(t, time.Second, func() bool { return admin.Exists(ctx, c.keys("{key}:a\x00")[0]).Val() == 0 })
		require.Equal(t, 2, mustGet(t, c, "{key}:a\x00"), "TTL did not refetch")
		other := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()+"-other").Fetch(fetch))
		require.Equal(t, 3, mustGet(t, other, "{key}:a\x00"), "namespace collision")
		require.Empty(t, c.HotKeys(ctx), "refresh disabled but hot keys present")
	})

	t.Run("no-refresh reads are write-free", func(t *testing.T) {
		var calls atomic.Int64
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).
			Fetch(func(context.Context, string) (int, error) { return int(calls.Add(1)), nil }))
		require.Equal(t, 1, mustGet(t, c, "key"), "initial miss fetch")

		before := map[string]int64{}
		for _, command := range []string{"get", "time", "zadd", "zrem", "pexpire", "expire"} {
			before[command] = commandCalls(t, admin, command)
		}
		require.Equal(t, 1, mustGet(t, c, "key"), "cache hit")
		for _, command := range []string{"time", "zadd", "zrem", "pexpire", "expire"} {
			require.Equal(t, before[command], commandCalls(t, admin, command), "read hit issued %s", command)
		}
		require.Equal(t, before["get"]+1, commandCalls(t, admin, "get"), "read hit did not look up the value")

		// A read miss must also leave the hot and due indexes untouched. Call the
		// script directly so acquire, fetch, and commit writes do not obscure it.
		missing := c.keys("missing")
		beforeMiss := commandCalls(t, admin, "time")
		for _, command := range []string{"zadd", "zrem", "pexpire", "expire"} {
			before[command] = commandCalls(t, admin, command)
		}
		_, err := readScript.Run(ctx, admin, []string{missing[0], missing[3]}, "missing", 0, 0).Text()
		require.ErrorIs(t, err, redis.Nil)
		require.Equal(t, beforeMiss, commandCalls(t, admin, "time"), "read miss issued TIME")
		for _, command := range []string{"zadd", "zrem", "pexpire", "expire"} {
			require.Equal(t, before[command], commandCalls(t, admin, command), "read miss issued %s", command)
		}
		require.Zero(t, admin.Exists(ctx, missing[3], missing[4]).Val(), "read miss wrote an index")
	})

	t.Run("refresh reads sample hotness", func(t *testing.T) {
		var calls atomic.Int64
		// Keep UpdateInterval below both HotKeyLifetime and TTL for issue #5.
		policy := AutoRefreshPolicy{HotKeyLifetime: 10 * time.Second, UpdateInterval: 2 * time.Second}
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).TTL(time.Minute).
			Fetch(func(context.Context, string) (int, error) { calls.Add(1); return 1, nil }).
			EnableAutoRefresh(policy))
		keys := c.keys("key")

		require.Equal(t, 1, mustGet(t, c, "key"), "initial miss fetch")
		dueScore, err := admin.ZScore(ctx, keys[4], "key").Result()
		require.NoError(t, err, "commit did not schedule the next refresh")
		require.Equal(t, int64(1), admin.ZCard(ctx, keys[4]).Val())
		require.Zero(t, admin.Exists(ctx, keys[3]).Val(), "miss recorded hotness")

		require.Equal(t, 1, mustGet(t, c, "key"), "first hit")
		hotBefore, err := admin.ZScore(ctx, keys[3], "key").Result()
		require.NoError(t, err, "hit did not record hotness")
		dueBefore, err := admin.ZScore(ctx, keys[4], "key").Result()
		require.NoError(t, err)
		require.InDelta(t, dueScore, dueBefore, 0, "hit changed the due schedule")

		require.Equal(t, 1, mustGet(t, c, "key"), "second immediate hit")
		hotAfter, err := admin.ZScore(ctx, keys[3], "key").Result()
		require.NoError(t, err)
		require.InDelta(t, hotBefore, hotAfter, 0, "hot score written inside the sample window")
		dueAfter, err := admin.ZScore(ctx, keys[4], "key").Result()
		require.NoError(t, err)
		require.InDelta(t, dueScore, dueAfter, 0, "read wrote the due index")
		require.Equal(t, int64(1), admin.ZCard(ctx, keys[4]).Val(), "read added a due member")

		// After the sample window, the next hit records access again.
		sample := min(policy.UpdateInterval, policy.HotKeyLifetime/2)
		lastAccess := hotBefore - float64(policy.HotKeyLifetime.Milliseconds())
		eventually(t, sample+time.Second, func() bool {
			now, err := admin.Time(ctx).Result()
			return err == nil && float64(now.UnixMilli()) >= lastAccess+float64(sample.Milliseconds())
		})
		// The due entry from the first commit fires during the window. A
		// background refresh replaces the value but must not touch the hot score.
		eventually(t, 2*time.Second, func() bool { return calls.Load() >= 2 })
		indexed, err := admin.ZScore(ctx, keys[3], "key").Result()
		require.NoError(t, err)
		require.InDelta(t, hotBefore, indexed, 0, "background refresh wrote the hot score")
		require.Equal(t, 1, mustGet(t, c, "key"), "hit after the sample window")
		hotLater, err := admin.ZScore(ctx, keys[3], "key").Result()
		require.NoError(t, err)
		require.Greater(t, hotLater, hotBefore, "hit after the sample window did not record access")
	})

	t.Run("continuous reads stay hot while sampling", func(t *testing.T) {
		var calls atomic.Int64
		policy := AutoRefreshPolicy{HotKeyLifetime: 600 * time.Millisecond, UpdateInterval: 200 * time.Millisecond}
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).TTL(2*time.Second).
			Fetch(func(context.Context, string) (int, error) { return int(calls.Add(1)), nil }).
			EnableAutoRefresh(policy))
		require.Equal(t, 1, mustGet(t, c, "continuous"), "initial miss fetch")
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.After(2 * policy.HotKeyLifetime)
	loop:
		for {
			select {
			case <-deadline:
				break loop
			case <-ticker.C:
				op, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := c.GetOrFetch(op, "continuous")
				cancel()
				require.NoError(t, err)
				require.Equal(t, []string{"continuous"}, c.HotKeys(ctx), "continuously read key fell out of the hot set")
			}
		}
		eventually(t, 2*time.Second, func() bool { return calls.Load() >= 2 })
	})

	t.Run("cross instance singleflight and renewal", func(t *testing.T) {
		var calls atomic.Int64
		fetch := func(ctx context.Context, _ string) (int, error) {
			calls.Add(1)
			select {
			case <-time.After(900 * time.Millisecond):
				return 42, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		caches := make([]*Cache[string, int], 3)
		for i := range caches {
			caches[i] = testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch).LeaseDuration(300*time.Millisecond).LimitConcurrentFetches(2))
		}
		start := make(chan struct{})
		type fetchResult struct {
			value int
			err   error
		}
		results := make(chan fetchResult, 24)
		var wg sync.WaitGroup
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				op, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				value, err := caches[i%3].GetOrFetch(op, "same")
				results <- fetchResult{value: value, err: err}
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		for result := range results {
			require.NoError(t, result.err)
			require.Equal(t, 42, result.value)
		}
		require.Equal(t, int64(1), calls.Load(), "backend fetch count")
	})

	t.Run("global concurrency", func(t *testing.T) {
		var active, peak atomic.Int64
		fetch := func(ctx context.Context, _ string) (int, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			select {
			case <-time.After(80 * time.Millisecond):
				return 1, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		a := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch).LimitConcurrentFetches(2))
		b := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch).LimitConcurrentFetches(2))
		errs := make(chan error, 16)
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c := a
				if i%2 == 1 {
					c = b
				}
				op, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				_, err := c.GetOrFetch(op, fmt.Sprint(i))
				errs <- err
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		require.Equal(t, int64(2), peak.Load(), "peak backend concurrency")
	})

	t.Run("errors and cancellation release ownership", func(t *testing.T) {
		backendErr := errors.New("backend failed")
		var calls atomic.Int64
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(ctx context.Context, key string) (int, error) {
			if calls.Add(1) == 1 {
				return 0, backendErr
			}
			return 9, nil
		}).LimitConcurrentFetches(1))
		_, err := c.GetOrFetch(ctx, "key")
		require.ErrorIs(t, err, backendErr)
		require.Equal(t, 9, mustGet(t, c, "key"))
		require.Equal(t, int64(2), calls.Load(), "error was cached or lease retained")
		entered := make(chan struct{})
		owner := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()+"-cancel").Fetch(func(ctx context.Context, _ string) (int, error) { close(entered); <-ctx.Done(); return 0, ctx.Err() }).LimitConcurrentFetches(1))
		op, cancel := context.WithCancel(ctx)
		result := make(chan error, 1)
		go func() { _, e := owner.GetOrFetch(op, "key"); result <- e }()
		<-entered
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		follower := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()+"-cancel").Fetch(func(context.Context, string) (int, error) { return 7, nil }).LimitConcurrentFetches(1))
		require.Equal(t, 7, mustGet(t, follower, "key"), "failed recovery")
	})

	t.Run("acquire transport error releases lock and permit", func(t *testing.T) {
		primeScripts(t, admin)
		client := acquireTestClient(options)
		t.Cleanup(func() { _ = client.Close() })
		hook := &acquireReplyHook{}
		hook.drops.Store(1)
		client.AddHook(hook)

		var calls atomic.Int64
		c := testCache(t, NewBuilder[string, int]().UseRedisClient(client).KeyPrefix(t.Name()).
			LimitConcurrentFetches(1).
			Fetch(func(context.Context, string) (int, error) { calls.Add(1); return 1, nil }))
		keys := c.keys("key")

		_, err := c.GetOrFetch(ctx, "key")
		require.ErrorIs(t, err, io.EOF, "injected acquire reply error lost")
		require.Zero(t, calls.Load(), "fetched without a usable acquire reply")
		assertAcquireCleaned(t, admin, keys, hook)
	})

	t.Run("acquire deadline releases lock and permit", func(t *testing.T) {
		primeScripts(t, admin)
		client := acquireTestClient(options)
		t.Cleanup(func() { _ = client.Close() })
		hook := &acquireReplyHook{deadline: true}
		hook.drops.Store(1)
		client.AddHook(hook)

		var calls atomic.Int64
		c := testCache(t, NewBuilder[string, int]().UseRedisClient(client).KeyPrefix(t.Name()).
			LimitConcurrentFetches(1).
			Fetch(func(context.Context, string) (int, error) { calls.Add(1); return 1, nil }))
		keys := c.keys("key")

		op, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, err := c.GetOrFetch(op, "key")
		require.ErrorIs(t, err, context.DeadlineExceeded, "caller deadline not observed")
		require.Zero(t, calls.Load(), "fetched without a usable acquire reply")
		assertAcquireCleaned(t, admin, keys, hook)
	})

	t.Run("acquire recovery after dropped reply", func(t *testing.T) {
		primeScripts(t, admin)
		client := acquireTestClient(options)
		t.Cleanup(func() { _ = client.Close() })
		hook := &acquireReplyHook{}
		hook.drops.Store(1)
		client.AddHook(hook)

		var calls atomic.Int64
		c := testCache(t, NewBuilder[string, int]().UseRedisClient(client).KeyPrefix(t.Name()).
			LimitConcurrentFetches(1).
			Fetch(func(context.Context, string) (int, error) { calls.Add(1); return 42, nil }))
		keys := c.keys("key")

		_, err := c.GetOrFetch(ctx, "key")
		require.ErrorIs(t, err, io.EOF, "injected acquire reply error lost")
		require.Equal(t, 42, mustGet(t, c, "key"), "retry after acquire error")
		require.Equal(t, int64(1), calls.Load(), "cleanup disturbed the retry")
		require.Zero(t, admin.Exists(ctx, keys[1]).Val(), "lock left after successful fetch")
		require.Zero(t, admin.ZCard(ctx, keys[2]).Val(), "permit left after successful fetch")
	})

	t.Run("waiter cancellation preserves owner", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int64
		fetch := func(ctx context.Context, _ string) (int, error) {
			calls.Add(1)
			close(entered)
			select {
			case <-release:
				return 8, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		a := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch))
		b := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch))
		done := make(chan error, 1)
		go func() { _, e := a.GetOrFetch(ctx, "key"); done <- e }()
		<-entered
		op, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		_, err := b.GetOrFetch(op, "key")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		close(release)
		require.NoError(t, <-done)
		require.Equal(t, 8, mustGet(t, b, "key"))
		require.Equal(t, int64(1), calls.Load(), "waiter disturbed owner")
	})

	// A notification must wake only the waiters registered for its key. Both
	// waiters share one cache and one notifications worker, so the waiter that is
	// expected to stay asleep shares the goroutine that consumes the A event.
	t.Run("notification wakes only waiters for its key", func(t *testing.T) {
		c, gate := testCacheWithGate(t, options, t.Name())
		waitForSubscribers(t, admin, c.base+"events")
		gate.register(c.keys("A")[1], "A")
		gate.register(c.keys("B")[1], "B")
		for _, key := range []string{"A", "B"} {
			require.NoError(t, admin.Set(ctx, c.keys(key)[1], "owner", time.Minute).Err())
		}

		gate.hold("A")
		gate.hold("B")
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		t.Cleanup(cancel)
		done := make(chan error, 2)
		for _, key := range []string{"A", "B"} {
			go func() {
				_, err := c.GetOrFetch(waitCtx, key)
				done <- err
			}()
		}
		eventually(t, time.Second, func() bool { return gate.blocked("A") == 1 && gate.blocked("B") == 1 })
		require.Zero(t, gate.attempts("A"))
		require.Zero(t, gate.attempts("B"))

		require.NoError(t, admin.Publish(ctx, c.base+"events", "A").Err())
		gate.release("A")
		eventually(t, wakeWindow, func() bool { return gate.attempts("A") >= 2 })

		gate.release("B")
		eventually(t, time.Second, func() bool { return gate.attempts("B") >= 1 })
		require.Never(t, func() bool { return gate.attempts("B") > 1 }, 50*time.Millisecond, 5*time.Millisecond,
			"event for A woke the waiter for B")

		// The waiter for B must still wake for its own key.
		gate.hold("B")
		eventually(t, time.Second, func() bool { return gate.blocked("B") >= 1 })
		before := gate.attempts("B")
		require.NoError(t, admin.Publish(ctx, c.base+"events", "B").Err())
		gate.release("B")
		eventually(t, wakeWindow, func() bool { return gate.attempts("B") >= before+2 })

		cancel()
		for range 2 {
			require.ErrorIs(t, <-done, context.Canceled)
		}
		eventually(t, time.Second, func() bool { return waiterCount(c) == 0 })
	})

	// One publish must wake every waiter registered for that key.
	t.Run("notification wakes all waiters for its key", func(t *testing.T) {
		c, gate := testCacheWithGate(t, options, t.Name())
		waitForSubscribers(t, admin, c.base+"events")
		gate.register(c.keys("A")[1], "A")
		require.NoError(t, admin.Set(ctx, c.keys("A")[1], "owner", time.Minute).Err())

		gate.hold("A")
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		t.Cleanup(cancel)
		done := make(chan error, 2)
		for range 2 {
			go func() {
				_, err := c.GetOrFetch(waitCtx, "A")
				done <- err
			}()
		}
		eventually(t, time.Second, func() bool { return gate.blocked("A") == 2 })
		require.Zero(t, gate.attempts("A"))

		require.NoError(t, admin.Publish(ctx, c.base+"events", "A").Err())
		gate.release("A")
		eventually(t, wakeWindow, func() bool { return gate.attempts("A") >= 4 })

		cancel()
		for range 2 {
			require.ErrorIs(t, <-done, context.Canceled)
		}
		eventually(t, time.Second, func() bool { return waiterCount(c) == 0 })
	})

	t.Run("abandoned lock and permit expire", func(t *testing.T) {
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { return 5, nil }).LimitConcurrentFetches(1))
		keys := c.keys("key")
		require.NoError(t, admin.Set(ctx, keys[1], "dead-owner", 150*time.Millisecond).Err())
		now, err := admin.Time(ctx).Result()
		require.NoError(t, err)
		require.NoError(t, admin.ZAdd(ctx, keys[2], redis.Z{Member: "dead-owner", Score: float64(now.Add(150 * time.Millisecond).UnixMilli())}).Err())
		require.Equal(t, 5, mustGet(t, c, "key"), "did not recover expired owner")
	})

	t.Run("lost owner cannot overwrite replacement", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		a := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { close(entered); <-release; return 1, nil }).LeaseDuration(300*time.Millisecond))
		b := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { return 2, nil }).LeaseDuration(300*time.Millisecond))
		done := make(chan error, 1)
		go func() { _, e := a.GetOrFetch(ctx, "key"); done <- e }()
		<-entered
		// Simulate lease loss while an uncooperative old backend call still runs.
		require.NoError(t, admin.Del(ctx, a.keys("key")[1]).Err())
		require.Equal(t, 2, mustGet(t, b, "key"), "replacement value must survive")
		// The deferred close also unblocks this callback if an earlier assertion fails.
		release <- struct{}{}
		require.ErrorIs(t, <-done, ErrLeaseLost)
		require.Equal(t, 2, mustGet(t, b, "key"), "replacement value must survive")
	})

	t.Run("hot keys expire despite refresh", func(t *testing.T) {
		var calls atomic.Int64
		policy := AutoRefreshPolicy{HotKeyLifetime: 650 * time.Millisecond, UpdateInterval: 150 * time.Millisecond}
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { return int(calls.Add(1)), nil }).EnableAutoRefresh(policy).TTL(time.Second))
		// First read misses and fetches; the second read hits and makes the key hot.
		mustGet(t, c, "warm")
		mustGet(t, c, "warm")
		require.Equal(t, []string{"warm"}, c.HotKeys(ctx), "missing hot key")
		eventually(t, 2*time.Second, func() bool { return calls.Load() >= 2 })
		eventually(t, 2*time.Second, func() bool { return len(c.HotKeys(ctx)) == 0 })
		time.Sleep(200 * time.Millisecond)
		before := calls.Load()
		time.Sleep(250 * time.Millisecond)
		require.Equal(t, before, calls.Load(), "background fetch extended hot lifetime")
	})

	t.Run("failed miss is not refresh-eligible", func(t *testing.T) {
		backendErr := errors.New("backend unavailable")
		var calls atomic.Int64
		policy := AutoRefreshPolicy{HotKeyLifetime: 900 * time.Millisecond, UpdateInterval: 150 * time.Millisecond}
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).TTL(2*time.Second).
			Fetch(func(context.Context, string) (int, error) { calls.Add(1); return 0, backendErr }).
			EnableAutoRefresh(policy))
		_, err := c.GetOrFetch(ctx, "failed")
		require.ErrorIs(t, err, backendErr)
		require.Equal(t, int64(1), calls.Load(), "initial miss fetch")
		time.Sleep(time.Second) // several UpdateInterval periods
		require.Equal(t, int64(1), calls.Load(), "failed miss was retried by background refresh")
		require.Empty(t, c.HotKeys(ctx), "failed miss became hot")
	})

	t.Run("successful miss becomes hot on next hit", func(t *testing.T) {
		var calls atomic.Int64
		policy := AutoRefreshPolicy{HotKeyLifetime: 2 * time.Second, UpdateInterval: 150 * time.Millisecond}
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).TTL(3*time.Second).
			Fetch(func(context.Context, string) (int, error) { return int(calls.Add(1)), nil }).
			EnableAutoRefresh(policy))
		require.Equal(t, 1, mustGet(t, c, "key"), "miss fetch")
		require.Empty(t, c.HotKeys(ctx), "miss became hot")
		require.Equal(t, 1, mustGet(t, c, "key"), "hit")
		require.Equal(t, []string{"key"}, c.HotKeys(ctx), "hit did not register hot key")
		eventually(t, 2*time.Second, func() bool { return calls.Load() >= 2 })
	})

	t.Run("failed refresh backs off from failure", func(t *testing.T) {
		const interval = 500 * time.Millisecond
		backendErr := errors.New("backend unavailable")
		type attempt struct{ start, end time.Time }
		var mu sync.Mutex
		var attempts []attempt
		var calls atomic.Int64
		fetch := func(ctx context.Context, _ string) (int, error) {
			if calls.Add(1) == 1 {
				return 1, nil
			}
			start := time.Now()
			select {
			case <-time.After(800 * time.Millisecond):
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			end := time.Now()
			mu.Lock()
			attempts = append(attempts, attempt{start: start, end: end})
			mu.Unlock()
			return 0, backendErr
		}
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch).
			EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: 10 * time.Second, UpdateInterval: interval}))
		// Read twice: the miss fetches the value and the hit records hotness, so
		// the key stays eligible for refresh even when a miss does not mark it hot.
		require.Equal(t, 1, mustGet(t, c, "key"))
		require.Equal(t, 1, mustGet(t, c, "key"))
		eventually(t, 4*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(attempts) >= 2
		})
		mu.Lock()
		got := append([]attempt(nil), attempts...)
		mu.Unlock()
		require.GreaterOrEqual(t, len(got), 2)
		// Each failed fetch lasts longer than the interval, so without an explicit
		// post-failure backoff the key is already due when the attempt ends and the
		// worker retries after about one poll (100ms). The backoff must be measured
		// from the failure, not from the attempt start.
		for i := 1; i < len(got); i++ {
			require.GreaterOrEqual(t, got[i].start.Sub(got[i-1].end), interval*9/10,
				"failed refresh retried before UpdateInterval elapsed from the failure")
		}
	})

	t.Run("acquire skips permit prune without a limit", func(t *testing.T) {
		var calls atomic.Int64
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).
			Fetch(func(context.Context, string) (int, error) { return int(calls.Add(1)), nil }))
		keys := c.keys("key")
		require.NoError(t, admin.ZAdd(ctx, keys[2], redis.Z{Member: "stale", Score: 1}).Err())
		before := commandCalls(t, admin, "zremrangebyscore")
		require.Equal(t, 1, mustGet(t, c, "key"))
		require.Equal(t, before, commandCalls(t, admin, "zremrangebyscore"), "pruned permits without a limit")
		score, err := admin.ZScore(ctx, keys[2], "stale").Result()
		require.NoError(t, err, "stale permit removed without a limit")
		require.InDelta(t, 1, score, 0)

		limited := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()+"-limited").
			Fetch(func(context.Context, string) (int, error) { return 1, nil }).LimitConcurrentFetches(1))
		limitedKeys := limited.keys("key")
		require.NoError(t, admin.ZAdd(ctx, limitedKeys[2], redis.Z{Member: "stale", Score: 1}).Err())
		require.Equal(t, 1, mustGet(t, limited, "key"))
		require.ErrorIs(t, admin.ZScore(ctx, limitedKeys[2], "stale").Err(), redis.Nil, "limited acquire did not prune permits")
	})

	t.Run("refresh and foreground share permits", func(t *testing.T) {
		var active, peak, calls atomic.Int64
		fetch := func(ctx context.Context, _ string) (int, error) {
			calls.Add(1)
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			select {
			case <-time.After(60 * time.Millisecond):
				return 3, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		builder := NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch).
			LimitConcurrentFetches(1).EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: 2 * time.Second, UpdateInterval: 150 * time.Millisecond})
		a := testCache(t, builder)
		b := testCache(t, builder)
		// The first read fetches; the second hits and schedules background work.
		mustGet(t, a, "hot")
		mustGet(t, a, "hot")
		eventually(t, time.Second, func() bool { return calls.Load() >= 2 })
		mustGet(t, b, "cold")
		require.Equal(t, int64(1), peak.Load(), "refresh exceeded concurrency")
	})

	t.Run("metrics", func(t *testing.T) {
		received := make(chan MetricValues, 4)
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { return 1, nil }).MetricsReceiver(func(m MetricValues) { received <- m }).EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: 20 * time.Second, UpdateInterval: 10 * time.Second}))
		mustGet(t, c, "key")
		mustGet(t, c, "key")
		select {
		case m := <-received:
			require.Equal(t, MetricValues{HitCount: 1, MissCount: 1, BackendFetchCount: 1, DecodeErrorCount: 0, HotKeyCount: 1}, m)
		case <-time.After(3 * time.Second):
			require.FailNow(t, "no metrics")
		}
	})

	t.Run("decode errors invalidate corrupt values", func(t *testing.T) {
		for _, tt := range []struct {
			name    string
			refresh bool
		}{
			{name: "auto refresh off"},
			{name: "auto refresh on", refresh: true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				var calls atomic.Int64
				builder := NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).
					Fetch(func(context.Context, string) (int, error) { calls.Add(1); return 42, nil }).
					UseSerializer(corruptSerializer{})
				if tt.refresh {
					builder.EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: 2 * time.Minute, UpdateInterval: 30 * time.Second})
				}
				c := testCache(t, builder)
				valueKey := c.keys("key")[0]
				require.NoError(t, admin.Set(ctx, valueKey, corruptPayload, time.Minute).Err())

				_, err := c.GetOrFetch(ctx, "key")
				require.ErrorContains(t, err, "decode")
				require.Zero(t, admin.Exists(ctx, valueKey).Val(), "corrupt value survived")
				require.Zero(t, calls.Load(), "corrupt hit reached the backend")

				value, err := c.GetOrFetch(ctx, "key")
				require.NoError(t, err)
				require.Equal(t, 42, value)
				require.Equal(t, int64(1), calls.Load(), "corrupt value was not rebuilt")
				stored, err := admin.Get(ctx, valueKey).Result()
				require.NoError(t, err)
				require.Equal(t, "42", stored, "valid value not stored")
			})
		}
	})

	t.Run("decode error metrics", func(t *testing.T) {
		received := make(chan MetricValues, 4)
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).
			Fetch(func(context.Context, string) (int, error) { return 42, nil }).
			UseSerializer(corruptSerializer{}).
			MetricsReceiver(func(m MetricValues) { received <- m }))
		require.NoError(t, admin.Set(ctx, c.keys("key")[0], corruptPayload, time.Minute).Err())
		_, err := c.GetOrFetch(ctx, "key")
		require.ErrorContains(t, err, "decode")
		deadline := time.After(3 * time.Second)
		for {
			select {
			case m := <-received:
				if m.DecodeErrorCount < 1 {
					continue
				}
				require.Equal(t, MetricValues{HitCount: 1, DecodeErrorCount: 1}, m)
				return
			case <-deadline:
				require.FailNow(t, "no metrics with decode error")
			}
		}
	})

	t.Run("compare and delete preserves replacement", func(t *testing.T) {
		valueKey := t.Name() + ":value"
		require.NoError(t, admin.Set(ctx, valueKey, "fresh", time.Minute).Err())
		deleted, err := deleteValueScript.Run(ctx, admin, []string{valueKey}, corruptPayload).Int()
		require.NoError(t, err)
		require.Zero(t, deleted, "deleted a replacement")
		require.Equal(t, "fresh", admin.Get(ctx, valueKey).Val())
		deleted, err = deleteValueScript.Run(ctx, admin, []string{valueKey}, "fresh").Int()
		require.NoError(t, err)
		require.Equal(t, 1, deleted, "did not delete matching value")
		require.Zero(t, admin.Exists(ctx, valueKey).Val())
	})

	t.Run("shared refresh pacing", func(t *testing.T) {
		starts := make(chan time.Time, 32)
		fetch := func(context.Context, string) (int, error) {
			starts <- time.Now()
			return 1, nil
		}
		policy := AutoRefreshPolicy{HotKeyLifetime: 5 * time.Second, UpdateInterval: 800 * time.Millisecond}
		a := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch).EnableAutoRefresh(policy))
		_ = testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(fetch).EnableAutoRefresh(policy))
		now, err := admin.Time(ctx).Result()
		require.NoError(t, err)
		// Make all keys due together, independent of foreground fetch timing.
		_, err = admin.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for i := 0; i < 4; i++ {
				key := fmt.Sprint(i)
				pipe.ZAdd(ctx, a.base+"hot", redis.Z{Member: key, Score: float64(now.Add(5 * time.Second).UnixMilli())})
				pipe.ZAdd(ctx, a.base+"due", redis.Z{Member: key, Score: float64(now.UnixMilli())})
			}
			return nil
		})
		require.NoError(t, err)
		var previous time.Time
		for i := 0; i < 4; i++ {
			select {
			case start := <-starts:
				// Expected spacing is 200ms. Allow scheduling/transport jitter.
				if !previous.IsZero() {
					require.GreaterOrEqual(t, start.Sub(previous), 120*time.Millisecond, "refresh burst across replicas")
				}
				previous = start
			case <-time.After(3 * time.Second):
				require.FailNow(t, "refresh did not start")
			}
		}
	})

	t.Run("expired permit rejects renewal and commit", func(t *testing.T) {
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { return 1, nil }).LimitConcurrentFetches(1))
		keys := c.keys("key")
		require.NoError(t, admin.Set(ctx, keys[1], "old", time.Second).Err())
		require.NoError(t, admin.ZAdd(ctx, keys[2], redis.Z{Member: "old", Score: 1}).Err())
		renewed, err := renewScript.Run(ctx, admin, []string{keys[1], keys[2]}, "old", 1000, 1).Int()
		require.NoError(t, err)
		require.Zero(t, renewed, "renewed expired permit")
		committed, err := commitScript.Run(ctx, admin, []string{keys[0], keys[1], keys[2], keys[4], keys[5]}, "old", "1", 1000, 1, "key", 0, 0).Int()
		require.NoError(t, err)
		require.Zero(t, committed, "committed with expired permit")
		require.Zero(t, admin.Exists(ctx, keys[0]).Val(), "expired owner stored a value")
	})

	t.Run("script replay is idempotent", func(t *testing.T) {
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { return 1, nil }))
		keys := c.keys("key")
		acquire := func(token string) []interface{} {
			t.Helper()
			result, err := acquireScript.Run(ctx, admin, keys[:4], token, 30000, 1, 0, "key").Slice()
			require.NoError(t, err)
			return result
		}

		// First acquisition takes the lock and a global permit.
		require.Equal(t, []interface{}{int64(2)}, acquire("token-a"))
		require.Equal(t, "token-a", admin.Get(ctx, keys[1]).Val())
		_, err := admin.ZScore(ctx, keys[2], "token-a").Result()
		require.NoError(t, err)

		// Age the lease and permit, then replay the same token as a retried EVALSHA would.
		require.NoError(t, admin.PExpire(ctx, keys[1], 100*time.Millisecond).Err())
		require.NoError(t, admin.ZAdd(ctx, keys[2], redis.Z{Member: "token-a", Score: 1}).Err())
		require.Equal(t, []interface{}{int64(2)}, acquire("token-a"), "replay reported own lock as busy")
		require.Equal(t, "token-a", admin.Get(ctx, keys[1]).Val(), "replay released ownership")
		require.Greater(t, admin.PTTL(ctx, keys[1]).Val(), 20*time.Second, "replay did not renew the lease")
		renewed, err := admin.ZScore(ctx, keys[2], "token-a").Result()
		require.NoError(t, err)
		// The score was aged to 1 above, so any real renewal is far greater even
		// when the replay lands in the same Redis millisecond as the first acquire.
		require.Greater(t, renewed, float64(1), "replay did not renew the permit")
		require.Greater(t, admin.PTTL(ctx, keys[2]).Val(), 20*time.Second, "replay did not extend the permit set")

		// A different token must still see the lock as busy.
		require.Equal(t, []interface{}{int64(0)}, acquire("token-b"), "foreign token bypassed the lock")
		require.Equal(t, "token-a", admin.Get(ctx, keys[1]).Val())

		payload := "42"
		committed, err := commitScript.Run(ctx, admin, []string{keys[0], keys[1], keys[2], keys[4], keys[5]}, "token-a", payload, 30000, 1, "key", 0).Int()
		require.NoError(t, err)
		require.Equal(t, 1, committed)
		require.Equal(t, payload, admin.Get(ctx, keys[0]).Val())
		require.Zero(t, admin.Exists(ctx, keys[1]).Val())
		_, err = admin.ZScore(ctx, keys[2], "token-a").Result()
		require.ErrorIs(t, err, redis.Nil)

		// Replaying the commit with the same token and payload reports the earlier success.
		committed, err = commitScript.Run(ctx, admin, []string{keys[0], keys[1], keys[2], keys[4], keys[5]}, "token-a", payload, 30000, 1, "key", 0).Int()
		require.NoError(t, err)
		require.Equal(t, 1, committed, "replayed commit reported lost lease")
		require.Equal(t, payload, admin.Get(ctx, keys[0]).Val())
		require.Zero(t, admin.Exists(ctx, keys[1]).Val())

		// A different token with the already stored payload also reports the stored
		// result without side effects; the contract only forbids overwriting and
		// resurrecting ownership.
		committed, err = commitScript.Run(ctx, admin, []string{keys[0], keys[1], keys[2], keys[4], keys[5]}, "token-b", payload, 30000, 1, "key", 0).Int()
		require.NoError(t, err)
		require.Equal(t, 1, committed)
		require.Equal(t, payload, admin.Get(ctx, keys[0]).Val())
		require.Zero(t, admin.Exists(ctx, keys[1]).Val(), "replay resurrected ownership")

		// A different payload from a non-owner must not overwrite the value.
		committed, err = commitScript.Run(ctx, admin, []string{keys[0], keys[1], keys[2], keys[4], keys[5]}, "token-b", "other", 30000, 1, "key", 0).Int()
		require.NoError(t, err)
		require.Zero(t, committed)
		require.Equal(t, payload, admin.Get(ctx, keys[0]).Val(), "lost owner overwrote the value")
		require.Zero(t, admin.Exists(ctx, keys[1]).Val(), "lost owner resurrected the lock")
	})

	t.Run("shutdown and caller owned client", func(t *testing.T) {
		client := redis.NewClient(options)
		defer func() { _ = client.Close() }()
		entered := make(chan struct{})
		c, err := NewBuilder[string, int]().
			UseRedisClient(client).
			KeyPrefix(t.Name()).
			Fetch(func(ctx context.Context, _ string) (int, error) { close(entered); <-ctx.Done(); return 0, ctx.Err() }).
			Build()
		require.NoError(t, err)
		t.Cleanup(c.Shutdown)
		done := make(chan error, 1)
		go func() { _, e := c.GetOrFetch(ctx, "key"); done <- e }()
		<-entered
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); c.Shutdown() }()
		}
		wg.Wait()
		require.ErrorIs(t, <-done, context.Canceled)
		_, err = c.GetOrFetch(ctx, "key")
		require.ErrorIs(t, err, ErrClosed)
		require.Nil(t, c.HotKeys(ctx), "closed HotKeys")
		require.NoError(t, client.Ping(ctx).Err(), "caller-owned client was closed")
	})

	t.Run("Redis failure does not bypass coordination", func(t *testing.T) {
		client := redis.NewClient(options)
		var calls atomic.Int64
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { calls.Add(1); return 1, nil }).UseRedisClient(client).EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: 2 * time.Second, UpdateInterval: time.Second}))
		require.NoError(t, client.Close())
		_, err := c.GetOrFetch(ctx, "key")
		require.Error(t, err, "expected Redis error")
		require.Zero(t, calls.Load(), "fetched without coordination")
		require.Nil(t, c.HotKeys(ctx), "HotKeys should return nil on Redis failure")
	})

	t.Run("custom serializer and named key", func(t *testing.T) {
		type key string
		c, err := NewBuilder[key, int]().
			RedisURL(url).
			KeyPrefix(t.Name()).
			Fetch(func(context.Context, key) (int, error) { return 17, nil }).
			UseSerializer(integerSerializer{}).
			Build()
		require.NoError(t, err)
		defer c.Shutdown()
		for i := 0; i < 2; i++ {
			v, e := c.GetOrFetch(ctx, key("name"))
			require.NoError(t, e)
			require.Equal(t, 17, v)
		}
		broken := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()+"broken").Fetch(func(context.Context, string) (int, error) { return 1, nil }).UseSerializer(brokenSerializer{}))
		_, err = broken.GetOrFetch(ctx, "key")
		require.Error(t, err, "encode error lost")
		require.NoError(t, admin.Set(ctx, broken.keys("key")[0], "corrupt", time.Second).Err())
		_, err = broken.GetOrFetch(ctx, "key")
		require.Error(t, err, "decode error lost")
		require.Zero(t, admin.Exists(ctx, broken.keys("key")[0]).Val(), "undecodable value not invalidated")
	})
}

const corruptPayload = "corrupt"

type corruptSerializer struct{}

func (corruptSerializer) Marshal(v int) ([]byte, error) { return []byte(fmt.Sprint(v)), nil }
func (corruptSerializer) Unmarshal(data []byte, v *int) error {
	if string(data) == corruptPayload {
		return errors.New("corrupt value")
	}
	_, err := fmt.Sscan(string(data), v)
	return err
}

type integerSerializer struct{}

func (integerSerializer) Marshal(v int) ([]byte, error) { return []byte(fmt.Sprint(v)), nil }
func (integerSerializer) Unmarshal(data []byte, v *int) error {
	_, err := fmt.Sscan(string(data), v)
	return err
}

type brokenSerializer struct{}

func (brokenSerializer) Marshal(int) ([]byte, error)  { return nil, errors.New("encode") }
func (brokenSerializer) Unmarshal([]byte, *int) error { return errors.New("decode") }

func acquireTestClient(options *redis.Options) *redis.Client {
	opts := *options
	opts.MaxRetries = -1 // The hook injects the failure; the client must not replay it.
	return redis.NewClient(&opts)
}

func primeScripts(t *testing.T, client redis.Scripter) {
	t.Helper()
	for _, script := range []*redis.Script{readScript, acquireScript, releaseScript} {
		require.NoError(t, script.Load(context.Background(), client).Err())
	}
}

// assertAcquireCleaned proves the acquire script executed server-side before the
// reply was dropped, and that no lock or permit remained for its token.
func assertAcquireCleaned(t *testing.T, admin *redis.Client, keys []string, hook *acquireReplyHook) {
	t.Helper()
	nextErr, _ := hook.nextErr.Load().(error)
	require.NoError(t, nextErr, "acquire did not reach the server")
	token, ok := hook.token.Load().(string)
	require.True(t, ok, "acquire token not observed")
	require.ErrorIs(t, admin.Get(context.Background(), keys[1]).Err(), redis.Nil, "lock left behind")
	require.ErrorIs(t, admin.ZScore(context.Background(), keys[2], token).Err(), redis.Nil, "permit left behind")
}

// acquireReplyHook drops the reply of the first successful acquire EVALSHA after
// the server executed it, leaving the created lock and permit in place. deadline
// waits for the caller context instead, covering a timeout observed after the
// script ran. Other commands and later acquires pass through untouched.
type acquireReplyHook struct {
	drops    atomic.Int64
	deadline bool
	token    atomic.Value // string
	nextErr  atomic.Value // error
}

func (h *acquireReplyHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *acquireReplyHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *acquireReplyHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if cmd.Name() != "evalsha" || len(args) < 8 || fmt.Sprint(args[1]) != acquireScript.Hash() {
			return next(ctx, cmd)
		}

		if err := next(ctx, cmd); err != nil {
			h.nextErr.Store(err)
			return err
		}

		// Only status 2 creates a lock and, with a limit, a permit. Returning the
		// real reply for other statuses keeps failure injection out of those paths.
		reply, ok := cmd.(*redis.Cmd)
		if !ok {
			return nil
		}
		result, ok := reply.Val().([]interface{})
		if !ok || len(result) == 0 || result[0] != int64(2) {
			return nil
		}
		if h.drops.Add(-1) < 0 {
			return nil
		}
		h.token.Store(fmt.Sprint(args[7]))
		if h.deadline {
			<-ctx.Done()
			return ctx.Err()
		}
		return io.EOF
	}
}
