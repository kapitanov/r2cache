package r2cache

import (
	"context"
	"errors"
	"fmt"
	"os"
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
		policy := AutoRefreshPolicy{HotKeyLifetime: time.Minute, UpdateInterval: time.Hour}
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
		mustGet(t, c, "warm")
		require.Equal(t, []string{"warm"}, c.HotKeys(ctx), "missing hot key")
		eventually(t, 2*time.Second, func() bool { return calls.Load() >= 2 })
		eventually(t, 2*time.Second, func() bool { return len(c.HotKeys(ctx)) == 0 })
		time.Sleep(200 * time.Millisecond)
		before := calls.Load()
		time.Sleep(250 * time.Millisecond)
		require.Equal(t, before, calls.Load(), "background fetch extended hot lifetime")
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
		mustGet(t, a, "hot")
		eventually(t, time.Second, func() bool { return calls.Load() >= 2 })
		mustGet(t, b, "cold")
		require.Equal(t, int64(1), peak.Load(), "refresh exceeded concurrency")
	})

	t.Run("metrics", func(t *testing.T) {
		received := make(chan MetricValues, 4)
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { return 1, nil }).MetricsReceiver(func(m MetricValues) { received <- m }).EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: 5 * time.Second, UpdateInterval: 10 * time.Second}))
		mustGet(t, c, "key")
		mustGet(t, c, "key")
		select {
		case m := <-received:
			require.Equal(t, MetricValues{HitCount: 1, MissCount: 1, BackendFetchCount: 1, HotKeyCount: 1}, m)
		case <-time.After(3 * time.Second):
			require.FailNow(t, "no metrics")
		}
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
		committed, err := commitScript.Run(ctx, admin, keys, "old", "1", 1000, 1, "key", 0).Int()
		require.NoError(t, err)
		require.Zero(t, committed, "committed with expired permit")
		require.Zero(t, admin.Exists(ctx, keys[0]).Val(), "expired owner stored a value")
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
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).Fetch(func(context.Context, string) (int, error) { calls.Add(1); return 1, nil }).UseRedisClient(client).EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: time.Second, UpdateInterval: time.Second}))
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
	})
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
