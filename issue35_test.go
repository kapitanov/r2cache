package r2cache

import (
	"context"
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

// readMarker tags contexts passed to GetOrFetch so the fetch callback can tell
// foreground reads from background refresh attempts, which run on the cache's
// internal context.
type readMarker struct{}

// oneOffMisses fetches distinct keys concurrently so a 500-key burst does not
// dominate the test runtime.
func oneOffMisses(t *testing.T, ctx context.Context, c *Cache[string, int], count, workers int) {
	t.Helper()

	work := make(chan int)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range work {
				op, cancel := context.WithTimeout(ctx, 30*time.Second)
				_, err := c.GetOrFetch(op, fmt.Sprintf("cold-%d", n))
				cancel()
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := range count {
		work <- i
	}
	close(work)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestIssue35(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker")
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

	// A cold commit must not schedule refresh, so one-off misses cannot grow
	// the due index or the hot index.
	t.Run("one-off cold misses leave indexes empty", func(t *testing.T) {
		policy := AutoRefreshPolicy{HotKeyLifetime: 2 * time.Second, UpdateInterval: 200 * time.Millisecond}
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).TTL(time.Minute).
			Fetch(func(context.Context, string) (int, error) { return 1, nil }).
			EnableAutoRefresh(policy))
		keys := c.keys("probe")

		const misses = 500
		oneOffMisses(t, ctx, c, misses, 25)
		require.Equal(t, int64(misses), c.misses.Load(), "not every one-off fetch was a miss")
		require.Zero(t, admin.ZCard(ctx, keys[3]).Val(), "cold misses made keys hot")
		require.Zero(t, admin.ZCard(ctx, keys[4]).Val(), "cold misses scheduled due entries")
	})

	// The normal miss-then-hit path: the first hit schedules exactly one due
	// entry, and a further immediate hit neither moves nor duplicates it.
	t.Run("first hit schedules exactly one due entry", func(t *testing.T) {
		policy := AutoRefreshPolicy{HotKeyLifetime: 10 * time.Second, UpdateInterval: time.Second}
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).TTL(time.Minute).
			Fetch(func(context.Context, string) (int, error) { return 1, nil }).
			EnableAutoRefresh(policy))
		keys := c.keys("key")

		require.Equal(t, 1, mustGet(t, c, "key"), "cold fetch")
		require.Zero(t, admin.ZCard(ctx, keys[4]).Val(), "cold fetch scheduled a due entry")

		require.Equal(t, 1, mustGet(t, c, "key"), "first hit")
		due, err := admin.ZScore(ctx, keys[4], "key").Result()
		require.NoError(t, err, "first hit did not schedule a refresh")
		require.Equal(t, int64(1), admin.ZCard(ctx, keys[4]).Val(), "first hit added more than one due entry")

		require.Equal(t, 1, mustGet(t, c, "key"), "second hit")
		dueAfter, err := admin.ZScore(ctx, keys[4], "key").Result()
		require.NoError(t, err)
		require.InDelta(t, due, dueAfter, 0, "second hit moved the due score")
		require.Equal(t, int64(1), admin.ZCard(ctx, keys[4]).Val(), "second hit duplicated the due entry")
	})

	// The issue #35 mixed scenario: one hot key stays refreshed while 500
	// one-off cold misses pass through the same namespace. Generous bounds keep
	// this stable in CI; the pre-fix code starves at roughly 4 refreshes per 4s.
	t.Run("hot key refreshes beside one-off misses", func(t *testing.T) {
		const (
			coldKeys = 500
			readFor  = 4500 * time.Millisecond
		)
		readEvery := 100 * time.Millisecond
		policy := AutoRefreshPolicy{HotKeyLifetime: 2 * time.Second, UpdateInterval: 200 * time.Millisecond}
		var background, foreground atomic.Int64
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).KeyPrefix(t.Name()).TTL(time.Second).
			Fetch(func(fctx context.Context, key string) (int, error) {
				if key == "hot" {
					if fctx.Value(readMarker{}) != nil {
						foreground.Add(1)
					} else {
						background.Add(1)
					}
				}
				return 1, nil
			}).
			EnableAutoRefresh(policy))
		keys := c.keys("hot")

		require.Equal(t, 1, mustGet(t, c, "hot"), "initial fetch")
		require.Equal(t, 1, mustGet(t, c, "hot"), "hit makes the key hot")
		require.Equal(t, int64(1), admin.ZCard(ctx, keys[4]).Val(), "first hit did not schedule one due entry")

		oneOffMisses(t, ctx, c, coldKeys, 25)

		missesBefore := c.misses.Load()
		backgroundBefore := background.Load()
		deadline := time.Now().Add(readFor)
		for time.Now().Before(deadline) {
			op, cancel := context.WithTimeout(context.WithValue(ctx, readMarker{}, true), 5*time.Second)
			value, err := c.GetOrFetch(op, "hot")
			cancel()
			require.NoError(t, err)
			require.Equal(t, 1, value)
			time.Sleep(readEvery)
		}

		require.LessOrEqual(t, c.misses.Load()-missesBefore, int64(1), "foreground misses on the hot key")
		require.LessOrEqual(t, foreground.Load(), int64(1), "foreground fetches of the hot key")
		require.GreaterOrEqual(t, background.Load()-backgroundBefore, int64(10), "background refreshes of the hot key")
		require.LessOrEqual(t, admin.ZCard(ctx, keys[4]).Val(), int64(3), "due index grew")
	})
}
