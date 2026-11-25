package r2cache

import (
	"context"
	"errors"
	"fmt"
	"os"
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

// renewInjectionHook intercepts renew-script EVALSHAs and fails the first
// pending ones with a transport-style error, without sending them to Redis. Later
// calls pass through and report whether Redis confirmed the renewal. failures is
// signaled on each injection so the fetch callback can wait for the fault instead
// of guessing when the renewal goroutine will fire.
type renewInjectionHook struct {
	mu        sync.Mutex
	pending   int
	failed    int
	succeeded int
	failures  chan struct{}
}

func newRenewInjectionHook(pending int) *renewInjectionHook {
	return &renewInjectionHook{pending: pending, failures: make(chan struct{}, pending)}
}

func (h *renewInjectionHook) counts() (failed, succeeded int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failed, h.succeeded
}

func (h *renewInjectionHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *renewInjectionHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if len(args) < 2 || !strings.EqualFold(fmt.Sprint(args[0]), "evalsha") || fmt.Sprint(args[1]) != renewScript.Hash() {
			return next(ctx, cmd)
		}

		h.mu.Lock()
		inject := h.pending > 0
		if inject {
			h.pending--
			h.failed++
		}
		h.mu.Unlock()

		if inject {
			select {
			case h.failures <- struct{}{}:
			default:
			}
			// Returning without calling next keeps the command off the wire, so
			// renew observes a transport error rather than a Redis reply.
			return errors.New("injected renewal failure: connection reset by peer")
		}

		err := next(ctx, cmd)
		if err == nil {
			h.mu.Lock()
			h.succeeded++
			h.mu.Unlock()
		}
		return err
	}
}

func (h *renewInjectionHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestIssue36Renewal(t *testing.T) {
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

	t.Run("transient renewal failure is retried", func(t *testing.T) {
		const lease = time.Second
		hook := newRenewInjectionHook(1)
		client := redis.NewClient(options)
		client.AddHook(hook)
		t.Cleanup(func() { _ = client.Close() })
		// Preload the script so every pass-through EVALSHA really executes
		// instead of falling back to EVAL after NOSCRIPT.
		require.NoError(t, renewScript.Load(ctx, client).Err())

		var fetches atomic.Int64
		cache, err := NewBuilder[string, int]().
			UseRedisClient(client).
			KeyPrefix("issue36:retry").
			LeaseDuration(lease).
			TTL(time.Minute).
			Fetch(func(ctx context.Context, _ string) (int, error) {
				fetches.Add(1)
				select {
				case <-hook.failures:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
				// Let the renewal goroutine retry after the injected failure and
				// confirm the lease again before the callback finishes.
				select {
				case <-time.After(200 * time.Millisecond):
					return 7, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}).
			Build()
		require.NoError(t, err)
		t.Cleanup(cache.Shutdown)

		fetchCtx, cancelFetch := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFetch()
		value, err := cache.GetOrFetch(fetchCtx, "product")
		require.NoError(t, err, "one failed renewal must not abort a healthy fetch")
		require.Equal(t, 7, value)
		require.Equal(t, int64(1), fetches.Load())

		failed, succeeded := hook.counts()
		require.Equal(t, 1, failed, "exactly one renewal attempt was injected")
		require.GreaterOrEqual(t, succeeded, 1, "a later renewal succeeded")

		stored, err := admin.Get(ctx, cache.keys("product")[0]).Result()
		require.NoError(t, err)
		require.Equal(t, "7", stored)

		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		value, err = cache.GetOrFetch(readCtx, "product")
		require.NoError(t, err)
		require.Equal(t, 7, value)
		require.Equal(t, int64(1), fetches.Load(), "committed value must be readable without another fetch")
	})

	t.Run("ownership loss still cancels", func(t *testing.T) {
		const lease = 600 * time.Millisecond
		var fetched atomic.Bool
		var cache *Cache[string, int]
		build, err := NewBuilder[string, int]().
			RedisURL(url).
			KeyPrefix("issue36:lost").
			LeaseDuration(lease).
			TTL(time.Minute).
			Fetch(func(ctx context.Context, key string) (int, error) {
				// Remove the lock while the callback is running. The next
				// renew.lua then reports 0, which is definite ownership loss.
				if err := admin.Del(context.Background(), cache.keys(key)[1]).Err(); err != nil {
					return 0, err
				}
				fetched.Store(true)
				<-ctx.Done()
				return 0, ctx.Err()
			}).
			Build()
		require.NoError(t, err)
		cache = build
		t.Cleanup(cache.Shutdown)

		start := time.Now()
		value, err := cache.GetOrFetch(ctx, "product")
		require.ErrorIs(t, err, ErrLeaseLost)
		require.Zero(t, value)
		require.True(t, fetched.Load(), "fetch callback ran before ownership loss")
		require.Less(t, time.Since(start), 3*lease, "ownership loss must cancel promptly")
	})
}
