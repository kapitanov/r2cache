package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/kapitanov/r2cache"
)

func TestBackend(t *testing.T) {
	b := newBackend(time.Millisecond)
	ctx := context.Background()
	first, err := b.fetch(ctx, "my-product-1")
	require.NoError(t, err)
	require.Equal(t, productID("my-product-1"), first.ID)
	require.Equal(t, 1250, first.PriceCents)
	require.Equal(t, 1, first.Revision)
	require.False(t, first.FetchedAt.IsZero())
	second, err := b.fetch(ctx, first.ID)
	require.NoError(t, err)
	require.Equal(t, 2, second.Revision)
	require.False(t, second.FetchedAt.Before(first.FetchedAt))
	_, err = b.fetch(ctx, "missing")
	require.ErrorIs(t, err, errNotFound)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	b.latency = time.Hour
	_, err = b.fetch(canceled, first.ID)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, b.active, "failed and canceled calls must release their active count")
	require.Equal(t, 4, b.calls)
	require.Equal(t, 2, b.catalog[first.ID].Revision, "cancellation must not update the catalog")
}

func TestRunInvalidConnection(t *testing.T) {
	err := run(context.Background(), settings{redisURL: "http://invalid"})
	require.ErrorContains(t, err, "create replica 1")
}

func TestExampleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration tests require Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	image := os.Getenv("R2CACHE_TEST_IMAGE")
	if image == "" {
		image = "redis:7.4.2-alpine"
	}
	container, err := tcredis.Run(ctx, image)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)
	url, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	options, err := redis.ParseURL(url)
	require.NoError(t, err)
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })

	t.Run("complete demonstration", func(t *testing.T) {
		prefix := "example-complete"
		require.NoError(t, run(ctx, settings{redisURL: url, prefix: prefix, readers: 12, latency: 50 * time.Millisecond}))
		// Inspect persisted results after run has shut down both replicas: refresh
		// must have advanced the first product, while errors must never be stored.
		key := func(id string) string { return prefix + ":value:" + base64.RawURLEncoding.EncodeToString([]byte(id)) }
		data, err := client.Get(ctx, key("my-product-1")).Bytes()
		require.NoError(t, err)
		var refreshed product
		require.NoError(t, json.Unmarshal(data, &refreshed))
		require.Greater(t, refreshed.Revision, 1)
		for _, id := range []string{"my-missing-product", "my-product-10"} {
			_, err := client.Get(ctx, key(id)).Result()
			require.ErrorIs(t, err, redis.Nil)
		}
	})

	t.Run("canceled demonstration", func(t *testing.T) {
		canceled, stop := context.WithCancel(ctx)
		stop()
		err := run(canceled, settings{redisURL: url, prefix: "example-canceled", readers: 2, latency: 50 * time.Millisecond})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("concurrent read failures", func(t *testing.T) {
		b := newBackend(time.Millisecond)
		cache, err := r2cache.NewBuilder[productID, product]().UseRedisClient(client).
			KeyPrefix("example-errors").Fetch(b.fetch).Build()
		require.NoError(t, err)
		defer cache.Shutdown()
		err = readConcurrently(ctx, []*r2cache.Cache[productID, product]{cache}, []productID{"missing-a", "my-product-1", "missing-b"})
		require.ErrorIs(t, err, errNotFound)
		b.mu.Lock()
		defer b.mu.Unlock()
		require.Zero(t, b.active, "all readers must finish before returning an error")
		require.Equal(t, 3, b.calls)
		require.Equal(t, 1, b.catalog["my-product-1"].Revision)
	})
}
