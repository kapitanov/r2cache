package r2cache

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestBuilderValidation(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Builder[string, int])
		message   string
	}{
		{"bad URL", func(b *Builder[string, int]) { b.RedisURL("http://localhost") }, "invalid URL scheme"},
		{"nil client", func(b *Builder[string, int]) { b.UseRedisClient(nil) }, "Redis connection is required"},
		{"missing connection", func(b *Builder[string, int]) { b.RedisURL("") }, "Redis connection is required"},
		{"nil fetcher", func(b *Builder[string, int]) { b.Fetch(nil) }, "fetcher is required"},
		{"nil serializer", func(b *Builder[string, int]) { b.UseSerializer(nil) }, "nil serializer"},
		{"TTL", func(b *Builder[string, int]) { b.TTL(0) }, "TTL must be at least 1ms"},
		{"submillisecond TTL", func(b *Builder[string, int]) { b.TTL(time.Microsecond) }, "TTL must be at least 1ms"},
		{"lease", func(b *Builder[string, int]) { b.LeaseDuration(time.Millisecond) }, "lease must be at least 100ms"},
		{"zero concurrency", func(b *Builder[string, int]) { b.LimitConcurrentFetches(0) }, "concurrency limit must be positive"},
		{"negative concurrency", func(b *Builder[string, int]) { b.LimitConcurrentFetches(-1) }, "concurrency limit must be positive"},
		{"hot lifetime", func(b *Builder[string, int]) { b.EnableAutoRefresh(AutoRefreshPolicy{UpdateInterval: time.Second}) }, "refresh durations must be at least 1ms"},
		{"refresh interval", func(b *Builder[string, int]) { b.EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: time.Second}) }, "refresh durations must be at least 1ms"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewBuilder[string, int]().RedisURL("redis://localhost:6379").Fetch(func(context.Context, string) (int, error) { return 1, nil })
			tt.configure(builder)
			cache, err := builder.Build()
			if cache != nil {
				t.Cleanup(cache.Shutdown)
			}
			require.ErrorContains(t, err, tt.message)
			require.Nil(t, cache)
		})
	}
	var absent *Builder[string, int]
	_, err := absent.Build()
	require.ErrorContains(t, err, "nil builder")
	_, err = NewBuilder[string, int]().Build()
	require.ErrorContains(t, err, "fetcher is required")
}

func TestBuilderRefreshPolicy(t *testing.T) {
	tests := []struct {
		name               string
		hot, ttl, interval time.Duration
		message            string
	}{
		{"interval above hot lifetime", time.Second, DefaultTTL, 2 * time.Second, "UpdateInterval (2s) must be less than HotKeyLifetime (1s)"},
		{"interval equals hot lifetime", time.Second, DefaultTTL, time.Second, "UpdateInterval (1s) must be less than HotKeyLifetime (1s)"},
		{"interval equals truncated hot lifetime", 1500 * time.Microsecond, DefaultTTL, time.Millisecond, "UpdateInterval (1ms) must be less than HotKeyLifetime (1.5ms)"},
		{"interval above TTL", time.Minute, 5 * time.Second, 10 * time.Second, "UpdateInterval (10s) must be less than TTL (5s)"},
		{"interval equals TTL", time.Minute, 5 * time.Second, 5 * time.Second, "UpdateInterval (5s) must be less than TTL (5s)"},
		{"interval equals truncated TTL", time.Minute, 1500 * time.Microsecond, time.Millisecond, "UpdateInterval (1ms) must be less than TTL (1.5ms)"},
		{"valid policy", time.Minute, DefaultTTL, time.Second, ""},
		{"valid policy below both boundaries", time.Second, time.Second, 999 * time.Millisecond, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := offlineBuilder(t).TTL(tt.ttl).EnableAutoRefresh(AutoRefreshPolicy{
				HotKeyLifetime: tt.hot,
				UpdateInterval: tt.interval,
			})
			cache, err := builder.Build()
			if tt.message == "" {
				require.NoError(t, err)
				require.NotNil(t, cache)
				require.Equal(t, tt.hot, cache.cfg.refresh.HotKeyLifetime)
				require.Equal(t, tt.interval, cache.cfg.refresh.UpdateInterval)
				cache.Shutdown()
				return
			}
			require.ErrorContains(t, err, tt.message)
			require.Nil(t, cache)
		})
	}
}

// Build is deliberately offline: a closed caller-owned client is enough for
// configuration tests, and its notification worker exits without dialing Redis.
func offlineBuilder(t *testing.T) *Builder[string, int] {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	require.NoError(t, client.Close())
	return NewBuilder[string, int]().UseRedisClient(client).Fetch(func(context.Context, string) (int, error) { return 1, nil })
}

func TestBuilderDefaults(t *testing.T) {
	builder := offlineBuilder(t)
	cache, err := builder.Build()
	require.NoError(t, err)
	defer cache.Shutdown()
	require.Equal(t, DefaultTTL, cache.cfg.ttl)
	require.Equal(t, DefaultLeaseDuration, cache.cfg.lease)
	require.Empty(t, cache.cfg.prefix)
	require.Zero(t, cache.cfg.max)
	require.Nil(t, cache.cfg.refresh)
	require.Nil(t, cache.cfg.metrics)
	require.IsType(t, NewJSONSerializer[int](), cache.cfg.serializer)
	require.False(t, cache.owned)
}

func TestBuilderCorrectionsAndOverrides(t *testing.T) {
	builder := offlineBuilder(t)
	require.Same(t, builder, builder.TTL(0))
	cache, err := builder.Build()
	require.Error(t, err)
	require.Nil(t, cache)

	builder.TTL(time.Second).LeaseDuration(time.Second).
		LimitConcurrentFetches(0).LimitConcurrentFetches(2).
		UseSerializer(nil).UseJSONSerializer().
		EnableAutoRefresh(AutoRefreshPolicy{}).DisableAutoRefresh().
		MetricsReceiver(func(MetricValues) {}).MetricsReceiver(nil)
	cache, err = builder.Build()
	require.NoError(t, err)
	defer cache.Shutdown()
	require.Equal(t, time.Second, cache.cfg.ttl)
	require.Equal(t, time.Second, cache.cfg.lease)
	require.Equal(t, 2, cache.cfg.max)
	require.Nil(t, cache.cfg.refresh)
	require.Nil(t, cache.cfg.metrics)
	require.IsType(t, NewJSONSerializer[int](), cache.cfg.serializer)

	// Connection setters replace one another, including an invalid earlier URL.
	client := builder.cfg.client
	builder.RedisURL("http://invalid").UseRedisClient(client)
	cache2, err := builder.Build()
	require.NoError(t, err)
	defer cache2.Shutdown()
	require.Same(t, client, cache2.client)
	builder.RedisURL("redis://localhost:6379")
	require.Nil(t, builder.cfg.client)
	require.Equal(t, "redis://localhost:6379", builder.cfg.url)
}

// Build parses the RedisURL from the builder's current state on every call, so
// reuse after changing the URL must not reuse parsed options from an earlier build.
func TestBuilderRedisURLReuse(t *testing.T) {
	builder := NewBuilder[string, int]().
		RedisURL("redis://localhost:6379").
		Fetch(func(context.Context, string) (int, error) { return 1, nil })

	first, err := builder.Build()
	require.NoError(t, err)
	defer first.Shutdown()
	require.True(t, first.owned)
	require.Equal(t, "localhost:6379", first.cfg.options.Addr)

	builder.RedisURL("http://localhost")
	_, err = builder.Build()
	require.ErrorContains(t, err, "invalid URL scheme")

	builder.RedisURL("redis://localhost:6380")
	second, err := builder.Build()
	require.NoError(t, err)
	defer second.Shutdown()
	require.True(t, second.owned)
	require.Equal(t, "localhost:6380", second.cfg.options.Addr)
	require.NotSame(t, first.client, second.client)
}

func TestJSONSerializer(t *testing.T) {
	type value struct {
		Name  string
		Count int
		Items []string
	}
	want := value{"hello", 2, []string{"a", "b"}}
	serializer := NewJSONSerializer[value]()
	data, err := serializer.Marshal(want)
	require.NoError(t, err)
	var got value
	require.NoError(t, serializer.Unmarshal(data, &got))
	require.Equal(t, want, got)
	require.Error(t, serializer.Unmarshal([]byte("{"), &got), "accepted invalid JSON")
	pointerSerializer := NewJSONSerializer[*value]()
	var ptr *value
	require.NoError(t, pointerSerializer.Unmarshal(data, &ptr))
	require.NotNil(t, ptr)
	require.Equal(t, want, *ptr)
}
