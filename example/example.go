// Command example demonstrates a complete R2Cache lifecycle against a real Redis
// or Valkey server. With Docker running, start a disposable local Redis server:
//
//	docker run --detach --rm --name r2cache-example -p 127.0.0.1:6379:6379 redis:7.4.2-alpine
//
// Wait until Redis responds with PONG, then run the example from the repository root:
//
//	docker exec r2cache-example redis-cli ping
//
//	go run ./example -redis-url redis://localhost:6379/0
//
// To try Valkey instead, replace the image above with valkey/valkey:8.0.2-alpine
// and use valkey-cli instead of redis-cli. If host port 6379 is occupied, publish
// 127.0.0.1:6380:6379 and use redis://localhost:6380/0 for the example.
//
// When finished, stop the container; --rm removes it and its temporary data:
//
//	docker stop r2cache-example
//
// The two cache instances represent application replicas. They have independent
// Redis connections and share a namespace and a simulated, concurrency-safe backend.
// Each run uses a fresh namespace unless -prefix is supplied; no Redis data is flushed.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/kapitanov/r2cache"
)

const (
	valueTTL        = 15 * time.Second
	refreshInterval = 2 * time.Second
	hotLifetime     = 8 * time.Second
	fetchLimit      = 2
)

var errNotFound = errors.New("product not found")

type productID string

type product struct {
	// ID identifies the product in both the backend and cache.
	ID productID `json:"id"`
	// Name is the product's display name.
	Name string `json:"name"`
	// PriceCents keeps the example's price in integer minor units.
	PriceCents int `json:"price_cents"`
	// Revision increases on each successful backend fetch, making refresh visible.
	Revision int `json:"revision"`
	// FetchedAt records when the backend produced this snapshot.
	FetchedAt time.Time `json:"fetched_at"`
}

type settings struct {
	redisURL string
	prefix   string
	readers  int
	latency  time.Duration
}

// backend simulates a small catalog behind a slow service. Its counters are shared
// by both replicas in this process, so we can observe their combined activity.
type backend struct {
	mu                  sync.Mutex
	catalog             map[productID]product
	calls, active, peak int
	latency             time.Duration
}

func newBackend(latency time.Duration) *backend {
	b := &backend{catalog: make(map[productID]product), latency: latency}
	for i := 1; i <= 10; i++ {
		id := productID(fmt.Sprintf("my-product-%d", i))
		b.catalog[id] = product{ID: id, Name: fmt.Sprintf("Example product %d", i), PriceCents: 1000 + i*250}
	}
	return b
}

func (b *backend) fetch(ctx context.Context, id productID) (product, error) {
	// Background refresh contexts have no request deadline. A real backend should
	// also set its own timeout instead of relying solely on foreground callers.
	ctx, cancel := context.WithTimeout(ctx, b.latency+2*time.Second)
	defer cancel()

	b.mu.Lock()
	b.calls++
	b.active++
	b.peak = max(b.peak, b.active)
	call, active := b.calls, b.active
	b.mu.Unlock()
	slog.Info("backend fetch started", "key", id, "call", call, "active", active)

	defer func() {
		b.mu.Lock()
		b.active--
		b.mu.Unlock()
	}()

	if err := pause(ctx, b.latency); err != nil {
		return product{}, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.catalog[id]
	if !ok {
		return product{}, fmt.Errorf("%w: %s", errNotFound, id)
	}

	p.Revision++
	p.FetchedAt = time.Now().UTC()
	b.catalog[id] = p
	return p, nil
}

func main() {
	cfg := settings{}
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}

	flag.StringVar(&cfg.redisURL, "redis-url", url, "Redis URL, defaults to REDIS_URL or redis://localhost:6379/0")
	flag.StringVar(&cfg.prefix, "prefix", "", "unused cache namespace, defaults to a unique namespace for this run")
	flag.IntVar(&cfg.readers, "readers", 12, "number of simultaneous reads in the singleflight demonstration")
	flag.DurationVar(&cfg.latency, "backend-latency", 150*time.Millisecond, "simulated backend latency, between 30ms and 1s")
	flag.Parse()

	if cfg.readers < 2 || cfg.latency < 30*time.Millisecond || cfg.latency > time.Second {
		slog.Error("invalid flags: readers must be at least 2 and backend-latency must be between 30ms and 1s")
		os.Exit(1)
	}

	if cfg.prefix == "" {
		cfg.prefix = "r2cache-example-" + rand.Text()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			slog.Info("example interrupted; caches shut down")
			return
		}

		slog.Error("example failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg settings) error {
	// Bound the entire demonstration as well as individual reads.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	b := newBackend(cfg.latency)
	caches := make([]*r2cache.Cache[productID, product], 0, 2)

	defer func() {
		// Shutdown cancels background work and waits for callbacks to finish.
		// Clients created by RedisURL are closed by their cache.
		for _, cache := range caches {
			cache.Shutdown()
		}

		slog.Info("all caches shut down")
	}()

	for replica := 1; replica <= 2; replica++ {
		cache, err := r2cache.NewBuilder[productID, product]().
			RedisURL(cfg.redisURL).
			KeyPrefix(cfg.prefix).
			Fetch(b.fetch).
			TTL(valueTTL).
			LimitConcurrentFetches(fetchLimit).
			EnableAutoRefresh(r2cache.AutoRefreshPolicy{
				HotKeyLifetime: hotLifetime,
				UpdateInterval: refreshInterval,
			}).
			// JSON is the default; this method shows how to select a serializer.
			UseSerializer(r2cache.NewJSONSerializer[product]()).
			MetricsReceiver(func(m r2cache.MetricValues) {
				// A receiver may run concurrently across cache instances. slog is safe
				// for concurrent use. Never call Shutdown from this callback.
				slog.Info("metrics", "replica", replica, "hits", m.HitCount, "misses", m.MissCount,
					"fetches", m.BackendFetchCount, "hot_keys", m.HotKeyCount)
			}).
			Build()
		if err != nil {
			return fmt.Errorf("create replica %d: %w", replica, err)
		}

		caches = append(caches, cache)
	}

	slog.Info("example started", "namespace", cfg.prefix, "replicas", len(caches), "fetch_limit", fetchLimit)

	first, err := read(ctx, caches[0], "my-product-1")
	if err != nil {
		return fmt.Errorf("initial read (check that Redis is running): %w", err)
	}

	hit, err := read(ctx, caches[1], first.ID)
	if err != nil {
		return err
	}

	slog.Info("cross-replica cache hit", "key", hit.ID, "first_revision", first.Revision, "cached_revision", hit.Revision)

	keys := make([]productID, cfg.readers)
	for i := range keys {
		keys[i] = "my-product-2"
	}

	slog.Info("singleflight: concurrent reads of one key", "readers", len(keys))
	if err := readConcurrently(ctx, caches, keys); err != nil {
		return err
	}

	slog.Info("global concurrency: concurrent reads of different keys", "limit", fetchLimit)
	keys = []productID{"my-product-3", "my-product-4", "my-product-5", "my-product-6"}

	if err := readConcurrently(ctx, caches, keys); err != nil {
		return err
	}

	// Wait until Redis serves a newer backend snapshot. Reading this key keeps it
	// hot; a 15-second TTL means its earlier refresh should happen before expiry.
	slog.Info("waiting for automatic refresh", "key", first.ID, "interval", refreshInterval)
	for {
		if err := pause(ctx, 250*time.Millisecond); err != nil {
			return err
		}

		refreshed, err := read(ctx, caches[1], first.ID)
		if err != nil {
			return err
		}

		if refreshed.Revision > first.Revision {
			slog.Info("refreshed value received", "key", refreshed.ID, "revision", refreshed.Revision, "fetched_at", refreshed.FetchedAt)
			break
		}
	}

	// Missing products return a recognizable backend error. Both attempts reach
	// the backend because error results are not cached.
	for attempt := 1; attempt <= 2; attempt++ {
		_, err := read(ctx, caches[0], "my-missing-product")
		if err == nil {
			return errors.New("expected product-not-found error, but the read succeeded")
		}

		if !errors.Is(err, errNotFound) {
			return fmt.Errorf("missing-product demonstration: %w", err)
		}

		slog.Info("backend error returned without caching", "attempt", attempt, "error", err)
	}

	shortCtx, stop := context.WithTimeout(ctx, cfg.latency/3)
	_, err = caches[0].GetOrFetch(shortCtx, "my-product-10")

	stop()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err == nil {
		return errors.New("expected a read deadline, but the read succeeded")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("deadline demonstration: %w", err)
	}

	slog.Info("short read canceled", "error", err)
	slog.Info("shared hot keys", "keys", caches[0].HotKeys(ctx))

	b.mu.Lock()
	calls, peak := b.calls, b.peak
	b.mu.Unlock()

	slog.Info("example complete", "backend_calls", calls, "peak_backend_concurrency", peak, "configured_limit", fetchLimit)
	return nil
}

func read(ctx context.Context, cache *r2cache.Cache[productID, product], key productID) (product, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	value, err := cache.GetOrFetch(ctx, key)
	if err != nil {
		return product{}, fmt.Errorf("read %s: %w", key, err)
	}

	return value, nil
}

func readConcurrently(ctx context.Context, caches []*r2cache.Cache[productID, product], keys []productID) error {
	type result struct {
		value product
		err   error
	}

	start := make(chan struct{})
	results := make(chan result, len(keys))

	for i, key := range keys {
		go func() {
			<-start
			value, err := read(ctx, caches[i%len(caches)], key)
			results <- result{value: value, err: err}
		}()
	}

	close(start)
	var firstErr error

	// Drain every result before returning, including on error, so no reader is
	// left running while the example shuts its caches down.
	for range keys {
		r := <-results
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}

		slog.Info("concurrent read completed", "key", r.value.ID, "revision", r.value.Revision)
	}

	return firstErr
}

func pause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
