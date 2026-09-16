// Package r2cache implements Redis-backed read-through caching with distributed
// fetch coordination and optional background refresh of recently accessed keys.
//
// Construct a Cache with NewBuilder, supplying Fetch and either RedisURL or
// UseRedisClient. Instances sharing a namespace coordinate through Redis and must
// agree on configuration and value encoding. Renewable leases prevent duplicate
// backend work during normal operation; recovery may overlap a stalled callback.
// Call Cache.Shutdown when finished to stop workers and release owned resources.
package r2cache

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrClosed is returned by GetOrFetch when Shutdown has already begun. Operations
// that were active when shutdown started are canceled instead. HotKeys reports a
// closed cache through its nil result because its signature has no error return.
var ErrClosed = errors.New("r2cache: cache is shut down")

// ErrLeaseLost indicates that a fetch could not retain or verify its ownership,
// for example because its lease expired or renewal failed. The callback context
// is canceled when renewal detects this condition, and a result from an expired
// owner is rejected at commit. Another replica may already be fetching the key.
// Callers can recognize this error with errors.Is and retry within their deadline.
var ErrLeaseLost = errors.New("r2cache: fetch lease lost")

// Cache stores values of type V under string-like keys of type K in Redis and
// coordinates backend fetches with other instances in its namespace. It is safe
// for concurrent use. Construct one with Builder.Build; its zero value is not usable.
// Do not copy a Cache after construction, and call Shutdown to release its workers.
type Cache[K ~string, V any] struct {
	cfg                   config[K, V]
	client                redis.UniversalClient
	owned                 bool
	base                  string
	ctx                   context.Context
	cancel                context.CancelFunc
	mu                    sync.Mutex
	closed                bool
	wg                    sync.WaitGroup
	once                  sync.Once
	signalMu              sync.Mutex
	signal                chan struct{}
	hits, misses, fetches atomic.Int64
}

// newCache starts workers only after Builder.Build has validated and copied settings.
func newCache[K ~string, V any](cfg config[K, V]) (*Cache[K, V], error) {
	owned := cfg.client == nil
	if owned {
		options, err := redis.ParseURL(cfg.url)
		if err != nil {
			return nil, err
		}

		options.ContextTimeoutEnabled = true
		cfg.client = redis.NewClient(options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	base := cfg.prefix
	if base != "" {
		base += ":"
	}
	c := &Cache[K, V]{
		cfg:    cfg,
		client: cfg.client,
		owned:  owned,
		base:   base,
		ctx:    ctx,
		cancel: cancel,
		signal: make(chan struct{}),
	}

	c.wg.Add(1)
	go c.notifications()

	if cfg.refresh != nil {
		c.wg.Add(1)
		go c.refresh()
	}

	if cfg.metrics != nil {
		c.wg.Add(1)
		go c.metrics()
	}

	return c, nil
}

func (c *Cache[K, V]) keys(key K) []string {
	id := base64.RawURLEncoding.EncodeToString([]byte(key))
	return []string{
		c.base + "value:" + id,
		c.base + "lock:" + id,
		c.base + "permits",
		c.base + "hot",
		c.base + "due",
		c.base + "events",
	}
}

func (c *Cache[K, V]) begin(ctx context.Context) (context.Context, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, ErrClosed
	}

	c.wg.Add(1)
	op, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	stopFunc := func() {
		stop()
		cancel()
		c.wg.Done()
	}

	return op, stopFunc, nil
}

func (c *Cache[K, V]) decode(data string) (V, error) {
	var value V
	err := c.cfg.serializer.Unmarshal([]byte(data), &value)
	if err != nil {
		return value, fmt.Errorf("r2cache: decode: %w", err)
	}

	return value, nil
}

// GetOrFetch returns an unexpired cached value, or waits for ownership to fetch
// and store one. Concurrent readers across replicas share a successful fetch;
// a reader may retry acquisition after another owner's fetch fails. Failed
// fetches are not cached, and Redis errors do not bypass coordination to call
// the backend. A decoding error is returned without replacing the stored value.
//
// With auto-refresh enabled, the initial lookup records this external access,
// even on a cache miss; background refreshes do not extend that access window.
// The context bounds waiting and is passed to a callback owned by this call.
// Canceling a waiter does not cancel another reader's fetch. Fetchers must honor
// cancellation: the owning call waits for its callback to return before completing.
// Use after Shutdown returns ErrClosed; detected ownership loss returns ErrLeaseLost.
func (c *Cache[K, V]) GetOrFetch(ctx context.Context, key K) (V, error) {
	var zero V
	ctx, done, err := c.begin(ctx)
	if err != nil {
		return zero, err
	}

	defer done()

	keys := c.keys(key)

	var hot, interval int64
	if p := c.cfg.refresh; p != nil {
		hot = p.HotKeyLifetime.Milliseconds()
		interval = p.UpdateInterval.Milliseconds()
	}

	data, err := readScript.Run(ctx, c.client, []string{keys[0], keys[3], keys[4]}, string(key), hot, interval).Text()
	if err == nil {
		c.hits.Add(1)
		return c.decode(data)
	}

	if !errors.Is(err, redis.Nil) {
		return zero, fmt.Errorf("r2cache: read: %w", err)
	}

	c.misses.Add(1)
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		c.signalMu.Lock()
		signal := c.signal
		c.signalMu.Unlock()

		value, finished, err := c.attempt(ctx, key, false)
		if err != nil || finished {
			return value, err
		}

		// Polling also handles lost Pub/Sub messages, owner crashes, and permit expiry.
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-signal:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (c *Cache[K, V]) attempt(ctx context.Context, key K, refresh bool) (V, bool, error) {
	var zero V
	keys := c.keys(key)
	token := rand.Text()
	mode := 0
	if refresh {
		mode = 1
	}

	result, err := acquireScript.Run(ctx, c.client, keys[:4], token, c.cfg.lease.Milliseconds(), c.cfg.max, mode, string(key)).Slice()
	if err != nil {
		return zero, false, fmt.Errorf("r2cache: acquire: %w", err)
	}
	if len(result) == 0 {
		return zero, false, errors.New("r2cache: empty acquire response")
	}

	status, ok := result[0].(int64)
	if !ok {
		return zero, false, errors.New("r2cache: invalid acquire status")
	}

	switch status {
	case 0, 3:
		return zero, false, nil

	case 1:
		if len(result) < 2 {
			return zero, false, errors.New("r2cache: missing cached value")
		}

		data, ok := result[1].(string)
		if !ok {
			return zero, false, errors.New("r2cache: invalid cached value")
		}

		v, e := c.decode(data)
		return v, true, e

	case 2:
		value, err := c.fetch(ctx, key, keys, token)
		return value, true, err

	default:
		return zero, false, fmt.Errorf("r2cache: unknown acquire status %d", status)
	}
}

func (c *Cache[K, V]) fetch(ctx context.Context, key K, keys []string, token string) (V, error) {
	var zero V
	fetchCtx, cancel := context.WithCancelCause(ctx)
	renewed := make(chan struct{})

	go func() { defer close(renewed); c.renew(fetchCtx, cancel, keys, token) }()

	defer func() {
		cancel(context.Canceled)
		<-renewed
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = releaseScript.Run(cleanup, c.client, []string{keys[1], keys[2], keys[5]}, token, string(key)).Err()
	}()

	c.fetches.Add(1)

	value, err := c.cfg.fetch(fetchCtx, key)
	if cause := context.Cause(fetchCtx); cause != nil {
		return zero, cause
	}
	if err != nil {
		return zero, err
	}

	data, err := c.cfg.serializer.Marshal(value)
	if err != nil {
		return zero, fmt.Errorf("r2cache: encode: %w", err)
	}

	var interval int64
	if c.cfg.refresh != nil {
		interval = c.cfg.refresh.UpdateInterval.Milliseconds()
	}

	committed, err := commitScript.Run(fetchCtx, c.client, keys, token, data, c.cfg.ttl.Milliseconds(), c.cfg.max, string(key), interval).Int()
	if err != nil {
		return zero, fmt.Errorf("r2cache: commit: %w", err)
	}

	if committed != 1 {
		return zero, ErrLeaseLost
	}

	return value, nil
}

func (c *Cache[K, V]) renew(ctx context.Context, cancel context.CancelCauseFunc, keys []string, token string) {
	ticker := time.NewTicker(c.cfg.lease / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		op, stop := context.WithTimeout(ctx, c.cfg.lease/3)
		ok, err := renewScript.Run(op, c.client, []string{keys[1], keys[2]}, token, c.cfg.lease.Milliseconds(), c.cfg.max).Int()

		stop()

		if err != nil || ok != 1 {
			cancel(ErrLeaseLost)
			return
		}
	}
}

// HotKeys returns keys whose last external read lies within HotKeyLifetime, in
// oldest-access-first order. It returns nil when refresh is disabled, the cache
// is closed, the context is canceled, or Redis fails. Errors cannot be distinguished
// from an empty result through this slice-only API.
// The result covers the shared namespace, not just this instance's reads. Calling
// HotKeys neither fetches values nor extends their hot-key eligibility.
func (c *Cache[K, V]) HotKeys(ctx context.Context) []K {
	ctx, done, err := c.begin(ctx)
	if err != nil {
		return nil
	}

	defer done()

	if c.cfg.refresh == nil {
		return nil
	}

	keys, err := hotKeysScript.Run(ctx, c.client, []string{c.base + "hot"}).StringSlice()
	if err != nil {
		return nil
	}

	result := make([]K, len(keys))
	for i, key := range keys {
		result[i] = K(key)
	}

	return result
}

// Shutdown cancels work, waits for callbacks and active operations, and closes an
// internally created Redis client. It is idempotent. Fetchers must honor their
// contexts. Do not call Shutdown from a fetcher or metrics callback.
// New reads are rejected once shutdown begins. Caller-owned Redis clients and
// shared cached values are left intact; other replicas may continue using them.
func (c *Cache[K, V]) Shutdown() {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		c.wg.Wait()

		if c.owned {
			_ = c.client.Close()
		}
	})
}
