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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// cleanupTimeout bounds best-effort lock and permit releases and undecodable
// value invalidation. They use a fresh context because the caller's context may
// already be canceled.
const cleanupTimeout = 2 * time.Second

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
	waitMu                sync.Mutex
	waiters               map[string]*wakeup
	hits, misses, fetches atomic.Int64
	decodeErrors          atomic.Int64
}

// newCache starts workers only after Builder.Build has validated and copied settings.
// When the cache owns its client, Build has already parsed cfg.options.
func newCache[K ~string, V any](cfg config[K, V]) (*Cache[K, V], error) {
	owned := cfg.client == nil
	if owned {
		cfg.client = redis.NewClient(cfg.options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	base := cfg.prefix
	if base != "" {
		base += ":"
	}
	c := &Cache[K, V]{
		cfg:     cfg,
		client:  cfg.client,
		owned:   owned,
		base:    base,
		ctx:     ctx,
		cancel:  cancel,
		waiters: make(map[string]*wakeup),
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

// decode unmarshals a stored value. Serializer.Unmarshal takes []byte, so the
// conversion below copies the Redis string once per call; the copy is inherent
// to that interface and is not worth a second, string-accepting serializer API.
func (c *Cache[K, V]) decode(data string) (V, error) {
	var value V
	err := c.cfg.serializer.Unmarshal([]byte(data), &value)
	if err != nil {
		return value, fmt.Errorf("r2cache: decode: %w", err)
	}

	return value, nil
}

// decodeCached decodes a stored value and, on failure, deletes the exact bytes
// that failed to decode. The delete uses a fresh context so caller cancellation
// cannot skip the repair, and the compare keeps a concurrent replacement intact.
func (c *Cache[K, V]) decodeCached(valueKey, data string) (V, error) {
	value, err := c.decode(data)
	if err == nil {
		return value, nil
	}

	c.decodeErrors.Add(1)

	op, stop := context.WithTimeout(context.Background(), cleanupTimeout)
	defer stop()

	// The decode error is the caller-visible failure; a failed cleanup only
	// leaves the corrupt value for a later read to invalidate.
	_ = deleteValueScript.Run(op, c.client, []string{valueKey}, data).Err()

	return value, err
}

// GetOrFetch returns an unexpired cached value, or waits for ownership to fetch
// and store one. Concurrent readers across replicas share a successful fetch;
// a reader may retry acquisition after another owner's fetch fails. Failed
// fetches are not cached, and Redis errors do not bypass coordination to call
// the backend. A decoding error deletes the stored bytes when they still match
// the undecodable value, returns the error, and leaves a concurrent replacement
// untouched; the next read fetches and stores a new value.
//
// With auto-refresh enabled, an initial lookup that finds a stored value records
// this external access; a cache miss does not. A key fetched after a miss becomes
// hot on a later read that hits, and that hit schedules its first background
// refresh. Hotness is sampled at most once per sample window, so eligibility is
// best-effort within that window; background refreshes do not extend it. With
// refresh disabled, the initial lookup performs no writes.
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
		return c.decodeCached(keys[0], data)
	}

	if !errors.Is(err, redis.Nil) {
		return zero, fmt.Errorf("r2cache: read: %w", err)
	}

	c.misses.Add(1)
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		// Register for this key before attempting so a notification that arrives
		// during the attempt still causes an immediate retry.
		signal, release := c.waitFor(key)

		value, finished, err := c.attempt(ctx, key, keys, false)
		if err != nil || finished {
			release()
			return value, err
		}

		// Polling also handles lost Pub/Sub messages, owner crashes, and permit expiry.
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			release()
			return zero, ctx.Err()
		case <-signal:
			timer.Stop()
		case <-timer.C:
		}
		release()
	}
}

// attempt makes one acquisition attempt for key. keys must be c.keys(key); the
// polling path already built it for the initial read, so it is passed in rather
// than rebuilt on every poll.
//
// The token stays per attempt instead of being hoisted across polls: it
// identifies one acquisition. An acquire can reach Redis and be applied even
// when its reply is lost, so reusing a token on the next poll would make the
// ownership of an already-written lease, and its later release, ambiguous.
func (c *Cache[K, V]) attempt(ctx context.Context, key K, keys []string, refresh bool) (V, bool, error) {
	var zero V
	token := rand.Text()
	mode := 0
	if refresh {
		mode = 1
	}

	// discard releases a lock and permit that a failed acquire may have created
	// server-side. It runs on a fresh context because the caller's context may be
	// the cause of the failure. The release can race with an acquire still queued
	// server-side: it may run before that acquire creates its lock, or another
	// replica may already own the key. The token guard keeps a replacement lock
	// intact, and the lease remains the backstop for anything left behind.
	discard := func() {
		cleanup, stop := context.WithTimeout(context.Background(), cleanupTimeout)
		_ = releaseScript.Run(cleanup, c.client, []string{keys[1], keys[2], keys[5]}, token, string(key)).Err()
		stop()
	}

	result, err := acquireScript.Run(ctx, c.client, keys[:4], token, c.cfg.lease.Milliseconds(), c.cfg.max, mode, string(key)).Slice()
	if err != nil {
		discard()
		return zero, false, fmt.Errorf("r2cache: acquire: %w", err)
	}
	if len(result) == 0 {
		discard()
		return zero, false, errors.New("r2cache: empty acquire response")
	}

	status, ok := result[0].(int64)
	if !ok {
		discard()
		return zero, false, errors.New("r2cache: invalid acquire status")
	}

	switch status {
	case 0, 3:
		return zero, false, nil

	case 1:
		if len(result) < 2 {
			discard()
			return zero, false, errors.New("r2cache: missing cached value")
		}

		data, ok := result[1].(string)
		if !ok {
			discard()
			return zero, false, errors.New("r2cache: invalid cached value")
		}

		v, e := c.decodeCached(keys[0], data)
		return v, true, e

	case 2:
		value, err := c.fetch(ctx, key, keys, token)
		return value, true, err

	default:
		discard()
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
		cleanup, stop := context.WithTimeout(context.Background(), cleanupTimeout)
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

	var hot, interval int64
	if c.cfg.refresh != nil {
		hot = c.cfg.refresh.HotKeyLifetime.Milliseconds()
		interval = c.cfg.refresh.UpdateInterval.Milliseconds()
	}

	committed, err := commitScript.Run(fetchCtx, c.client, []string{keys[0], keys[1], keys[2], keys[4], keys[5], keys[3]}, token, data, c.cfg.ttl.Milliseconds(), c.cfg.max, string(key), interval, hot).Int()
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

// HotKeys returns keys whose last sampled external read lies within
// HotKeyLifetime, in oldest-sampled-access-first order. Hotness is recorded at
// most once per sample window of min(UpdateInterval, HotKeyLifetime/2), at least
// 1ms. Sampling never extends eligibility beyond an exact last-read window: it
// can expire up to one window earlier, and the reported access order can lag by
// up to one window. Within that bound, eligibility is best-effort, not an exact
// read trace. A nil result means the answer is unavailable: refresh is disabled,
// the cache is closed, the context is canceled, or Redis failed. A non-nil empty
// slice means the query succeeded and no keys are currently eligible. The
// signature has no error return, so compare against nil to tell the two cases
// apart; len(result) == 0 conflates them.
// The result is unbounded: Redis materializes the entire eligible set and this
// call allocates O(n) keys, so its cost grows with the namespace's hot-key
// population. Prefer HotKeysLimit when the caller needs a bounded result.
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

	// make returns a non-nil slice even for zero length, so a successful query
	// never returns nil; nil is reserved for the unavailable cases above.
	result := make([]K, len(keys))
	for i, key := range keys {
		result[i] = K(key)
	}

	return result
}

// HotKeysLimit returns up to limit eligible hot keys in oldest-sampled-access-first
// order and reports whether the full result was truncated. Eligibility and order
// follow the sampled-hotness semantics of HotKeys. It returns nil when refresh is
// disabled, the cache is closed, the context is canceled, Redis fails, or
// limit <= 0. A successful empty result is a non-nil empty slice, so nil is
// unambiguously the unavailable sentinel.
//
// Redis is asked for at most limit+1 keys, so the reply and this call's allocation
// are bounded by limit; truncated reports whether more eligible keys exist beyond
// the returned page. Like HotKeys, the result covers the shared namespace, and
// calling it neither fetches values nor extends hot-key eligibility.
func (c *Cache[K, V]) HotKeysLimit(ctx context.Context, limit int) ([]K, bool) {
	if limit <= 0 {
		return nil, false
	}

	ctx, done, err := c.begin(ctx)
	if err != nil {
		return nil, false
	}

	defer done()

	if c.cfg.refresh == nil {
		return nil, false
	}

	// Request one extra key to tell a full page from a truncated one. At MaxInt
	// there is no representable limit+1, but MaxInt itself is already a safe cap:
	// a reply cannot contain more keys than an int can count.
	request := limit
	if limit < math.MaxInt {
		request++
	}

	keys, err := hotKeysScript.Run(ctx, c.client, []string{c.base + "hot"}, request).StringSlice()
	if err != nil {
		return nil, false
	}

	truncated := len(keys) > limit
	if truncated {
		keys = keys[:limit]
	}

	result := make([]K, len(keys))
	for i, key := range keys {
		result[i] = K(key)
	}

	return result, truncated
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
