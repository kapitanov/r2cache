package r2cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultTTL is the lifetime of a stored value when Builder.TTL is not called.
// The lifetime starts at a successful write; cache hits do not extend it.
const DefaultTTL = 5 * time.Minute

// DefaultLeaseDuration is the recovery window for an abandoned fetch when
// Builder.LeaseDuration is not called. Active fetches renew their leases before expiry.
const DefaultLeaseDuration = 30 * time.Second

type config[K ~string, V any] struct {
	client     redis.UniversalClient
	url        string
	prefix     string
	fetch      func(context.Context, K) (V, error)
	serializer Serializer[V]
	ttl, lease time.Duration
	max        int
	refresh    *AutoRefreshPolicy
	metrics    MetricsReceiver
}

// Builder collects cache settings before starting any workers or creating clients.
// Construct one with NewBuilder; its zero value is not usable. Setters mutate the
// builder and return it for chaining. The last setter for a feature wins, and
// Build validates the final settings so an invalid value can be corrected.
// Builders are not safe for concurrent use. A builder may build multiple caches;
// subsequent changes do not alter caches it has already built.
type Builder[K ~string, V any] struct {
	cfg      config[K, V]
	limitSet bool
}

// NewBuilder returns a builder with compact JSON, DefaultTTL,
// DefaultLeaseDuration, an empty prefix, no refresh or metrics, and no global
// concurrency limit. Set Fetch and either RedisURL or UseRedisClient before Build.
func NewBuilder[K ~string, V any]() *Builder[K, V] {
	return &Builder[K, V]{cfg: config[K, V]{
		ttl:        DefaultTTL,
		lease:      DefaultLeaseDuration,
		serializer: NewJSONSerializer[V](),
	}}
}

// Build validates the current settings and starts an independent cache. It does
// not check Redis connectivity. Validation failures allocate no client or worker
// and leave the builder available for correction and reuse.
//
// Each build snapshots the settings and refresh policy. Fetch callbacks,
// serializers, metrics receivers, and an injected Redis client remain shared by
// reference and must support concurrent use. A RedisURL creates a separately
// owned client for each cache. Call Shutdown on every successfully built cache.
func (b *Builder[K, V]) Build() (*Cache[K, V], error) {
	if b == nil {
		return nil, errors.New("r2cache: nil builder")
	}
	cfg := b.cfg
	if cfg.fetch == nil {
		return nil, errors.New("r2cache: fetcher is required")
	}
	if cfg.client == nil {
		if cfg.url == "" {
			return nil, errors.New("r2cache: Redis connection is required")
		}
		if _, err := redis.ParseURL(cfg.url); err != nil {
			return nil, err
		}
	}
	if cfg.serializer == nil {
		return nil, errors.New("r2cache: nil serializer")
	}
	if cfg.ttl < time.Millisecond {
		return nil, errors.New("r2cache: TTL must be at least 1ms")
	}
	if cfg.lease < 100*time.Millisecond {
		return nil, errors.New("r2cache: lease must be at least 100ms")
	}
	if b.limitSet && cfg.max <= 0 {
		return nil, errors.New("r2cache: concurrency limit must be positive")
	}
	if cfg.refresh != nil {
		policy := *cfg.refresh
		if policy.HotKeyLifetime < time.Millisecond || policy.UpdateInterval < time.Millisecond {
			return nil, errors.New("r2cache: refresh durations must be at least 1ms")
		}
		cfg.refresh = &policy
	}
	return newCache(cfg)
}

// RedisURL selects a standalone Redis or Valkey server using a redis:// or
// rediss:// URL, including any credentials and database number in that URL.
// Build validates the URL but does not check connectivity. The cache owns this
// client and closes it on Shutdown. This method replaces any earlier
// UseRedisClient setting; use that method for Sentinel or Cluster connections.
func (b *Builder[K, V]) RedisURL(url string) *Builder[K, V] {
	b.cfg.url = url
	b.cfg.client = nil
	return b
}

// UseRedisClient selects a caller-owned standalone, Sentinel, or Cluster client,
// replacing any earlier RedisURL setting. A nil client is rejected by Build.
// Configure the client to read from primaries and honor context deadlines.
// Shutdown leaves the client open; close it after all caches using it shut down.
func (b *Builder[K, V]) UseRedisClient(client redis.UniversalClient) *Builder[K, V] {
	b.cfg.client = client
	b.cfg.url = ""
	return b
}

// KeyPrefix prepends prefix verbatim followed by ":" to every internal Redis
// key. For example, "my-cache" produces "my-cache:value:<encoded-key>" and
// "my-cache:hot". The default empty prefix adds neither a prefix nor a separator.
// Prefixes are not hashed, escaped, or trimmed; "my-cache:" produces "my-cache::".
// For Redis Cluster, include a non-empty Redis hash tag in the prefix yourself
// (for example, "my-cache:{products}") so multi-key scripts use a single slot.
// All replicas in a namespace must agree on configuration, key/value types,
// serialization, and fetch semantics. Use separate namespaces for unrelated data.
func (b *Builder[K, V]) KeyPrefix(prefix string) *Builder[K, V] {
	b.cfg.prefix = prefix
	return b
}

// Fetch supplies the required backend callback for cache misses and enabled
// background refreshes. Build rejects a nil callback. Only successful results are
// stored; backend errors are returned to the caller that performed the fetch.
//
// The callback must support concurrent calls for different keys and honor its
// context, which is canceled on caller cancellation, shutdown, or lease loss.
// Background calls have no automatic fetch deadline; apply backend timeouts as
// needed. Recovery can overlap calls for the same key, so fetching must be safe
// to repeat. Do not call Shutdown from the callback: it would wait for itself.
func (b *Builder[K, V]) Fetch(fetcher func(context.Context, K) (V, error)) *Builder[K, V] {
	b.cfg.fetch = fetcher
	return b
}

// LimitConcurrentFetches limits backend calls globally within the namespace.
// Foreground fetches and background refreshes share the limit, and max must be
// positive. Unless this method is called, different keys may be fetched without a limit;
// per-key coordination still applies. Recovery from expired leases can temporarily
// exceed the limit when an old callback continues running after losing ownership.
func (b *Builder[K, V]) LimitConcurrentFetches(max int) *Builder[K, V] {
	b.cfg.max = max
	b.limitSet = true
	return b
}

// TTL sets how long a successfully stored value remains readable, starting
// when the write completes. Cache hits do not extend the TTL; successful
// refreshes replace the value and restart it. TTL defaults to DefaultTTL and
// must be at least one millisecond; fractional milliseconds are truncated.
func (b *Builder[K, V]) TTL(ttl time.Duration) *Builder[K, V] {
	b.cfg.ttl = ttl
	return b
}

// LeaseDuration sets the recovery window for abandoned fetches. Leases renew
// every third of this duration while a callback runs. The duration defaults to
// DefaultLeaseDuration and must be at least 100 milliseconds; Redis lease expiry
// uses whole milliseconds. Shorter leases recover faster but are more sensitive
// to pauses and network delays. Recovery can overlap a stalled backend call;
// this duration is not a backend request timeout.
func (b *Builder[K, V]) LeaseDuration(duration time.Duration) *Builder[K, V] {
	b.cfg.lease = duration
	return b
}

// AutoRefreshPolicy controls which recently accessed keys are refreshed and the
// desired refresh cadence. Both durations must be at least one millisecond and
// are truncated to whole milliseconds. Replicas in a namespace must use the same
// policy. Refresh deadlines are best-effort under load or backend failures.
type AutoRefreshPolicy struct {
	// HotKeyLifetime is the time after the last external GetOrFetch read during
	// which a key remains eligible for refresh. Background refreshes and HotKeys
	// queries do not extend this window.
	HotKeyLifetime time.Duration

	// UpdateInterval is the target delay from a successful fetch to its next
	// refresh. Failed or busy refresh attempts are deferred for another interval.
	// Starts are spaced across hot keys and replicas to reduce bursts. Choose an
	// interval below the TTL, with room for backend latency, to keep values warm.
	UpdateInterval time.Duration
}

// DisableAutoRefresh disables hot-key tracking and background refreshes for the
// cache being constructed, overriding any earlier EnableAutoRefresh call.
// This is the default. It does not delete stored values or shared hot-key indexes.
func (b *Builder[K, V]) DisableAutoRefresh() *Builder[K, V] {
	b.cfg.refresh = nil
	return b
}

// EnableAutoRefresh enables hot-key tracking for external reads and starts a
// background worker using policy. Refreshes use the same per-key leases and
// global concurrency permits as foreground fetches. Invalid policy durations
// cause Build to fail. Refresh errors leave an existing value until its TTL expires
// and are retried on a later interval; they are not delivered to foreground readers.
func (b *Builder[K, V]) EnableAutoRefresh(policy AutoRefreshPolicy) *Builder[K, V] {
	b.cfg.refresh = &policy
	return b
}

// Serializer converts values to and from their Redis representation. Methods
// must be safe for concurrent use, and all replicas sharing a namespace must use
// compatible encodings. Serialization errors are returned by GetOrFetch.
type Serializer[V any] interface {
	// Marshal encodes a fetched value for storage. An error prevents that value
	// from being cached. The returned bytes must remain valid and unchanged until
	// the cache finishes writing them to Redis.
	Marshal(V) ([]byte, error)

	// Unmarshal decodes a stored value into the non-nil destination supplied by
	// the cache. An error is returned to the reader without fetching a replacement;
	// the stored bytes remain until expiry or external removal.
	Unmarshal([]byte, *V) error
}

type jsonSerializer[V any] struct{}

// Marshal encodes v as compact JSON, preserving encoding/json's error behavior.
func (jsonSerializer[V]) Marshal(v V) ([]byte, error) { return json.Marshal(v) }

// Unmarshal decodes JSON into v using encoding/json's decoding rules.
func (jsonSerializer[V]) Unmarshal(data []byte, v *V) error { return json.Unmarshal(data, v) }

// NewJSONSerializer returns the stateless, concurrency-safe serializer used by
// default. It delegates to encoding/json, including custom JSON marshalers and
// unmarshalers implemented by V, and writes compact JSON without a trailing newline.
func NewJSONSerializer[V any]() Serializer[V] { return jsonSerializer[V]{} }

// UseSerializer replaces the default JSON encoding for both reads and writes.
// Build rejects a nil serializer. The implementation must support concurrent calls;
// changing an existing namespace's encoding requires compatible decoding or a
// new namespace to avoid errors when reading previously cached values.
func (b *Builder[K, V]) UseSerializer(serializer Serializer[V]) *Builder[K, V] {
	b.cfg.serializer = serializer
	return b
}

// MetricValues contains cumulative counters for one Cache instance and a shared
// hot-key gauge. Counters start at zero for each new instance and are not reset
// after delivery. Fields are sampled separately, not as an atomic snapshot.
type MetricValues struct {
	// HitCount counts external reads whose initial lookup found a stored value,
	// including values that subsequently failed to decode.
	HitCount int64

	// MissCount counts external reads whose initial lookup found no value,
	// including readers that waited for another owner instead of fetching.
	// Redis lookup errors are counted as neither hits nor misses.
	MissCount int64

	// BackendFetchCount counts local callback invocations, including background
	// refreshes and calls that fail or are canceled.
	BackendFetchCount int64

	// HotKeyCount is the current number of eligible hot keys across the namespace.
	// It is zero when refresh is disabled and -1 if Redis could not be queried.
	HotKeyCount int64
}

// MetricsReceiver consumes a metrics sample on a background goroutine, scheduled
// once per second. Calls do not overlap within one cache; slow callbacks or Redis
// queries can delay delivery. A receiver shared by multiple caches may be called
// concurrently. Return promptly and do not call Shutdown, which waits for the
// receiver to return.
type MetricsReceiver func(MetricValues)

// MetricsReceiver registers the callback for periodic metrics delivery.
// No callbacks run by default, and a nil receiver disables delivery. Shutdown
// stops delivery and waits for an active callback; there is no final flush.
func (b *Builder[K, V]) MetricsReceiver(receiver MetricsReceiver) *Builder[K, V] {
	b.cfg.metrics = receiver
	return b
}

// UseJSONSerializer restores the default encoding/json serializer, replacing any
// earlier UseSerializer setting. JSON encoding and decoding follow V's custom
// JSON methods when present; unsupported values fail when they are serialized.
func (b *Builder[K, V]) UseJSONSerializer() *Builder[K, V] {
	b.cfg.serializer = NewJSONSerializer[V]()
	return b
}
