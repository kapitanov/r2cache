# Implementation and validation plan

Approved contracts:

- Keep `HotKeys(context.Context) []K`; return nil when unavailable.
- `Builder.Build` returns `*Cache[K,V]`; serializers decode into `*V`.
- Renewable leases coordinate fetches. Recovery may overlap a stalled owner;
  expired owners cannot commit over newer results.
- Use go-redis universal clients; one Redis Cluster hash slot per namespace.
- Preserve key prefixes verbatim and add a colon separator for non-empty prefixes.
  Cluster users supply their own Redis hash tag; no prefix hashing is performed.
- Use testcontainers-go for real Redis integration tests, locally and in GitHub.

Implementation sequence:

1. Configuration, serialization, lifecycle, and namespace keys.
2. Atomic reads/acquisition, global permits, lease renewal, conditional commit,
   notification, and polling recovery.
3. Shared hot-key tracking, globally paced refresh, and metrics.
4. Unit tests and real-server integration tests, race detection, documentation,
   and GitHub Actions.

Validation covers cache hits and TTL, named keys and arbitrary values, serializers,
configuration rejection, builder correction and independent snapshots, cross-instance singleflight, global concurrency across
keys, renewal during slow fetches, recovery and expired-owner rejection, caller
cancellation, fetch failures, Redis client failures, hot-key aging, refresh coordination,
metrics, and shutdown/client ownership. Integration tests must fail rather than
silently skip when Docker is unavailable. `-short` explicitly runs only unit tests.

Constraints: leases are best-effort under failover/partitions; all replicas sharing
one namespace must agree on settings and fetch semantics. Refresh intervals are
best-effort under slow backends or saturated concurrency. Callback cancellation is
cooperative. No stale values or backend errors are cached.

## Completed validation

- `go test -short ./...`: passed without starting containers.
- `make check`: build, vet, and race-enabled Redis integration tests passed.
- `R2CACHE_TEST_IMAGE=valkey/valkey:8.0.2-alpine go test -race -count=1
  -coverprofile=/private/tmp/r2cache-coverage.out -timeout=5m ./...`: passed;
  Go statement coverage was 95.0% (this does not measure Lua branch coverage).
- Actual Redis Cluster script execution and Sentinel discovery passed. The
  fixtures use a single cluster node and a single Sentinel/master pair;
  resharding, failover, and partition recovery are not simulated.
- Formatting and `git diff --check`: clean.
- GitHub Actions is configured to run the same checks for Redis and Valkey;
  it has not been executed on GitHub from this local workspace.
