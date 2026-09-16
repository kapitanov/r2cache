package r2cache

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Exercise scripts against an actual cluster-enabled server. A one-node cluster
// owns all slots; this checks CROSSSLOT compatibility, not failover or resharding.
func TestCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("integration tests require Docker")
	}
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7.4.2-alpine", testcontainers.WithCmd("redis-server", "--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf"))
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)
	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	admin := redis.NewClient(&redis.Options{Addr: endpoint})
	defer func() { _ = admin.Close() }()
	require.NoError(t, admin.Do(ctx, "CLUSTER", "ADDSLOTSRANGE", 0, 16383).Err())
	eventually(t, 10*time.Second, func() bool { return errors.Is(admin.Get(ctx, "cluster-ready").Err(), redis.Nil) })
	// Docker maps the node's port. Supply the mapped address for slot discovery.
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{endpoint}, ContextTimeoutEnabled: true,
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: endpoint}}}}, nil
		},
	})
	defer func() { _ = client.Close() }()
	topologySmoke(t, client)
}

// Exercise Sentinel discovery using a real Sentinel and master in one disposable
// container. Address translation is needed for Docker's mapped master port.
func TestSentinel(t *testing.T) {
	if testing.Short() {
		t.Skip("integration tests require Docker")
	}
	ctx := context.Background()
	container, err := testcontainers.Run(ctx, "redis:7.4.2-alpine",
		testcontainers.WithExposedPorts("6379/tcp", "26379/tcp"),
		testcontainers.WithCmd("sh", "-c", `redis-server --daemonize yes
printf 'port 26379\nsentinel monitor mymaster 127.0.0.1 6379 1\n' > /tmp/sentinel.conf
exec redis-server /tmp/sentinel.conf --sentinel`),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("6379/tcp"), wait.ForListeningPort("26379/tcp")),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)
	master, err := container.PortEndpoint(ctx, "6379/tcp", "")
	require.NoError(t, err)
	sentinel, err := container.PortEndpoint(ctx, "26379/tcp", "")
	require.NoError(t, err)
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "mymaster", SentinelAddrs: []string{sentinel}, ContextTimeoutEnabled: true,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if addr == "127.0.0.1:6379" {
				addr = master
			}
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	})
	defer func() { _ = client.Close() }()
	topologySmoke(t, client)
}

func topologySmoke(t *testing.T, client redis.UniversalClient) {
	t.Helper()
	var calls atomic.Int64
	cache, err := NewBuilder[string, int]().
		UseRedisClient(client).
		KeyPrefix(t.Name() + "{arbitrary}").
		Fetch(func(ctx context.Context, _ string) (int, error) {
			n := calls.Add(1)
			select {
			case <-time.After(1500 * time.Millisecond):
				return int(n), nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}).
		// Fetch longer than the lease to exercise renewal, with enough headroom
		// for Docker scheduling and race instrumentation on busy runners.
		LeaseDuration(time.Second).
		LimitConcurrentFetches(1).
		EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: 5 * time.Second, UpdateInterval: 500 * time.Millisecond}).
		Build()
	require.NoError(t, err)
	defer cache.Shutdown()
	first, second := mustGet(t, cache, "key{other}"), mustGet(t, cache, "key{other}")
	require.Equal(t, 1, first)
	require.Equal(t, 1, second, "read-through failed")
	require.Len(t, cache.HotKeys(context.Background()), 1, "hot key tracking failed")
	eventually(t, 3*time.Second, func() bool { return calls.Load() >= 2 })
}
