package r2cache

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Redis Ring shards keys by hash, so Build must reject a namespace without a
// hash tag before any Redis I/O. The shard address points nowhere; both the
// rejection and the successful build must stay offline.
func TestIssue37RingKeyPrefixValidation(t *testing.T) {
	client := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"shard-0": "127.0.0.1:0"},
	})
	t.Cleanup(func() { _ = client.Close() })
	tests := []struct {
		name   string
		prefix string
		valid  bool
	}{
		{"empty prefix", "", false},
		{"untagged prefix", "svc", false},
		{"empty tag", "svc:{}", false},
		{"empty first tag ignores later tag", "a{}c{d}", false},
		{"unclosed tag", "svc:{products", false},
		{"tagged prefix", "svc:{products}", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := NewBuilder[string, int]().
				UseRedisClient(client).
				KeyPrefix(tt.prefix).
				Fetch(func(context.Context, string) (int, error) { return 0, nil }).
				Build()
			if !tt.valid {
				require.ErrorContains(t, err, "KeyPrefix")
				require.ErrorContains(t, err, "Redis Cluster or Redis Ring")
				require.ErrorContains(t, err, "requires a non-empty hash tag")
				require.Nil(t, cache)
				return
			}
			require.NoError(t, err)
			cache.Shutdown()
		})
	}
}
