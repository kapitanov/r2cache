package r2cache

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Issue #38: one scheduler poll must drain a large expired backlog in bounded
// batches instead of stopping after a single 128-member batch.
const (
	issue38Batch   = 128
	issue38Cap     = 1024 // 8 batches per call
	issue38Backlog = 2000
)

func TestIssue38(t *testing.T) {
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

	const unexpired = "unexpired"
	// seed writes issue38Backlog expired hot members with matching due members,
	// plus one unexpired hot/due pair whose due time is far in the future. Scores
	// come from server time so the script's own TIME comparison agrees with them.
	seed := func(t *testing.T, hotKey, dueKey string, now time.Time) {
		t.Helper()
		_, err := admin.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for i := range issue38Backlog {
				member := fmt.Sprintf("expired-%04d", i)
				pipe.ZAdd(ctx, hotKey, redis.Z{Member: member, Score: float64(now.UnixMilli() - 1000 - int64(i))})
				pipe.ZAdd(ctx, dueKey, redis.Z{Member: member, Score: float64(now.UnixMilli())})
			}
			future := float64(now.Add(time.Minute).UnixMilli())
			pipe.ZAdd(ctx, hotKey, redis.Z{Member: unexpired, Score: future})
			pipe.ZAdd(ctx, dueKey, redis.Z{Member: unexpired, Score: future})
			return nil
		})
		require.NoError(t, err)
	}

	t.Run("bounded batches drain on immediate re-poll", func(t *testing.T) {
		now, err := admin.Time(ctx).Result()
		require.NoError(t, err)
		hotKey := t.Name() + ":hot"
		dueKey := t.Name() + ":due"
		paceKey := t.Name() + ":pace"
		seed(t, hotKey, dueKey, now)

		interval := int64(time.Second / time.Millisecond)
		result, err := nextRefreshScript.Run(ctx, admin, []string{hotKey, dueKey, paceKey}, interval).Slice()
		require.NoError(t, err)
		require.Equal(t, []interface{}{int64(1)}, result, "capped call must request an immediate re-poll")

		// The first call must prune the documented per-call cap, not one batch.
		hotLeft := admin.ZCard(ctx, hotKey).Val()
		pruned := int64(issue38Backlog+1) - hotLeft
		require.Equal(t, int64(issue38Cap), pruned, "first call must prune exactly the per-call cap")
		require.Greater(t, pruned, int64(issue38Batch), "single-batch pruning regressed")
		require.Equal(t, hotLeft, admin.ZCard(ctx, dueKey).Val(), "pruned members left in the due index")
		// Scores ascend, so the oldest members (highest index) are pruned first.
		require.ErrorIs(t, admin.ZScore(ctx, dueKey, "expired-1999").Err(), redis.Nil, "due entry of a pruned key survived")
		require.NoError(t, admin.ZScore(ctx, dueKey, unexpired).Err(), "unexpired due entry pruned")

		// The second call drains the remainder and falls back to normal pacing.
		result, err = nextRefreshScript.Run(ctx, admin, []string{hotKey, dueKey, paceKey}, interval).Slice()
		require.NoError(t, err)
		require.Equal(t, []interface{}{int64(0)}, result, "drained call must keep the {delay} reply shape")
		require.Equal(t, int64(1), admin.ZCard(ctx, hotKey).Val(), "backlog not drained")
		require.Equal(t, int64(1), admin.ZCard(ctx, dueKey).Val(), "due backlog not drained")
		hot, err := admin.ZScore(ctx, hotKey, unexpired).Result()
		require.NoError(t, err, "unexpired hot member pruned")
		require.Greater(t, hot, float64(now.UnixMilli()), "unexpired hot member aged out")
		due, err := admin.ZScore(ctx, dueKey, unexpired).Result()
		require.NoError(t, err, "unexpired due member pruned")
		require.Greater(t, due, float64(now.UnixMilli()), "unexpired due member aged out")
		for _, member := range []string{"expired-0000", "expired-1999"} {
			require.ErrorIs(t, admin.ZScore(ctx, hotKey, member).Err(), redis.Nil, "expired hot member survived")
			require.ErrorIs(t, admin.ZScore(ctx, dueKey, member).Err(), redis.Nil, "expired due member survived")
		}
	})

	t.Run("background scheduler drains a seeded backlog", func(t *testing.T) {
		c := testCache(t, NewBuilder[string, int]().RedisURL(url).
			KeyPrefix(t.Name()).
			Fetch(func(context.Context, string) (int, error) { return 1, nil }).
			EnableAutoRefresh(AutoRefreshPolicy{HotKeyLifetime: time.Minute, UpdateInterval: time.Second}))
		now, err := admin.Time(ctx).Result()
		require.NoError(t, err)
		seed(t, c.base+"hot", c.base+"due", now)

		// The old 128-per-poll cap needed about 16s for this backlog; draining
		// within a few polls proves the immediate re-poll path works end to end.
		eventually(t, 5*time.Second, func() bool { return admin.ZCard(ctx, c.base+"hot").Val() == 1 })
		require.Equal(t, int64(1), admin.ZCard(ctx, c.base+"due").Val(), "due backlog not drained")
		require.NoError(t, admin.ZScore(ctx, c.base+"hot", unexpired).Err(), "unexpired hot member pruned")
	})
}
