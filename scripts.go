package r2cache

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

// Embed self-contained Lua scripts so deployments need no runtime script files.
// All keys passed to a script must share the namespace hash tag on Redis Cluster.
var (
	//go:embed scripts/read.lua
	readSource string

	readScript = redis.NewScript(readSource)

	//go:embed scripts/acquire.lua
	acquireSource string

	acquireScript = redis.NewScript(acquireSource)

	//go:embed scripts/renew.lua
	renewSource string

	renewScript = redis.NewScript(renewSource)

	//go:embed scripts/commit.lua
	commitSource string

	commitScript = redis.NewScript(commitSource)

	//go:embed scripts/release.lua
	releaseSource string

	releaseScript = redis.NewScript(releaseSource)

	//go:embed scripts/hot_keys.lua
	hotKeysSource string

	hotKeysScript = redis.NewScript(hotKeysSource)

	//go:embed scripts/hot_count.lua
	hotCountSource string

	hotCountScript = redis.NewScript(hotCountSource)

	//go:embed scripts/next_refresh.lua
	nextRefreshSource string

	nextRefreshScript = redis.NewScript(nextRefreshSource)
)
