package r2cache

import (
	"context"
	"time"
)

func (c *Cache[K, V]) notifications() {
	defer c.wg.Done()

	sub := c.client.Subscribe(c.ctx, c.base+"events")
	defer func() { _ = sub.Close() }()
	messages := sub.Channel()

	for {
		select {
		case <-c.ctx.Done():
			return

		case _, ok := <-messages:
			if !ok {
				return
			}

			c.signalMu.Lock()
			close(c.signal)
			c.signal = make(chan struct{})
			c.signalMu.Unlock()
		}
	}
}

func (c *Cache[K, V]) refresh() {
	defer c.wg.Done()

	// One synchronous worker per replica bounds local background work. The shared
	// scheduler spreads starts globally, including when many keys become due at once.

	delay := time.Millisecond
	for {
		timer := time.NewTimer(delay)

		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		op, stop := context.WithTimeout(c.ctx, 2*time.Second)

		result, err := nextRefreshScript.Run(op, c.client, []string{c.base + "hot", c.base + "due", c.base + "pace"}, c.cfg.refresh.UpdateInterval.Milliseconds()).Slice()
		stop()

		delay = 100 * time.Millisecond
		if err != nil || len(result) == 0 {
			continue
		}

		ms, ok := result[0].(int64)
		if !ok {
			continue
		}

		if ms > 0 {
			delay = time.Duration(ms) * time.Millisecond
		}

		if delay > 100*time.Millisecond {
			delay = 100 * time.Millisecond
		}

		if len(result) > 1 {
			if key, ok := result[1].(string); ok {
				_, _, _ = c.attempt(c.ctx, K(key), true)
			}
		}
	}
}

func (c *Cache[K, V]) metrics() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}

		count := int64(0)
		if c.cfg.refresh != nil {
			op, stop := context.WithTimeout(c.ctx, time.Second)

			var err error
			count, err = hotCountScript.Run(op, c.client, []string{c.base + "hot"}).Int64()
			stop()

			if err != nil {
				count = -1
			}
		}

		if c.ctx.Err() != nil {
			return
		}

		c.cfg.metrics(MetricValues{HitCount: c.hits.Load(), MissCount: c.misses.Load(), BackendFetchCount: c.fetches.Load(), HotKeyCount: count})
	}
}
