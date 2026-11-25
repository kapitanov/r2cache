package r2cache

import (
	"context"
	"time"
)

// wakeup groups the waiters currently registered for one key. They share a
// channel, so one notification wakes every waiter for that key.
type wakeup struct {
	channel chan struct{}
	waiters int
}

// waitFor registers the caller as a waiter for key. The returned channel is
// closed by notify when the key changes; release must be called when the caller
// stops waiting so an unpublished key does not retain an entry.
func (c *Cache[K, V]) waitFor(key K) (<-chan struct{}, func()) {
	name := string(key)

	c.waitMu.Lock()
	w := c.waiters[name]
	if w == nil {
		w = &wakeup{channel: make(chan struct{})}
		c.waiters[name] = w
	}
	w.waiters++
	c.waitMu.Unlock()

	return w.channel, func() {
		c.waitMu.Lock()
		w.waiters--
		if w.waiters == 0 && c.waiters[name] == w {
			delete(c.waiters, name)
		}
		c.waitMu.Unlock()
	}
}

// notify wakes every waiter registered for key. The entry is removed before its
// channel is closed, which keeps each channel closed exactly once and drops keys
// whose only event was the notification.
func (c *Cache[K, V]) notify(key string) {
	c.waitMu.Lock()
	w := c.waiters[key]
	delete(c.waiters, key)
	c.waitMu.Unlock()

	if w != nil {
		close(w.channel)
	}
}

func (c *Cache[K, V]) notifications() {
	defer c.wg.Done()

	sub := c.client.Subscribe(c.ctx, c.base+"events")
	defer func() { _ = sub.Close() }()
	messages := sub.Channel()

	for {
		select {
		case <-c.ctx.Done():
			return

		case message, ok := <-messages:
			if !ok {
				return
			}

			c.notify(message.Payload)
		}
	}
}

func (c *Cache[K, V]) refresh() {
	defer c.wg.Done()

	// One synchronous worker per replica bounds local background work. The shared
	// scheduler spreads starts globally, including when many keys become due at once.

	interval := c.cfg.refresh.UpdateInterval.Milliseconds()
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

		result, err := nextRefreshScript.Run(op, c.client, []string{c.base + "hot", c.base + "due", c.base + "pace"}, interval).Slice()
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
				if _, _, err := c.attempt(c.ctx, K(key), true); err != nil {
					// The due time set before the attempt may already be past when a
					// slow fetch fails. Defer from the failure instead; busy and
					// no-longer-hot keys never reach this branch.
					op, stop := context.WithTimeout(c.ctx, 2*time.Second)
					_ = backoffScript.Run(op, c.client, []string{c.base + "hot", c.base + "due"}, key, interval).Err()
					stop()
				}
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

		c.cfg.metrics(MetricValues{HitCount: c.hits.Load(), MissCount: c.misses.Load(), BackendFetchCount: c.fetches.Load(), DecodeErrorCount: c.decodeErrors.Load(), HotKeyCount: count})
	}
}
