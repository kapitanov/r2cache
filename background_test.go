package r2cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWakeupRegistry(t *testing.T) {
	c := &Cache[string, int]{waiters: make(map[string]*wakeup)}

	first, releaseFirst := c.waitFor("A")
	second, releaseSecond := c.waitFor("A")
	other, releaseOther := c.waitFor("B")
	require.Len(t, c.waiters, 2)
	require.Equal(t, first, second, "waiters on one key must share a wakeup channel")

	c.notify("A")
	for _, channel := range []<-chan struct{}{first, second} {
		select {
		case <-channel:
		default:
			t.Fatal("waiter for the notified key was not woken")
		}
	}
	select {
	case <-other:
		t.Fatal("waiter for an unrelated key was woken")
	default:
	}
	require.Len(t, c.waiters, 1, "notified key left in registry")

	releaseFirst()
	releaseSecond()
	require.Len(t, c.waiters, 1, "released waiters disturbed another key")

	c.notify("B")
	select {
	case <-other:
	default:
		t.Fatal("second key was not woken")
	}
	releaseOther()
	require.Empty(t, c.waiters, "released waiters left in registry")

	// A repeated notification must not close a channel twice.
	c.notify("A")
	fresh, releaseFresh := c.waitFor("A")
	select {
	case <-fresh:
		t.Fatal("new waiter received a stale notification")
	default:
	}
	releaseFresh()
	require.Empty(t, c.waiters, "idle waiter left in registry")

	// Notification drops the entry even while waiters still hold its channel.
	held, releaseHeld := c.waitFor("C")
	c.notify("C")
	select {
	case <-held:
	default:
		t.Fatal("held waiter was not woken")
	}
	require.Empty(t, c.waiters, "notified key left in registry")
	releaseHeld()
	require.Empty(t, c.waiters, "released waiter came back")
}
