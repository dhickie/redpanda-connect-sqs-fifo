package util

import (
	"context"
	"sync"
)

// ContextCond provides the ability to wait until a signal is received or a context is cancelled
type ContextCond struct {
	ch chan struct{} // Channel used to signal that something has changed
	l  sync.Locker   // The locker that underlies the signal
}

// NewContextCond returns a new instance using the provided underlying locker
func NewContextCond(l sync.Locker) *ContextCond {
	return &ContextCond{
		ch: make(chan struct{}, 1),
		l:  l,
	}
}

// Wait waits until either a signal is provided to the cond or the provided context is cancelled.
// The thread calling Wait must currently hold the underlying lock before calling this method.
// Returns the error from the context if the context is cancelled.
func (c *ContextCond) Wait(ctx context.Context) error {
	c.l.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ch:
		c.l.Lock()
		return nil
	}
}

// Signal signals a waiting thread, if there is one, that is waiting on this condition
func (c *ContextCond) Signal() {
	select {
	case c.ch <- struct{}{}:
		return
	default:
	}
}
