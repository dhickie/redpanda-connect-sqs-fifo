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

// ------------------------------------------------------------------------------------------------------------------ //

// AsyncCond allows code to signal another single goroutine that it is OK to continue.
// The routines don't need to share a lock, and the signal and wait can happen asynchronously - if the signaller
// signals before a goroutine is waiting, then the next goroutine to get there won't need to wait at all.
// If the signaller signals when there is already a signal there, the signal is skipped.
type AsyncCond struct {
	ch chan struct{} // The underlying channel
}

func NewAsyncCond() *AsyncCond {
	return &AsyncCond{
		ch: make(chan struct{}, 1),
	}
}

// Signal signals that it is OK for another goroutine to continue. Does not block if the signal has already been set
func (c *AsyncCond) Signal() {
	select {
	case c.ch <- struct{}{}:
	default:
	}
}

// Wait waits for the condition to be signalled, if it hasn't already.
func (c *AsyncCond) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ch:
		return nil
	}
}

// WaitChan returns a channel for use in select statements that can be read from when the cond is signalled
func (c *AsyncCond) WaitChan() <-chan struct{} {
	return c.ch
}
