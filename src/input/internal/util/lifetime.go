package util

import (
	"context"
	"sync"
)

// Lifetime informs ongoing processes that the application is being stopped and should stop processing work.
type Lifetime struct {
	Ctx context.Context // Ctx provides access to the lifetime context, which is completed if the application is killed

	sigterm chan struct{}  // sigterm tells processes they should finish their current work and then close.
	sigkill chan struct{}  // sigkill tells processes they should stop immediately.
	stopped chan struct{}  // stopped is sent a signal when processes have stopped.
	wg      sync.WaitGroup // wg is used to ensure all processes have finished shutting down before sending the stopped signal.
}

// NewLifetime returns a new Lifetime object that is used to signal the end of the application lifetime to goroutines.
func NewLifetime() *Lifetime {
	l := Lifetime{
		Ctx:     context.Background(),
		sigterm: make(chan struct{}, 1),
		sigkill: make(chan struct{}, 1),
		stopped: make(chan struct{}, 1),
		wg:      sync.WaitGroup{},
	}
	return &l
}

// Terminate tells processes that the application is stopping soon, and they should finish their current work without
// taking on anything new.
func (l *Lifetime) Terminate() {
	l.sigterm <- struct{}{}
}

// Kill tells processes that the application is stopping immediately, and they should stop regardless of the state of
// their current work.
func (l *Lifetime) Kill() {
	l.sigkill <- struct{}{}
}

// Register is used to let Lifetime know there is a process that needs to stop when closing the application.
// It returns a pointer to the WaitGroup that can be used to signal that stopping is complete.
func (l *Lifetime) Register(n int) *sync.WaitGroup {
	l.wg.Add(n)
	return &l.wg
}

// Stopped returns a channel that can be used in a select statement to know when all child goroutines have finished
func (l *Lifetime) Stopped() chan struct{} {
	return l.stopped
}

// Terminated returns a channel that can be used in a select statement to know when the application is shutting down and
// current work should be completed without taking on any new work
func (l *Lifetime) Terminated() chan struct{} {
	return l.sigterm
}

// Killed returns a channel that can be used in a select statement to know when the application is shutting down and all
// work should cease immediately
func (l *Lifetime) Killed() chan struct{} {
	return l.sigkill
}

// StartStopListener starts a goroutine that signals when all registered subroutines have finished after a terminate or
// kill order
func (l *Lifetime) StartStopListener() {
	go l.wait()
}

// Runs in a goroutine to signal the Stopped channel when all processes have completed shutdown.
func (l *Lifetime) wait() {
	l.wg.Wait()
	l.stopped <- struct{}{}
}
