package sqs_fifo

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type sqsInFlightTracker struct {
	handles map[string]*list.Element
	fifo    *list.List // contains *sqsMessageHandle
	limit   int
	timeout time.Duration
	m       sync.Mutex
	l       *sync.Cond
}

func (t *sqsInFlightTracker) PullToRefresh(limit int) []*sqsMessageHandle {
	t.m.Lock()
	defer t.m.Unlock()

	handles := make([]*sqsMessageHandle, 0, limit)
	now := time.Now()
	// Pull the front of our fifo until we reach our limit or we reach elements that do not
	// need to be refreshed
	for e := t.fifo.Front(); e != nil && len(handles) < limit; e = t.fifo.Front() {
		v := e.Value.(*sqsMessageHandle)
		if v.deadline.Sub(now) > (t.timeout / 2) {
			break
		}
		handles = append(handles, v)
		v.deadline = now.Add(t.timeout)
		// Keep our fifo in deadline sorted order
		t.fifo.MoveToBack(e)
	}
	return handles
}

func (t *sqsInFlightTracker) Size() int {
	t.m.Lock()
	defer t.m.Unlock()
	return len(t.handles)
}

func (t *sqsInFlightTracker) Remove(id string) {
	t.m.Lock()
	defer t.m.Unlock()
	entry, ok := t.handles[id]
	if ok {
		t.fifo.Remove(entry)
		delete(t.handles, id)
	}
	t.l.Signal()
}

func (t *sqsInFlightTracker) IsTracking(id string) bool {
	t.m.Lock()
	defer t.m.Unlock()
	_, ok := t.handles[id]
	return ok
}

func (t *sqsInFlightTracker) Clear() {
	t.m.Lock()
	defer t.m.Unlock()
	clear(t.handles)
	t.fifo = list.New()
	t.l.Signal()
}

func (t *sqsInFlightTracker) AddNew(ctx context.Context, messages ...sqsMessage) {
	t.m.Lock()
	defer t.m.Unlock()

	// Treat this as a soft limit, we can burst over, but we should be able to make progress.
	for len(t.handles) >= t.limit {
		if ctx.Err() != nil {
			return
		}
		t.l.Wait()
	}

	for _, m := range messages {
		if m.handle == nil {
			continue
		}
		// If this is a duplicate (a re-receive of an inflight message due to timeout)
		// we can just update the existing handle.
		if e, ok := t.handles[m.handle.id]; ok {
			e.Value = m.handle
			t.fifo.MoveToBack(e)
		} else {
			e := t.fifo.PushBack(m.handle)
			t.handles[m.handle.id] = e
		}
	}
}
