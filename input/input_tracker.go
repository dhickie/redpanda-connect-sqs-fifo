package sqs_fifo

import (
	"container/list"
	"sync"
)

// Tracks the current processing status of a message group that must be processed sequentially
// Each group can only have a single message in flight at once in order to guarantee ordering
type groupTracker struct {
	queue    []*sqsMessage // The queue of pending messages for this group
	inFlight bool          // Whether the message at the front of the queue is currently in flight
	m        sync.Mutex    // For thread safety
}

// Create a new empty group tracker for a group of messages that share a message group ID
func newGroupTracker() *groupTracker {
	return &groupTracker{
		queue:    make([]*sqsMessage, 0),
		inFlight: false,
		m:        sync.Mutex{},
	}
}

// Add a collection of messages to the group tracker.
// If the group doesn't currently have an inflight message, it returns the next message and marks the group as in flight
func (t *groupTracker) add(msgs []*sqsMessage) *sqsMessage {
	t.m.Lock()
	defer t.m.Unlock()

	t.queue = append(t.queue, msgs...)

	if !t.inFlight {
		t.inFlight = true
		return t.queue[0]
	}

	return nil
}

// Deletes the message at the front of the queue, and returns the next message if there is one ready
func (t *groupTracker) delete() (*sqsMessage, error) {
	t.m.Lock()
	defer t.m.Unlock()

	if !t.inFlight {
		// TODO return error type
	}

	t.queue = t.queue[1:]

	if len(t.queue) > 0 {
		return t.queue[0], nil
	}

	t.inFlight = false
	return nil, nil
}

// ------------------------------------------------------------------------------------------------------------------ //

type messageTracker struct {
	groups       map[string]*groupTracker // Messages split by message group ID
	pendingFlush []*sqsMessage            // Messages pending a flush downstream
	pendingAck   []*sqsMessage            // Messages pending acknowledgement (and deletion from the queue)
	refreshQueue *list.List               // A time ordered list of all in-flight messages by their visibility expiry
}

func newMessageTracker() *messageTracker {
	return &messageTracker{
		groups:       make(map[string]*groupTracker),
		pendingFlush: make([]*sqsMessage, 0),
		pendingAck:   make([]*sqsMessage, 0),
		refreshQueue: list.New(),
	}
}

func (t *messageTracker) add(msgs []*sqsMessage) {
	// Group the messages by message group ID
	tempMap := make(map[string][]*sqsMessage)
	for _, msg := range msgs {
		mgid := msg.Attributes["MessageGroupId"]
		_, ok := tempMap[mgid]
		if !ok {
			tempMap[mgid] = make([]*sqsMessage, 0)
		}

		tempMap[mgid] = append(tempMap[mgid], msg)
	}

	// Add them to their respective groups, and add to the pending flush list there's a new message ready to pass
	// downstream
	for k, v := range tempMap {
		_, ok := t.groups[k]
		if !ok {
			t.groups[k] = newGroupTracker()
		}
		next := t.groups[k].add(v)
		if next != nil {
			t.pendingFlush = append(t.pendingFlush, next)
		}
	}
}
