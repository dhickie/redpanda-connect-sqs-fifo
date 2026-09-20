package tracking

import (
	"container/list"
	"context"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/util"
	"slices"
	"sync"
	"time"
)

// MessageTracker tracks all messages that have been received from SQS but haven't yet been deleted from the queue
type MessageTracker struct {
	groups       map[string]*groupTracker       // Messages split by message group ID
	idMap        map[*string]*models.SqsMessage // Map of message IDs to messages
	pendingFlush []*models.SqsMessage           // Messages pending a flush downstream
	refreshQueue *list.List                     // A time ordered list of all in-flight messages by their visibility expiry
	refreshMap   map[string]*list.Element       // Map of message IDs to elements in the refresh queue
	conf         *models.InputConfig            // The configuration for the input
	sqs          *aws.SqsClient                 // The SQS client
	m            *sync.RWMutex                  // The mutex used to provide thread safety
	lt           *util.Lifetime                 // Manages application lifetime and shutdown events
}

// NewMessageTracker returns a new message tracker using the provided configuration and SQS client
func NewMessageTracker(conf *models.InputConfig, sqs *aws.SqsClient, lt *util.Lifetime) *MessageTracker {
	return &MessageTracker{
		groups:       make(map[string]*groupTracker),
		idMap:        make(map[*string]*models.SqsMessage),
		pendingFlush: make([]*models.SqsMessage, 0),
		refreshQueue: list.New(),
		conf:         conf,
		sqs:          sqs,
		m:            &sync.RWMutex{},
		lt:           lt,
	}
}

// Start starts the message tracker by starting the visibility refresh loop
func (t *MessageTracker) Start() {
	wg := t.lt.Register(1)
	wg.Go(t.refreshLoop)
}

// Add adds a collection of messages to the tracker
func (t *MessageTracker) Add(msgs []*models.SqsMessage) {
	t.m.Lock()
	defer t.m.Unlock()

	// Group the messages by message group ID
	tempMap := make(map[string][]*models.SqsMessage)
	for _, msg := range msgs {
		mgid := msg.GetGroupId()

		_, ok := tempMap[mgid]
		if !ok {
			tempMap[mgid] = make([]*models.SqsMessage, 0)
		}

		tempMap[mgid] = append(tempMap[mgid], msg)

		// Add to the relevant maps/lists to support later processes
		t.idMap[msg.Msg.MessageId] = msg
		e := t.refreshQueue.PushBack(msg)
		t.refreshMap[*msg.Msg.MessageId] = e
	}

	// Add them to their respective groups, and add to the pending flush list if there's a new message ready to pass
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

// Peek gets a message by its ID without flushing or acknowledging it
func (t *MessageTracker) Peek(id *string) (*models.SqsMessage, error) {
	t.m.RLock()
	defer t.m.RUnlock()

	msg, ok := t.idMap[id]
	if !ok {
		// TODO return error type
	}

	return msg, nil
}

// Flush returns the next message to be sent downstream and removes it from the pending messages list
func (t *MessageTracker) Flush() *models.SqsMessage {
	t.m.Lock()
	defer t.m.Unlock()

	// TODO deal with case where no messages are available yet
	msg := t.pendingFlush[0]
	t.pendingFlush = t.pendingFlush[1:]
	return msg
}

// Ack acknowledges that a message has been processed and deleted from the SQS queue
func (t *MessageTracker) Ack(id *string) {
	t.m.Lock()
	defer t.m.Unlock()

	msg, ok := t.idMap[id]
	if !ok {
		panic("Fatal: Ack called for unknown message") // TODO maybe just log error instead?
	}

	// Delete the message from the front of its group
	gId := msg.GetGroupId()
	next, err := t.groups[gId].delete()
	if err != nil {
		panic(err)
	}

	if next != nil {
		// Add the next message to the queue if there is one
		t.pendingFlush = append(t.pendingFlush, next)
	} else {
		// Delete the group from the map since it is now empty
		delete(t.groups, gId)
	}

	// Clean up the message ID map and refresh queue
	t.cleanupMessages(msg)
}

// Nack acknowledges that messages have failed to be processed correctly,
// and either need to be retried or returned to the queue.
// Returns a collection of messages which are being abandoned.
func (t *MessageTracker) Nack(ctx context.Context, ids ...*string) error {
	t.m.Lock()
	defer t.m.Unlock()

	noRetry := make([]*models.SqsMessage, 0, len(ids))
	for _, id := range ids {
		msg, ok := t.idMap[id]
		if !ok {
			panic("Fatal: Nack called for unknown message") // TODO maybe just log error instead?
		}

		// Increment the attempt counter
		msg.AttemptNo++

		// Retry - add it back to the end of pendingFlush
		if t.conf.MaxProcessingAttempts == 0 || t.conf.MaxProcessingAttempts <= msg.AttemptNo {
			t.pendingFlush = append(t.pendingFlush, msg)
			continue
		}

		// Abandon
		noRetry = append(noRetry, msg)
	}

	// For abandoned messages, abandon all messages in the group - we can't process them without breaking ordering
	abandon := make([]*models.SqsMessage, 0, len(noRetry))
	for _, msg := range noRetry {
		groupId := msg.GetGroupId()
		group, ok := t.groups[groupId]
		if !ok {
			panic("Fatal: Unable to find group for message") // TODO maybe just log error instead?
		}

		abandon = append(abandon, group.queue...)

		// Clean up id maps/lists and delete the group - this is OK to do here as we are inside the lock
		t.cleanupMessages(group.queue...)
		delete(t.groups, groupId)
	}

	// Batch abandoned messages
	batches := slices.Chunk(abandon, 10)
	for batch := range batches {
		// Reset message visibility to make it available again
		err := t.sqs.SetMessageVisibility(ctx, 0, batch...)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			// TODO handle partial batch failures - log a warning
		}
	}

	return nil
}

// Length returns how many messages are currently in the message tracker awaiting flushing or acknowledgment
func (t *MessageTracker) Length() int {
	t.m.RLock()
	defer t.m.RUnlock()

	i := 0
	for _, v := range t.groups {
		i += v.len()
	}

	return i
}

// Cleans up the tracking list/map when we no longer care about a message
func (t *MessageTracker) cleanupMessages(msgs ...*models.SqsMessage) {
	for _, msg := range msgs {
		id := msg.Msg.MessageId
		delete(t.idMap, id)
		e := t.refreshMap[*id]
		delete(t.refreshMap, *id)
		t.refreshQueue.Remove(e)
	}
}

// refreshLoop checks for any messages requiring a refresh of their visibility timeout
func (t *MessageTracker) refreshLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

refreshLoop:
	for {
		select {
		case <-t.lt.Terminated():
			t.resetVisibility(t.lt.Ctx)
			break refreshLoop
		case <-t.lt.Killed():
			break refreshLoop
		case <-ticker.C:
			t.refreshVisibility(t.lt.Ctx)
		}
	}
}

func (t *MessageTracker) refreshVisibility(ctx context.Context) {
	t.m.Lock()
	defer t.m.Unlock()

	// Build the list of messages which currently need refreshing
	refList := make([]*models.SqsMessage, 0)
	for element := t.refreshQueue.Front(); element != nil; element = t.refreshQueue.Front() {
		msg := element.Value.(*models.SqsMessage)
		if msg.RemainingDeadline() < t.conf.VisibilityTimeoutSeconds/2 {
			refList = append(refList, msg)
			// TODO don't move to back until refresh has been successful
			t.refreshQueue.MoveToBack(element) // Keep the refresh queue in order
		} else {
			break
		}
	}

	// Chunk into groups of 10 max
	for batch := range slices.Chunk(refList, 10) {
		err := t.sqs.SetMessageVisibility(ctx, int32(t.conf.VisibilityTimeoutSeconds), batch...)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return
			}

			// TODO log a warning here
		}

		for _, msg := range batch {
			msg.RefreshDeadline(t.conf.VisibilityTimeoutSeconds)
		}
	}
}

// Resets the visibility of all messages to zero to allow other applications to pick them up as quickly as possible
func (t *MessageTracker) resetVisibility(ctx context.Context) {
	t.m.Lock()
	defer t.m.Unlock()

	msgs := make([]*models.SqsMessage, 0, t.conf.MaxInFlightMessages)
	for _, g := range t.groups {
		for _, msg := range g.queue {
			msgs = append(msgs, msg)
		}
	}

	// Batch to 10 max
	batches := slices.Chunk(msgs, 10)
	for batch := range batches {
		if err := t.sqs.SetMessageVisibility(ctx, 0, batch...); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return
			}

			// TODO log an error here
		}
	}
}
