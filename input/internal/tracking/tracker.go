package tracking

import (
	"container/list"
	"context"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"slices"
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
}

// NewMessageTracker returns a new message tracker using the provided configuration and SQS client
func NewMessageTracker(conf *models.InputConfig, sqs *aws.SqsClient) *MessageTracker {
	return &MessageTracker{
		groups:       make(map[string]*groupTracker),
		idMap:        make(map[*string]*models.SqsMessage),
		pendingFlush: make([]*models.SqsMessage, 0),
		refreshQueue: list.New(),
		conf:         conf,
		sqs:          sqs,
	}
}

// Start starts the message tracker by starting the visibility refresh loop
func (t *MessageTracker) Start() {
	go t.refreshLoop()
}

// Add adds a collection of messages to the tracker
func (t *MessageTracker) Add(msgs []*models.SqsMessage) {
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
	msg, ok := t.idMap[id]
	if !ok {
		// TODO return error type
	}

	return msg, nil
}

// Flush returns the next message to be sent downstream and removes it from the pending messages list
func (t *MessageTracker) Flush() *models.SqsMessage {
	// TODO deal with case where no messages are available yet
	msg := t.pendingFlush[0]
	t.pendingFlush = t.pendingFlush[1:]
	return msg
}

// Ack acknowledges that a message has been processed and deleted from the SQS queue
func (t *MessageTracker) Ack(id *string) {
	msg, ok := t.idMap[id]
	if !ok {
		panic("Fatal: Ack called for unknown message")
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
	delete(t.idMap, id)
	e := t.refreshMap[*msg.Msg.MessageId]
	delete(t.refreshMap, *msg.Msg.MessageId)
	t.refreshQueue.Remove(e)
}

// Length returns how many messages are currently in the message tracker awaiting flushing or acknowledgment
func (t *MessageTracker) Length() int32 {
	i := int32(0)
	for _, v := range t.groups {
		i += v.len()
	}

	return i
}

// refreshLoop checks for any messages requiring a refresh of their visibility timeout
func (t *MessageTracker) refreshLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Build the list of messages which currently need refreshing
		refList := make([]*models.SqsMessage, 0)
		for element := t.refreshQueue.Front(); element != nil; element = t.refreshQueue.Front() {
			msg := element.Value.(*models.SqsMessage)
			if msg.RemainingDeadline() < t.conf.VisibilityTimeout/2 {
				refList = append(refList, msg)
				t.refreshQueue.MoveToBack(element) // Keep the refresh queue in order
			} else {
				break
			}
		}

		// Chunk into groups of 10 max
		for chunk := range slices.Chunk(refList, 10) {
			err := t.sqs.SetMessageVisibility(context.TODO(), int32(t.conf.VisibilityTimeout.Seconds()), chunk...)
			if err != nil {
				// TODO log a warning here
			}
		}
	}
}
