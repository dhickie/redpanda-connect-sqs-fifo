package tracking

import (
	"container/list"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
)

type MessageTracker struct {
	groups       map[string]*groupTracker      // Messages split by message group ID
	idMap        map[string]*models.SqsMessage // Map of message IDs to messages
	pendingFlush []*models.SqsMessage          // Messages pending a flush downstream
	pendingAck   []*models.SqsMessage          // Messages pending acknowledgement (and deletion from the queue)
	refreshQueue *list.List                    // A time ordered list of all in-flight messages by their visibility expiry
}

func NewMessageTracker() *MessageTracker {
	return &MessageTracker{
		groups:       make(map[string]*groupTracker),
		idMap:        make(map[string]*models.SqsMessage),
		pendingFlush: make([]*models.SqsMessage, 0),
		pendingAck:   make([]*models.SqsMessage, 0),
		refreshQueue: list.New(),
	}
}

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
		t.idMap[*msg.Msg.MessageId] = msg
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
func (t *MessageTracker) Peek(id string) (*models.SqsMessage, error) {
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
	t.pendingAck = append(t.pendingAck, msg)
	return msg
}

// Ack acknowledges that a message has been processed and deleted from the SQS queue
func (t *MessageTracker) Ack(id string) {
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

	// Clean up the message ID map
	delete(t.idMap, id)
}
