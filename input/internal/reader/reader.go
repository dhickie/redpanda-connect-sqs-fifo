package reader

import (
	"container/list"
	"context"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/tracking"
	"time"
)

type SqsFifoReader struct {
	client      *aws.SqsClient           // A client for the SQS API
	tracker     *tracking.MessageTracker // Tracks all in-flight messages
	pendingAck  *list.List               // Messages that have been acknowledged by the runtime but not yet deleted
	pendingNack *list.List               // Messages that have had a negative acknowledgement by the runtime but not yet processed
	conf        *models.InputConfig      // The configuration for the input
}

// TODO add max capacity to all slices where possible
func NewSqsFifoReader(conf *models.InputConfig) *SqsFifoReader {
	client := &aws.SqsClient{}
	return &SqsFifoReader{
		client:      client,
		tracker:     tracking.NewMessageTracker(conf, client),
		pendingAck:  list.New(),
		pendingNack: list.New(),
		conf:        conf,
	}
}

// Healthcheck checks whether the reader can successfully connect to the target queue.
// Returns nil if the connection was successful
func (r *SqsFifoReader) Healthcheck() error {
	_, err := r.client.GetQueueVisibilityTimeout(context.TODO())
	return err
}

// Start starts the reader and begins populating the message queue
func (r *SqsFifoReader) Start() {
	r.tracker.Start()

	go r.readLoop()
	go r.ackLoop()
}

// Next returns the next message available for processing
func (r *SqsFifoReader) Next() *models.SqsMessage {
	return r.tracker.Flush()
}

// Ack acknowledges a message and adds it to the list of pending acknowledgements
func (r *SqsFifoReader) Ack(id *string) {
	r.pendingAck.PushBack(id)
}

// Nack acknowledges a message has failed processing and adds it to the list of pending failed acknowledgements
func (r *SqsFifoReader) Nack(id *string) {
	r.pendingNack.PushBack(id)
}

// Reads messages from the queue if there's room in the buffer
func (r *SqsFifoReader) readLoop() {
	// TODO replace ticker with cond
	t := time.NewTicker(time.Second)
	for range t.C {
		// Only pull more messages if there's capacity in the tracker
		capacity := r.conf.MaxInFlightMessages - r.tracker.Length()
		if capacity >= r.conf.MinReceiveBatchSize {
			msgs, err := r.client.ReceiveMessages(context.TODO(), capacity)
			if err != nil {
				// TODO log an error here
				continue
			}

			r.tracker.Add(msgs)
		}
	}
}

// Acknowledges pending messages by deleting them from the queue and removing them from the tracker
func (r *SqsFifoReader) ackLoop() {
	t := time.NewTicker(time.Second)
	for range t.C {
		batch := make([]*models.SqsMessage, 0, 10)
		for e := r.pendingAck.Front(); e != nil; e = e.Next() {
			msg, err := r.tracker.Peek(e.Value.(*string))
			if err != nil {
				// TODO log an error here
				continue
			}

			batch = append(batch, msg)
			if len(batch) == 10 || e.Next() == nil {
				err := r.client.DeleteMessages(context.TODO(), batch)
				if err != nil {
					// TODO log an error here
					// TODO deal with failed batch members
				}

				for _, bMsg := range batch {
					r.tracker.Ack(bMsg.Msg.MessageId)
				}

				clear(batch)
			}
		}
	}
}

// TODO add thread safety around pending collections
func (r *SqsFifoReader) nackLoop() {
	t := time.NewTicker(time.Second)
	for range t.C {
		ids := make([]*string, 0, r.pendingNack.Len())
		for e := r.pendingNack.Front(); e != nil; e = e.Next() {
			id := e.Value.(*string)
			_, err := r.tracker.Peek(id)
			if err != nil {
				// TODO log an error here
				continue
			}

			// We don't batch here for Nacks because the tracker will need to batch again anyway when resetting
			// message visibility for additional messages in the group
			ids = append(ids, id)
			r.tracker.Nack(ids...)
		}
	}
}
