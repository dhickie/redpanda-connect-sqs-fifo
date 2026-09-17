package reader

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/tracking"
	"slices"
	"time"
)

type SqsFifoReader struct {
	client     *aws.SqsClient           // A client for the SQS API
	tracker    *tracking.MessageTracker // Tracks all in-flight messages
	pendingAck []*string                // Messages that have been acknowledged by the runtime but not yet deleted
	conf       *models.InputConfig      // The configuration for the input
}

func NewSqsFifoReader(conf *models.InputConfig) *SqsFifoReader {
	client := &aws.SqsClient{}
	return &SqsFifoReader{
		client:     client,
		tracker:    tracking.NewMessageTracker(conf, client),
		pendingAck: make([]*string, 0), // TODO add max capacity based on max in flight
		conf:       conf,
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
	r.pendingAck = append(r.pendingAck, id)
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
		// Get the actual messages
		msgs := make([]*models.SqsMessage, 0, len(r.pendingAck))
		for _, id := range r.pendingAck {
			msg, err := r.tracker.Peek(id)
			if err != nil {
				// TODO log an error here
				continue
			}
			msgs = append(msgs, msg)
		}

		// Batch them into 10 max messages
		batches := slices.Chunk(msgs, 10)
		for batch := range batches {
			err := r.client.DeleteMessages(context.TODO(), batch)
			if err != nil {
				// TODO log an error here
				continue
			}

			for _, msg := range batch {
				r.tracker.Ack(msg.Msg.MessageId)
			}
		}
	}
}
