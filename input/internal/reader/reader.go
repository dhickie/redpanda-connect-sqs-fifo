package reader

import (
	"container/list"
	"context"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/tracking"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/util"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type SqsFifoReader struct {
	client      *aws.SqsClient           // A client for the SQS API
	tracker     *tracking.MessageTracker // Tracks all in-flight messages
	pendingAck  *list.List               // Messages that have been acknowledged by the runtime but not yet deleted
	ackLock     *sync.Mutex              // A lock for protecting the collection of pending acks
	ackCond     *util.AsyncCond          // For signalling the ackloop to process acks
	pendingNack *list.List               // Messages that have had a negative acknowledgement by the runtime but not yet processed
	nackLock    *sync.Mutex              // A lock for protecting the collection of pending nacks
	nackCond    *util.AsyncCond          // For signalling the nackloop to process nacks
	readCond    *util.AsyncCond          // For being signalled that there is capacity for more in-flight messages
	conf        *models.InputConfig      // The configuration for the input
	lt          *util.Lifetime           // Manages application lifetime and shutdown events
	logger      *service.Logger          // For writing custom logs
}

func NewSqsFifoReader(conf *models.InputConfig, lt *util.Lifetime, logger *service.Logger) *SqsFifoReader {
	client := &aws.SqsClient{}
	readCond := util.NewAsyncCond()
	readCond.Signal() // Start in a signalled state to start reading immediately

	return &SqsFifoReader{
		client:      client,
		tracker:     tracking.NewMessageTracker(conf, client, lt, readCond, logger),
		pendingAck:  list.New(),
		ackLock:     &sync.Mutex{},
		ackCond:     util.NewAsyncCond(),
		pendingNack: list.New(),
		nackLock:    &sync.Mutex{},
		nackCond:    util.NewAsyncCond(),
		readCond:    readCond,
		conf:        conf,
		lt:          lt,
		logger:      logger,
	}
}

// Healthcheck checks whether the reader can successfully connect to the target queue.
// Returns nil if the connection was successful
func (r *SqsFifoReader) Healthcheck(ctx context.Context) error {
	_, err := r.client.GetQueueVisibilityTimeout(ctx)
	return err
}

// Start starts the reader and begins populating the message queue
func (r *SqsFifoReader) Start() {
	r.tracker.Start()

	wg := r.lt.Register(2)
	wg.Go(r.readLoop)
	wg.Go(r.ackLoop)
	r.logger.Debug("Read and ack loops started")
}

// Next returns the next message available for processing
func (r *SqsFifoReader) Next(ctx context.Context) (*models.SqsMessage, error) {
	return r.tracker.Flush(ctx)
}

// Ack acknowledges a message and adds it to the list of pending acknowledgements
func (r *SqsFifoReader) Ack(id *string) {
	r.ackLock.Lock()
	defer r.ackLock.Unlock()

	r.pendingAck.PushBack(id)
	if r.pendingAck.Len() >= r.conf.MaxPendingAcks {
		r.ackCond.Signal()
	}
}

// Nack acknowledges a message has failed processing and adds it to the list of pending failed acknowledgements
func (r *SqsFifoReader) Nack(id *string) {
	r.nackLock.Lock()
	defer r.nackLock.Unlock()

	r.pendingNack.PushBack(id)
	if r.pendingNack.Len() >= r.conf.MaxPendingAcks {
		r.nackCond.Signal()
	}
}

// Reads messages from the queue if there's room in the buffer
func (r *SqsFifoReader) readLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()

readLoop:
	for {
		select {
		case <-r.lt.Terminated():
			r.logger.Debug("Read loop received graceful termination order - breaking loop")
			break readLoop
		case <-r.lt.Killed():
			r.logger.Debug("Read loop received kill order - breaking loop")
			break readLoop
		case <-t.C:
			r.read(r.lt.Ctx)
		case <-r.readCond.WaitChan():
			r.read(r.lt.Ctx)
		}
	}
}

func (r *SqsFifoReader) read(ctx context.Context) {
	// Only pull more messages if there's capacity in the tracker
	capacity := r.conf.MaxInFlightMessages - r.tracker.Length()
	if capacity >= r.conf.MinReceiveBatchSize {
		msgs, err := r.client.ReceiveMessages(ctx, capacity)
		if err != nil {
			r.logger.Errorf("Failed to receive messages from queue: %v", err.Error())
			return
		}

		r.logger.Debugf("Received %d messages from queue", len(msgs))
		newLen := r.tracker.Add(msgs)
		if r.conf.MaxInFlightMessages-newLen >= r.conf.MinReceiveBatchSize {
			// Still room for more
			r.readCond.Signal()
		}
	}
}

// Acknowledges pending messages by deleting them from the queue and removing them from the tracker
func (r *SqsFifoReader) ackLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()

ackLoop:
	for {
		select {
		// TODO do a better job of graceful shutdown - process until pending acks is empty
		case <-r.lt.Killed():
			break ackLoop
		case <-t.C:
			if err := r.ack(r.lt.Ctx); err != nil {
				r.logger.Debug("Ack loop received kill order - breaking loop")
				break ackLoop
			}
		case <-r.ackCond.WaitChan():
			if err := r.ack(r.lt.Ctx); err != nil {
				r.logger.Debug("Ack loop received kill order - breaking loop")
				break ackLoop
			}
		}
	}
}

func (r *SqsFifoReader) ack(ctx context.Context) error {
	r.ackLock.Lock()
	defer r.ackLock.Unlock()

	r.logger.Debugf("Ack loop found %v acks to process", r.pendingAck.Len())

	batch := make([]*models.SqsMessage, 0, 10)
	failedIds := make([]*string, 0)
	for e := r.pendingAck.Front(); e != nil; e = e.Next() {
		msg, err := r.tracker.Peek(e.Value.(*string))
		if err != nil {
			r.logger.Errorf("Failed to get message details during ack: %v", err.Error())
			return nil
		}

		batch = append(batch, msg)
		if len(batch) == 10 || e.Next() == nil {
			res, err := r.client.DeleteMessages(ctx, batch)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}

				r.logger.Errorf("Failed to delete messages during ack: %v", err.Error())
				return nil
			}

			// TODO deal with case where delete fails because we no longer have the current receipt handle
			for _, failure := range res.Failures {
				r.logger.Error(failure.Sprint()) // Log failed batch members
				failedIds = append(failedIds, failure.MsgId)
			}

			for _, success := range res.Successes {
				r.tracker.Ack(success.MsgId)
			}

			clear(batch)
		}
	}

	// Clear the pending list
	r.pendingAck = r.pendingAck.Init()
	// Add any failed acks back to the pending list
	for _, id := range failedIds {
		r.pendingAck.PushBack(id)
	}
	return nil
}

func (r *SqsFifoReader) nackLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()

nackLoop:
	for {
		select {
		// TODO do a better job of graceful shutdown - process until pending nacks is empty
		case <-r.lt.Killed():
			break nackLoop
		case <-t.C:
			if err := r.nack(r.lt.Ctx); err != nil {
				r.logger.Debug("Nack loop received kill order - breaking loop")
				break nackLoop
			}
		case <-r.ackCond.WaitChan():
			if err := r.nack(r.lt.Ctx); err != nil {
				r.logger.Debug("Nack loop received kill order - breaking loop")
				break nackLoop
			}
		}
	}
}

func (r *SqsFifoReader) nack(ctx context.Context) error {
	r.nackLock.Lock()
	defer r.nackLock.Unlock()

	r.logger.Debugf("Nack loop found %v nacks to process", r.pendingNack.Len())

	ids := make([]*string, 0, r.pendingNack.Len())
	for e := r.pendingNack.Front(); e != nil; e = e.Next() {
		id := e.Value.(*string)
		if _, err := r.tracker.Peek(id); err != nil {
			if ctxErr := r.lt.Ctx.Err(); ctxErr != nil {
				return err
			}

			r.logger.Errorf("Failed to get message details during nack: %v", err.Error())
			return nil
		}

		// We don't batch here for Nacks because the tracker will need to batch again anyway when resetting
		// message visibility for additional messages in the group
		ids = append(ids, id)
		err := r.tracker.Nack(ctx, ids...)
		if err != nil {
			if ctxErr := r.lt.Ctx.Err(); ctxErr != nil {
				return err
			}

			// This can't happen - Nack only returns an error if the context is cancelled, so just panic if this happens
			panic("Nack failed with an unknown error: " + err.Error())
		}
	}

	// Clear the pending list
	r.pendingNack = r.pendingNack.Init()
	return nil
}
