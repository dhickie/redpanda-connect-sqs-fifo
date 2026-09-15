package sqs_fifo

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type sqsAPI interface {
	GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibilityBatch(context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
	SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

type awsSQSReader struct {
	conf sqsiConfig

	aconf aws.Config
	sqs   sqsAPI

	messagesChan     chan sqsMessage
	ackMessagesChan  chan *sqsMessageHandle
	nackMessagesChan chan *sqsMessageHandle
	closeSignal      *shutdown.Signaller

	log *service.Logger
}

func newAWSSQSReader(conf sqsiConfig, aconf aws.Config, log *service.Logger) (*awsSQSReader, error) {
	return &awsSQSReader{
		conf:             conf,
		aconf:            aconf,
		log:              log,
		messagesChan:     make(chan sqsMessage),
		ackMessagesChan:  make(chan *sqsMessageHandle),
		nackMessagesChan: make(chan *sqsMessageHandle),
		closeSignal:      shutdown.NewSignaller(),
	}, nil
}

// ConnectionTest attempts to test the connection configuration of this input
// without actually consuming data. The connection, if successful, is then
// closed.
func (a *awsSQSReader) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	client := sqs.NewFromConfig(a.aconf)
	_, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(a.conf.URL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return service.ConnectionTestFailed(fmt.Errorf("getting queue attributes: %w", err)).AsList()
	}
	return service.ConnectionTestSucceeded().AsList()
}

// Connect attempts to establish a connection to the target SQS
// queue.
func (a *awsSQSReader) Connect(context.Context) error {
	if a.sqs == nil {
		a.sqs = sqs.NewFromConfig(a.aconf)
	}

	ift := &sqsInFlightTracker{
		handles: map[string]*list.Element{},
		fifo:    list.New(),
		limit:   a.conf.MaxOutstanding,
		timeout: a.conf.MessageTimeout,
	}
	ift.l = sync.NewCond(&ift.m)

	var wg sync.WaitGroup
	wg.Add(3)
	go a.readLoop(&wg, ift)
	go a.ackLoop(&wg, ift)
	go a.refreshLoop(&wg, ift)
	go func() {
		wg.Wait()
		a.closeSignal.TriggerHasStopped()
	}()
	return nil
}

// ReadBatch attempts to read a new message from the target SQS.
func (a *awsSQSReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if a.sqs == nil {
		return nil, nil, service.ErrNotConnected
	}

	var next sqsMessage
	var open bool
	select {
	case next, open = <-a.messagesChan:
		if !open {
			return nil, nil, service.ErrEndOfInput
		}
	case <-a.closeSignal.SoftStopChan():
		return nil, nil, service.ErrEndOfInput
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	if next.Body == nil {
		return nil, nil, context.Canceled
	}

	msg := service.NewMessage([]byte(*next.Body))
	addSQSMetadata(msg, next.Message)
	mHandle := next.handle
	return msg, func(rctx context.Context, res error) error {
		if mHandle == nil {
			return nil
		}
		if res == nil {
			select {
			case <-rctx.Done():
				return rctx.Err()
			case <-a.closeSignal.SoftStopChan():
				return a.deleteMessages(rctx, mHandle)
			case a.ackMessagesChan <- mHandle:
			}
			return nil
		}

		select {
		case <-rctx.Done():
			return rctx.Err()
		case <-a.closeSignal.SoftStopChan():
			return a.resetMessages(rctx, mHandle)
		case a.nackMessagesChan <- mHandle:
		}
		return nil
	}, nil
}

func (a *awsSQSReader) Close(ctx context.Context) error {
	a.closeSignal.TriggerSoftStop()

	var closeNowAt time.Duration
	if dline, ok := ctx.Deadline(); ok {
		if closeNowAt = time.Until(dline) - time.Second; closeNowAt <= 0 {
			a.closeSignal.TriggerHardStop()
		}
	}
	if closeNowAt > 0 {
		select {
		case <-time.After(closeNowAt):
			a.closeSignal.TriggerHardStop()
		case <-ctx.Done():
			return ctx.Err()
		case <-a.closeSignal.HasStoppedChan():
			return nil
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closeSignal.HasStoppedChan():
	}
	return nil
}

func (a *awsSQSReader) deleteMessages(ctx context.Context, msgs ...*sqsMessageHandle) error {
	if !a.conf.DeleteMessage {
		return nil
	}
	const maxBatchSize = 10
	for len(msgs) > 0 {
		input := sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(a.conf.URL),
			Entries:  []types.DeleteMessageBatchRequestEntry{},
		}

		for i := range msgs {
			msg := msgs[i]
			input.Entries = append(input.Entries, types.DeleteMessageBatchRequestEntry{
				Id:            &msg.id,
				ReceiptHandle: &msg.receiptHandle,
			})
			if len(input.Entries) == maxBatchSize {
				break
			}
		}

		msgs = msgs[len(input.Entries):]
		response, err := a.sqs.DeleteMessageBatch(ctx, &input)
		if err != nil {
			return err
		}
		for _, fail := range response.Failed {
			msg := "(no message)"
			if fail.Message != nil {
				msg = *fail.Message
			}
			a.log.Errorf("Failed to delete consumed SQS message '%v', response code: %v, message: %q, sender fault: %v", *fail.Id, *fail.Code, msg, fail.SenderFault)
		}
	}
	return nil
}

func (a *awsSQSReader) resetMessages(ctx context.Context, msgs ...*sqsMessageHandle) error {
	if !a.conf.ResetVisibility {
		return nil
	}
	return a.updateVisibilityMessages(ctx, 0, msgs...)
}

func (a *awsSQSReader) updateVisibilityMessages(ctx context.Context, timeout int, msgs ...*sqsMessageHandle) error {
	const maxBatchSize = 10
	batchError := &batchUpdateVisibilityError{}
	for len(msgs) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(a.conf.URL),
			Entries:  []types.ChangeMessageVisibilityBatchRequestEntry{},
		}

		for i := range msgs {
			msg := msgs[i]
			input.Entries = append(input.Entries, types.ChangeMessageVisibilityBatchRequestEntry{
				Id:                &msg.id,
				ReceiptHandle:     &msg.receiptHandle,
				VisibilityTimeout: int32(timeout),
			})
			if len(input.Entries) == maxBatchSize {
				break
			}
		}

		msgs = msgs[len(input.Entries):]
		if len(input.Entries) == 0 {
			continue
		}
		response, err := a.sqs.ChangeMessageVisibilityBatch(ctx, &input)
		if err != nil {
			return err
		}
		if len(response.Failed) != 0 {
			batchError.entries = append(batchError.entries, response.Failed...)
		}
	}
	if len(batchError.entries) > 0 {
		return batchError
	}
	return nil
}

func (a *awsSQSReader) ackLoop(wg *sync.WaitGroup, inFlightTracker *sqsInFlightTracker) {
	defer wg.Done()
	defer inFlightTracker.Clear()

	closeNowCtx, done := a.closeSignal.HardStopCtx(context.Background())
	defer done()

	flushFinishedHandles := func(handles []*sqsMessageHandle, erase bool) {
		if len(handles) == 0 {
			return
		}
		seen := make(map[string]bool, len(handles))
		// deduplicate handles, unlikely that there are duplicates, so this is defensive.
		handles = slices.DeleteFunc(handles, func(h *sqsMessageHandle) bool {
			if seen[h.id] {
				return true
			}
			seen[h.id] = true
			return false
		})
		if erase {
			if err := a.deleteMessages(closeNowCtx, handles...); err != nil {
				a.log.Errorf("Failed to delete messages: %v", err)
			}
		} else {
			if err := a.resetMessages(closeNowCtx, handles...); err != nil {
				// Downgrade this to Info level - it's not really an error, it's just going to take longer
				// to reset the visibility so the messages might be delayed is all. It's possible for delays
				// if this succeeds anyways as it might be racing with the refresh loop. Fixing that
				// would mean moving nacks to the refresh loop, but I don't think this will be a big deal in
				// practice.
				a.log.Infof("Failed to reset the visibility timeout of messages: %v", err)
			}
		}
	}

	flushTimer := time.NewTicker(time.Second)
	defer flushTimer.Stop()

	pendingAcks := []*sqsMessageHandle{}
	pendingNacks := []*sqsMessageHandle{}

ackLoop:
	for {
		select {
		case h := <-a.ackMessagesChan:
			pendingAcks = append(pendingAcks, h)
			inFlightTracker.Remove(h.id)
			if len(pendingAcks) >= a.conf.MaxNumberOfMessages {
				flushFinishedHandles(pendingAcks, true)
				pendingAcks = pendingAcks[:0]
			}
		case h := <-a.nackMessagesChan:
			pendingNacks = append(pendingNacks, h)
			inFlightTracker.Remove(h.id)
			if len(pendingNacks) >= a.conf.MaxNumberOfMessages {
				flushFinishedHandles(pendingNacks, false)
				pendingNacks = pendingNacks[:0]
			}
		case <-flushTimer.C:
			flushFinishedHandles(pendingAcks, true)
			pendingAcks = pendingAcks[:0]
			flushFinishedHandles(pendingNacks, false)
			pendingNacks = pendingNacks[:0]
		case <-a.closeSignal.SoftStopChan():
			break ackLoop
		}
	}

	flushFinishedHandles(pendingAcks, true)
	flushFinishedHandles(pendingNacks, false)
}

func (a *awsSQSReader) refreshLoop(wg *sync.WaitGroup, inFlightTracker *sqsInFlightTracker) {
	defer wg.Done()
	closeNowCtx, done := a.closeSignal.HardStopCtx(context.Background())
	defer done()
	refreshCurrentHandles := func() {
		for !a.closeSignal.IsSoftStopSignalled() {
			// updateVisibilityMessages can only make an API request with 10 messages at most, so grab 10 then refresh to prevent
			// an issue where we grab a ton of messages and they are acked before we actual make the API call. Note that this scenario
			// can still happen because we refresh async with acking, but this makes it a lot less likely.
			currentHandles := inFlightTracker.PullToRefresh(10)
			if len(currentHandles) == 0 {
				// There is nothing to refresh, return and sleep for a second
				return
			}
			err := a.updateVisibilityMessages(closeNowCtx, int(a.conf.MessageTimeout.Seconds()), currentHandles...)
			if err == nil {
				continue
			}
			partialErr := &batchUpdateVisibilityError{}
			if errors.As(err, &partialErr) {
				for _, fail := range partialErr.entries {
					// Mitigate erroneous log statements due to the race described above by making sure we're still tracking the message
					if !inFlightTracker.IsTracking(*fail.Id) {
						continue
					}
					msg := "(no message)"
					if fail.Message != nil {
						msg = *fail.Message
					}
					a.log.Debugf("Failed to update SQS message '%v', response code: %v, message: %q, sender fault: %v", *fail.Id, *fail.Code, msg, fail.SenderFault)
				}
			} else {
				a.log.Debugf("Failed to update messages visibility timeout: %v", err)
			}
		}
	}

	for {
		select {
		case <-time.After(time.Second):
			refreshCurrentHandles()
		case <-a.closeSignal.SoftStopChan():
			return
		}
	}
}

func (a *awsSQSReader) readLoop(wg *sync.WaitGroup, inFlightTracker *sqsInFlightTracker) {
	defer wg.Done()

	var pendingMsgs []sqsMessage
	defer func() {
		if len(pendingMsgs) > 0 {
			tmpNacks := make([]*sqsMessageHandle, 0, len(pendingMsgs))
			for _, m := range pendingMsgs {
				if m.handle == nil {
					continue
				}
				tmpNacks = append(tmpNacks, m.handle)
			}
			ctx, done := a.closeSignal.HardStopCtx(context.Background())
			defer done()
			if err := a.resetMessages(ctx, tmpNacks...); err != nil {
				a.log.Errorf("Failed to reset visibility timeout for pending messages: %v", err)
			}
		}
	}()

	closeAtLeisureCtx, done := a.closeSignal.SoftStopCtx(context.Background())
	defer done()

	readBackoff := backoff.NewExponentialBackOff()
	readBackoff.InitialInterval = 10 * time.Millisecond
	readBackoff.MaxInterval = time.Minute
	readBackoff.MaxElapsedTime = 0

	getMsgs := func() {
		res, err := a.sqs.ReceiveMessage(closeAtLeisureCtx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(a.conf.URL),
			MaxNumberOfMessages:   int32(a.conf.MaxNumberOfMessages),
			WaitTimeSeconds:       int32(a.conf.WaitTimeSeconds),
			AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
			VisibilityTimeout:     int32(a.conf.MessageTimeout.Seconds()),
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			if !awsErrIsTimeout(err) {
				a.log.Errorf("Failed to pull new SQS messages: %v", err)
			}
			return
		}
		if len(res.Messages) > 0 {
			for _, msg := range res.Messages {
				var handle *sqsMessageHandle
				if msg.MessageId != nil && msg.ReceiptHandle != nil {
					handle = &sqsMessageHandle{
						id:            *msg.MessageId,
						receiptHandle: *msg.ReceiptHandle,
						deadline:      time.Now().Add(a.conf.MessageTimeout),
					}
				}
				pendingMsgs = append(pendingMsgs, sqsMessage{
					Message: msg,
					handle:  handle,
				})
			}
			inFlightTracker.AddNew(closeAtLeisureCtx, pendingMsgs[len(pendingMsgs)-len(res.Messages):]...)
		}
		if len(res.Messages) > 0 || a.conf.WaitTimeSeconds > 0 {
			// When long polling we want to reset our back off even if we didn't
			// receive messages. However, with long polling disabled we back off
			// each time we get an empty response.
			readBackoff.Reset()
		}
	}

	for {
		if len(pendingMsgs) == 0 {
			getMsgs()
			if len(pendingMsgs) == 0 {
				select {
				case <-time.After(readBackoff.NextBackOff()):
				case <-a.closeSignal.SoftStopChan():
					return
				}
				continue
			}
		}
		select {
		case a.messagesChan <- pendingMsgs[0]:
			pendingMsgs = pendingMsgs[1:]
		case <-a.closeSignal.SoftStopChan():
			return
		}
	}
}
