package sqs_fifo

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/reader"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	metaKeyMsgId         = "sqs_message_id"
	metaKeyReceiptHandle = "sqs_receipt_handle"
	metaKeyReceiveCount  = "sqs_approximate_receive_count"
	metaKeyGroupId       = "sqs_message_group_id"

	attKeyReceiveCount = "ApproximateReceiveCount"
	attKeyGroupId      = "MessageGroupId"
)

// SqsFifoInput is the top level object that interacts with the Connect SDK
type SqsFifoInput struct {
	reader  *reader.SqsFifoReader
	ackChan chan *string
}

// NewSqsFifoInput returns a new input object, ready for connecting to SQS
func NewSqsFifoInput(conf *models.InputConfig) *SqsFifoInput {
	return &SqsFifoInput{
		reader:  reader.NewSqsFifoReader(conf),
		ackChan: make(chan *string),
	}
}

// ConnectionTest tests that the input can reach the target queue successfully by running a health check
func (i *SqsFifoInput) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	err := i.reader.Healthcheck() // TODO pass in ctx here
	if err != nil {
		return service.ConnectionTestFailed(err).AsList()
	}

	return service.ConnectionTestSucceeded().AsList()
}

// Connect starts the input by starting the reader and ack/nack callback loop
func (i *SqsFifoInput) Connect(context.Context) error {
	i.reader.Start()
	go i.callbackLoop()

	return nil
}

// Read gets the next message to be processed by the pipeline, and a callback function to call once it has been
// fully processed
func (i *SqsFifoInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	sqsMsg := i.reader.Next()

	sMsg := service.NewMessage([]byte(*sqsMsg.Msg.Body))
	addSQSMetadata(sMsg, sqsMsg)

	ackFunc := func(pCtx context.Context, err error) error {
		if err != nil {
			// TODO handle nack
		}

		select {
		case <-pCtx.Done():
			return pCtx.Err()
		case i.ackChan <- sqsMsg.Msg.MessageId:
		}
		return nil
	}

	return sMsg, ackFunc, nil
}

// Close closes the connection to the SQS queue
func (i *SqsFifoInput) Close(ctx context.Context) error {
	// TODO gracefully stop the goroutine loops
	return nil
}

func (i *SqsFifoInput) callbackLoop() {
	for {
		select {
		case mId := <-i.ackChan:
			i.reader.Ack(mId)
			// TODO add nack channel
		}
	}
}

func addSQSMetadata(sMsg *service.Message, sqsMsg *models.SqsMessage) {
	sMsg.MetaSetMut(metaKeyMsgId, *sqsMsg.Msg.MessageId)
	sMsg.MetaSetMut(metaKeyReceiptHandle, *sqsMsg.Msg.ReceiptHandle)

	addAttributeMetadataIfNotNil(sMsg, metaKeyReceiveCount, attKeyReceiveCount, sqsMsg.Msg.Attributes)
	addAttributeMetadataIfNotNil(sMsg, metaKeyGroupId, attKeyGroupId, sqsMsg.Msg.Attributes)

	for k, v := range sqsMsg.Msg.MessageAttributes {
		if v.StringValue != nil {
			sMsg.MetaSetMut(k, *v.StringValue)
		}
	}
}

func addAttributeMetadataIfNotNil(msg *service.Message, mKey string, aKey string, attributes map[string]string) {
	if v, ok := attributes[aKey]; ok {
		msg.MetaSetMut(mKey, v)
	}
}
