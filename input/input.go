package sqs_fifo

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/reader"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type SqsFifoInput struct {
	reader *reader.SqsFifoReader
}

func NewSqsFifoInput(conf *models.InputConfig) *SqsFifoInput {
	return &SqsFifoInput{
		reader: reader.NewSqsFifoReader(conf),
	}
}

func (i *SqsFifoInput) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	err := i.reader.Healthcheck() // TODO pass in ctx here
	if err != nil {
		return service.ConnectionTestFailed(err).AsList()
	}

	return service.ConnectionTestSucceeded().AsList()
}

func (i *SqsFifoInput) Connect(context.Context) error {
	i.reader.Start()
	return nil
}

func (i *SqsFifoInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	sqsMsg := i.reader.Next()

	sMsg := service.NewMessage([]byte(*sqsMsg.Msg.Body))
	addSQSMetadata(sMsg, sqsMsg)
	// TODO add ack callback
}

// TODO add Close function

func addSQSMetadata(sMsg *service.Message, sqsMsg *models.SqsMessage) {
	sMsg.MetaSetMut("sqs_message_id", *sqsMsg.Msg.MessageId)
	sMsg.MetaSetMut("sqs_receipt_handle", *sqsMsg.Msg.ReceiptHandle)
	// TODO add FIFO message attributes

	if count, ok := sqsMsg.Msg.Attributes["ApproximateReceiveCount"]; ok {
		sMsg.MetaSetMut("sqs_approximate_receive_count", count)
	}

	for k, v := range sqsMsg.Msg.MessageAttributes {
		if v.StringValue != nil {
			sMsg.MetaSetMut(k, *v.StringValue)
		}
	}
}
