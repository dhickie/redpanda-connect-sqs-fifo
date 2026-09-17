package aws

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	maxBatchSize = 10
)

// SqsClient provides access to functions of the SQS API
type SqsClient struct {
	client *sqs.Client
	conf   *models.InputConfig
}

// NewSqsClient returns a new client using the provided configuration
func NewSqsClient(conf models.InputConfig) (*SqsClient, error) {
	return &SqsClient{
		client: sqs.NewFromConfig(*aws.NewConfig()),
	}, nil
}

// SetMessageVisibility updates the visibility of between 1 and 10 messages to a time in the future
func (c *SqsClient) SetMessageVisibility(ctx context.Context, newTimeoutSeconds int32, msgs ...*models.SqsMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	if len(msgs) > maxBatchSize {
		// TODO return an error
	}

	req := sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: &c.conf.QueueUrl,
		Entries:  []types.ChangeMessageVisibilityBatchRequestEntry{},
	}
	for _, msg := range msgs {
		entry := types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                msg.Msg.MessageId,
			ReceiptHandle:     msg.Msg.ReceiptHandle,
			VisibilityTimeout: newTimeoutSeconds,
		}
		req.Entries = append(req.Entries, entry)
	}

	_, err := c.client.ChangeMessageVisibilityBatch(ctx, &req)
	if err != nil {
		return err
	}

	// TODO check to see if any bits of the batch failed
	return nil
}

// ReceiveMessages pulls a batch of messages from the queue
func (c *SqsClient) ReceiveMessages(ctx context.Context, maxMsgs int32) ([]*models.SqsMessage, error) {
	req := sqs.ReceiveMessageInput{
		QueueUrl:            &c.conf.QueueUrl,
		MaxNumberOfMessages: maxMsgs,
	}

	res, err := c.client.ReceiveMessage(ctx, &req)
	if err != nil {
		return nil, err
	}

	msgs := make([]*models.SqsMessage, len(res.Messages))
	for i, rawMsg := range res.Messages {
		msgs[i] = &models.SqsMessage{
			Msg:      rawMsg,
			Deadline: time.Now().Add(c.conf.VisibilityTimeout),
		}
	}

	return msgs, nil
}

func (c *SqsClient) DeleteMessages(ctx context.Context, msgs []*models.SqsMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	if len(msgs) > maxBatchSize {
		// TODO return an error here
	}

	req := sqs.DeleteMessageBatchInput{
		QueueUrl: &c.conf.QueueUrl,
		Entries:  make([]types.DeleteMessageBatchRequestEntry, len(msgs)),
	}
	for i, msg := range msgs {
		entry := types.DeleteMessageBatchRequestEntry{
			Id:            msg.Msg.MessageId,
			ReceiptHandle: msg.Msg.ReceiptHandle,
		}
		req.Entries[i] = entry
	}

	_, err := c.client.DeleteMessageBatch(ctx, &req)
	if err != nil {
		return err
	}

	// TODO check for failed batch members
	return nil
}
