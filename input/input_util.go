package sqs_fifo

import (
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func addSQSMetadata(p *service.Message, sqsMsg types.Message) {
	p.MetaSetMut("sqs_message_id", *sqsMsg.MessageId)
	p.MetaSetMut("sqs_receipt_handle", *sqsMsg.ReceiptHandle)
	if rCountStr, exists := sqsMsg.Attributes["ApproximateReceiveCount"]; exists {
		p.MetaSetMut("sqs_approximate_receive_count", rCountStr)
	}
	for k, v := range sqsMsg.MessageAttributes {
		if v.StringValue != nil {
			p.MetaSetMut(k, *v.StringValue)
		}
	}
}

func awsErrIsTimeout(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		(err != nil && strings.HasSuffix(err.Error(), "context canceled"))
}
