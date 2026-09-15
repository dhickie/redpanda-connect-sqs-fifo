// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
