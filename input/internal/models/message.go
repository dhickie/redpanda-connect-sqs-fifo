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

package models

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	messageGroupId string = "MessageGroupId"
)

type SqsMessage struct {
	Msg      types.Message // The actual SQS message
	Deadline time.Time     // The deadline after which the message will be visible to other clients
}

func (msg *SqsMessage) GetGroupId() string {
	id, ok := msg.Msg.Attributes[messageGroupId]
	if !ok {
		panic("Fatal: No message group ID found on message")
	}

	return id
}

func (msg *SqsMessage) RemainingDeadline() time.Duration {
	return msg.Deadline.Sub(time.Now())
}
