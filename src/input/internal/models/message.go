package models

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	messageGroupId string = "MessageGroupId"
)

type SqsMessage struct {
	Msg       types.Message // The actual SQS message
	Deadline  time.Time     // The deadline after which the message will be visible to other clients
	AttemptNo int           // The number of the current attempt to process the message
}

func NewSqsMessage(msg types.Message, deadlineSeconds int) *SqsMessage {
	return &SqsMessage{
		Msg:       msg,
		Deadline:  time.Now().Add(time.Duration(deadlineSeconds) * time.Second),
		AttemptNo: 1,
	}
}

func (msg *SqsMessage) GetGroupId() string {
	id, ok := msg.Msg.Attributes[messageGroupId]
	if !ok {
		panic("Fatal: No message group ID found on message")
	}

	return id
}

func (msg *SqsMessage) RemainingDeadline() int {
	return int(msg.Deadline.Sub(time.Now()).Seconds())
}

func (msg *SqsMessage) RefreshDeadline(extensionSeconds int) {
	msg.Deadline = time.Now().Add(time.Duration(extensionSeconds) * time.Second)
}
