package test

import (
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"
	"time"
	"uuid"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func CreateMessages(nGroups, nMsgs int) []*models.SqsMessage {
	var msgs []*models.SqsMessage
	gIds := make([]string, 0, nGroups)
	for range nGroups {
		gIds = append(gIds, uuid.New().String())
	}

	for range nMsgs {
		msgId := uuid.NewV4().String()

		for j := range nGroups {
			gId := gIds[j]
			receiptHandle := uuid.New().String()
			rawMsg := types.Message{
				Attributes: map[string]string{
					"MessageGroupId": gId,
				},
				Body:          &msgId,
				MessageId:     &msgId,
				ReceiptHandle: &receiptHandle,
			}
			msg := models.NewSqsMessage(rawMsg, 2)
			msgs = append(msgs, msg)
		}
	}

	return msgs
}

func CreateMessagesWithDeadline(nGroups, nMsgs, deadlineSeconds int) []*models.SqsMessage {
	msgs := CreateMessages(nGroups, nMsgs)

	util.Select(msgs, func(m *models.SqsMessage) *models.SqsMessage {
		m.Deadline = time.Now().Add(time.Duration(deadlineSeconds) * time.Second)
		return m
	})

	return msgs
}

func BatchSuccessResult(msgs []*models.SqsMessage) *aws.BatchOpResult {
	ids := util.Select(msgs, func(m *models.SqsMessage) *aws.BatchItemSuccess {
		return &aws.BatchItemSuccess{
			MsgId: *m.Msg.MessageId,
		}
	})
	return &aws.BatchOpResult{
		Successes: ids,
		Failures:  []*aws.BatchItemFailure{},
	}
}
