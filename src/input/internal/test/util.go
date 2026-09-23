package test

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"
	"strconv"
	"time"
	"uuid"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func CreateMessages(nGroups, nMsgs int) []*models.SqsMessage {
	var msgs []*models.SqsMessage

	for range nMsgs {
		msgId := uuid.NewV4().String()

		for j := range nGroups {
			gId := strconv.Itoa(j)
			rawMsg := types.Message{
				Attributes: map[string]string{
					"MessageGroupId": gId,
				},
				Body:      &msgId,
				MessageId: &msgId,
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

func TryWithTimeout[T any](parentCtx context.Context, timeout time.Duration, f func(context.Context) (T, error)) (bool, *T, error) {
	ctx, cancel := context.WithDeadline(parentCtx, time.Now().Add(timeout))
	defer cancel()

	vC := make(chan T, 1)
	eC := make(chan error, 1)

	go func() {
		res, err := f(ctx)
		if ctx.Err() != nil {
			return
		}

		vC <- res
		eC <- err
	}()

	select {
	case <-ctx.Done():
		return false, nil, nil
	case res := <-vC:
		return true, &res, <-eC
	}
}
