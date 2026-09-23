package tracking

import (
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/mocks"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"
	"testing"
	"uuid"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
)

func TestAdd_ReturnsCorrectNumberOfInflightMessages(t *testing.T) {
	// Arrange
	tracker := createTracker()
	msgs := createMessages()

	// Act
	n := tracker.Add(msgs)

	// Assert
	assert.Equal(t, len(msgs), n, "The number of inflight messages should be the same as the number of messages added")
}

func createTracker() *MessageTracker {
	conf := mocks.NewMockConfig()
	sqs := new(mocks.MockSqsClient)
	lt := util.NewLifetime()

	return NewMessageTracker(conf, sqs, lt, nil)
}

func createMessages() []*models.SqsMessage {
	var msgs []*models.SqsMessage

	for i := 0; i < 4; i++ {
		msgId := uuid.NewV4().String()

		for j := 0; j < 4; j++ {
			gId := string(rune(j))
			rawMsg := types.Message{
				Attributes: map[string]string{
					"MessageGroupId": gId,
				},
				Body:      &msgId,
				MessageId: &msgId,
			}
			msg := models.NewSqsMessage(rawMsg, 30)
			msgs = append(msgs, msg)
		}
	}

	return msgs
}
