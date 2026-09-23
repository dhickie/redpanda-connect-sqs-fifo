package reading

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/mocks"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
)

func TestAck_SignalsAckLoopToRun_WhenHittingMaxPendingAcks(t *testing.T) {
	// Assemble
	setupConf := func(config *models.InputConfig) {
		config.MaxPendingAcks = 2
	}
	reader := createReader(setupConf)
	waitFunc := func(ctx context.Context) (*struct{}, error) {
		err := reader.ackCond.Wait(ctx)
		return nil, err
	}

	// Act
	reader.Ack(randomId())
	s1, _, _ := test.TryWithTimeout(t.Context(), 10*time.Millisecond, waitFunc)
	reader.Ack(randomId())
	s2, _, _ := test.TryWithTimeout(t.Context(), 10*time.Millisecond, waitFunc)

	// Assert
	assert.Equal(t, false, s1, "The ack loop should not have been signalled")
	assert.Equal(t, true, s2, "The ack loop should have been signalled")
}

func createReader(setupConf func(config *models.InputConfig)) *SqsFifoReader {
	tracker := new(mocks.MockMessageTracker)
	client := new(mocks.MockSqsClient)

	config := mocks.NewMockConfig()
	setupConf(config)

	lt := util.NewLifetime()

	return NewSqsFifoReader(tracker, client, config, lt, nil)
}

func randomId() *string {
	id := uuid.NewV4().String()
	return &id
}
