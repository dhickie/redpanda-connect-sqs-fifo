package reading

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test/mocks"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test/wait"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/tracking"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAck_SignalsAckLoopToRun_WhenHittingMaxPendingAcks(t *testing.T) {
	// Assemble
	setupConf := func(config *models.InputConfig) {
		config.MaxPendingAcks = 2
	}
	reader := createReader(setupConf, nil)
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

func TestNack_SignalsNackLoopToRun_WhenHittingMaxPendingNacks(t *testing.T) {
	// Assemble
	setupConf := func(config *models.InputConfig) {
		config.MaxPendingAcks = 2
	}
	reader := createReader(setupConf, nil)
	waitFunc := func(ctx context.Context) (*struct{}, error) {
		err := reader.nackCond.Wait(ctx)
		return nil, err
	}

	// Act
	reader.Nack(randomId())
	s1, _, _ := test.TryWithTimeout(t.Context(), 10*time.Millisecond, waitFunc)
	reader.Nack(randomId())
	s2, _, _ := test.TryWithTimeout(t.Context(), 10*time.Millisecond, waitFunc)

	// Assert
	assert.Equal(t, false, s1, "The nack loop should not have been signalled")
	assert.Equal(t, true, s2, "The nack loop should have been signalled")
}

func TestReadLoop_PerformsRead_WhenTriggeredByReadCondition(t *testing.T) {
	// Assemble
	msgs := test.CreateMessages(1, 1)
	setupConf := func(c *models.InputConfig) {
		c.MaxInFlightMessages = 1
	}
	setupClient := func(c *mocks.MockSqsClient) {
		c.
			On("ReceiveMessages", mock.Anything, mock.Anything).
			Return(msgs, nil)
	}
	reader := createReader(setupConf, setupClient)

	// Act
	reader.Start()
	reader.readCond.Signal()
	reader.lt.Kill()
	msg, err := reader.Next(t.Context())

	// Act
	assert.NoError(t, err, "No error should have been returned from Next()")
	assert.EqualValues(t, msgs[0], msg, "The message should have been read")
}

func TestReadLoop_PerformsRead_WhenTriggeredByTimer(t *testing.T) {
	// Assemble
	msgs := test.CreateMessagesWithDeadline(1, 1, 30)
	setupConf := func(c *models.InputConfig) {
		c.MaxInFlightMessages = 2
		c.MinReceiveBatchSize = 1
		c.VisibilityTimeoutSeconds = 30
	}
	setupClient := func(c *mocks.MockSqsClient) {
		c.
			On("ReceiveMessages", mock.Anything, mock.Anything).
			Return(msgs, nil)
	}
	reader := createReader(setupConf, setupClient)

	// Act
	reader.Start()
	var msg *models.SqsMessage
	var err error
	waitFunc := func(ctx context.Context) (bool, error) {
		if _, iErr := reader.Next(ctx); iErr != nil { // Read the initial message read immediately after starting
			return false, iErr
		}
		if msg, err = reader.Next(ctx); err != nil { // Read the second message from the timer
			return false, err
		}

		return msg != nil, nil
	}
	_ = wait.Until(t.Context(), waitFunc, 2*time.Second) // Read the initial message
	reader.lt.Kill()

	// Act
	assert.NoError(t, err, "No error should have been returned from Next()")
	assert.EqualValues(t, msgs[0], msg, "The message should have been read")
}

func createReader(
	setupConf func(config *models.InputConfig),
	setupClient func(c *mocks.MockSqsClient)) *SqsFifoReader {

	client := new(mocks.MockSqsClient)
	if setupClient != nil {
		setupClient(client)
	}

	config := mocks.NewMockConfig()
	if setupConf != nil {
		setupConf(config)
	}

	lt := util.NewLifetime()
	tracker := tracking.NewMessageTracker(config, client, lt, nil)
	return NewSqsFifoReader(tracker, client, config, lt, nil)
}

func randomId() *string {
	id := uuid.NewV4().String()
	return &id
}
