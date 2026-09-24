//go:build stress

package sqs_fifo

import (
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test/mocks"
	"math/rand/v2"

	"github.com/stretchr/testify/mock"
)

const (
	NumMessages = 1000
	MinPerGroup = 1
	MaxPerGroup = 10
)

var testData []*models.SqsMessage

func TestInput_UnderHighLoad(t *testing.T) {
	// Setup test data
	generateTestData()
}

func generateTestData() {
	testData = make([]*models.SqsMessage, 0, NumMessages)
	for {
		nMsgs := randRange(MinPerGroup, MaxPerGroup)

		if nMsgs > NumMessages-len(testData) {
			nMsgs = NumMessages - len(testData)
		}

		testData = append(testData, test.CreateMessages(1, nMsgs))

		if len(testData) == NumMessages {
			break
		}
	}
}

func randRange(min, max int) int {
	return min + rand.IntN(max-min)
}

func createInput(confFunc func(*models.InputConfig), sqsFunc func(*mocks.MockSqsClient)) *SqsFifoInput {
	config := mocks.NewMockConfig()
	if confFunc != nil {
		confFunc(config)
	}

	var rCall *mock.Call
	rCallback := func(args mock.Arguments) {
		n := args.Int(1)
		nextMsgs := test.CreateMessages(1, n)
		rCall.ReturnArguments = mock.Arguments{nextMsgs, nil}
	}
	var dCall *mock.Call
	dCallback := func(args mock.Arguments) {
		msgs := args.Get(1).([]*models.SqsMessage)
		res := test.BatchSuccessResult(msgs)
		dCall.ReturnArguments = mock.Arguments{res, nil}
	}
	var vCall *mock.Call
	vCallback := func(args mock.Arguments) {
		msgs := args.Get(2).([]*models.SqsMessage)
		res := test.BatchSuccessResult(msgs)
		vCall.ReturnArguments = mock.Arguments{res, nil}
	}
	client := new(mocks.MockSqsClient)
	rCall = client.On("ReceiveMessages", mock.Anything, mock.Anything).Run(rCallback).Return(nil, nil)
	dCall = client.On("DeleteMessages", mock.Anything, mock.Anything).Run(dCallback).Return(nil, nil)
	vCall = client.On("SetMessageVisibility", mock.Anything, mock.Anything, mock.Anything).Run(vCallback).Return(nil, nil)
	client.On("GetQueueVisibilityTimeout", mock.Anything).Return(30, nil)

	if sqsFunc != nil {
		sqsFunc(client)
	}

	return NewSqsFifoInput(client, config, nil)
}
