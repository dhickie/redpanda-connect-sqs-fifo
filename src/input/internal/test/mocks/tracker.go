package mocks

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"

	"github.com/stretchr/testify/mock"
)

type MockMessageTracker struct {
	mock.Mock
}

func (m *MockMessageTracker) Start() {
}

func (m *MockMessageTracker) Add(msgs []*models.SqsMessage) int {
	args := m.Called(msgs)
	return args.Int(0)
}

func (m *MockMessageTracker) Peek(id *string) (*models.SqsMessage, error) {
	args := m.Called(id)
	return args.Get(0).(*models.SqsMessage), args.Error(1)
}

func (m *MockMessageTracker) Flush(ctx context.Context) (*models.SqsMessage, error) {
	args := m.Called(ctx)
	return args.Get(0).(*models.SqsMessage), args.Error(1)
}

func (m *MockMessageTracker) Ack(id string) {
}

func (m *MockMessageTracker) Nack(ctx context.Context, ids ...*string) error {
	args := m.Called(ctx, ids)
	return args.Error(0)
}

func (m *MockMessageTracker) Length() int {
	args := m.Called()
	return args.Int(0)
}
