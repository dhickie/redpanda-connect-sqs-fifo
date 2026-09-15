package sqs_fifo

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type sqsMessage struct {
	types.Message
	handle *sqsMessageHandle
}

type sqsMessageHandle struct {
	id, receiptHandle string
	// The timestamp of when the message expires
	deadline time.Time
}
