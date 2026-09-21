package aws

import "fmt"

// BatchItemFailure represents a failed item in a batch operation
type BatchItemFailure struct {
	operation   *string // What operation was being attempted for the batch
	batchId     *string // An ID for the batch in which this failure occurred
	msgId       *string // The message ID the failure occurred for
	error       *string // The error that occurred
	senderError bool    // Whether the error was the fault of the sender
}

func newBatchItemFailure(
	operation string,
	batchId string,
	msgId *string,
	error *string,
	senderError bool) *BatchItemFailure {
	return &BatchItemFailure{
		operation:   &operation,
		batchId:     &batchId,
		msgId:       msgId,
		error:       error,
		senderError: senderError,
	}
}

// Sprint prints the error to a formatted string for logging
func (f *BatchItemFailure) Sprint() string {
	var blame string
	if f.senderError {
		blame = "client"
	} else {
		blame = "server"
	}

	return fmt.Sprintf("A %v error occurred in batch %v while performing a %v operation for message ID %v: %v",
		blame, f.batchId, f.operation, f.msgId, f.error)
}
