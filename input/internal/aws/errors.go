package aws

import "fmt"

type BatchSizeError struct {
	attemptedSize int    // The size of batch requested by the caller
	operation     string // The operation being requested
}

func newBatchSizeError(attemptedSize int, operation string) error {
	return &BatchSizeError{
		attemptedSize: attemptedSize,
		operation:     operation,
	}
}

func (e *BatchSizeError) Error() string {
	return fmt.Sprintf(
		"Cannot perform %v operation with batch size of %v - maximum is 10",
		e.operation,
		e.attemptedSize)
}
