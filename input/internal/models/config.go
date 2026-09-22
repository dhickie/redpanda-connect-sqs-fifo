package models

type InputConfig struct {
	QueueUrl                 string
	VisibilityTimeoutSeconds int
	MinReceiveBatchSize      int
	MaxReceiveBatchSize      int
	MaxInFlightMessages      int
	MaxProcessingAttempts    int
	MaxPendingAcks           int
}
