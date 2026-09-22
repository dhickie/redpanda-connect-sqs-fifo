package models

type InputConfig struct {
	QueueUrl                 string
	BaseEndpoint             string
	VisibilityTimeoutSeconds int
	MinReceiveBatchSize      int
	MaxReceiveBatchSize      int
	MaxInFlightMessages      int
	MaxProcessingAttempts    int
	MaxPendingAcks           int
}
