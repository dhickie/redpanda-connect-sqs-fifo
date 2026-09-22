package models

type InputConfig struct {
	QueueUrl                 string
	VisibilityTimeoutSeconds int // TODO move visibility timeout to a property based on in situ queue config
	MinReceiveBatchSize      int
	MaxReceiveBatchSize      int
	MaxInFlightMessages      int
	MaxProcessingAttempts    int
	MaxPendingAcks           int
}
