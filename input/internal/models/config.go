package models

import "time"

type InputConfig struct {
	QueueUrl            string
	VisibilityTimeout   time.Duration // TODO move visibility timeout to a property based on in situ queue config
	MinReceiveBatchSize int
	MaxReceiveBatchSize int
	MaxInFlightMessages int
}
