package models

import "time"

type InputConfig struct {
	VisibilityTimeout time.Duration // TODO move visibility timeout to a property based on in situ queue config
	QueueUrl          string
}
