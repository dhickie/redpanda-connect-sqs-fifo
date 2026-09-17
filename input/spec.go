package sqs_fifo

import (
	"dhickie/redpanda-connect-sqs-fifo/input/internal/models"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Input configuration fields
const (
	confFieldUrl                 = "url"
	confFieldVisibilityTimeout   = "visibility_timeout"
	confFieldMinReceiveBatchSize = "min_receive_batch_size"
	confFieldMaxReceiveBatchSize = "max_receive_batch_size"
	confFieldMaxInFlightMessages = "max_in_flight_messages"
)

func inputConfigFromConnectConfig(cConfig *service.ParsedConfig) (*models.InputConfig, error) {
	conf := &models.InputConfig{}
	var err error

	if conf.QueueUrl, err = cConfig.FieldString(confFieldUrl); err != nil {
		return nil, err
	}
	if conf.MinReceiveBatchSize, err = cConfig.FieldInt(confFieldMinReceiveBatchSize); err != nil {
		return nil, err
	}
	if conf.MaxReceiveBatchSize, err = cConfig.FieldInt(confFieldMaxReceiveBatchSize); err != nil {
		return nil, err
	}
	if conf.MaxInFlightMessages, err = cConfig.FieldInt(confFieldMaxInFlightMessages); err != nil {
		return nil, err
	}

	t, err := cConfig.FieldInt(confFieldVisibilityTimeout)
	if err != nil {
		return nil, err
	}
	conf.VisibilityTimeout = time.Duration(t) * time.Second

	return conf, nil
}

func sqsFifoInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "AWS").
		Summary(`Consume messages from a AWS FIFO SQS queue, preserving message ordering`).
		Description(`
		== Credentials
		
		TODO fill out details on credentials
		
		== Metadata
		
		This input adds the following metadata fields to each message:
		
		- sqs_message_id
		- sqs_receipt_handle
		- sqs_approximate_receive_count
		- TODO Add details of FIFO specific attributes
		- All message attributes
		
		You can access these metadata fields using
		xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(
			service.NewURLField(confFieldUrl).
				Description("The URL of the SQS FIFO queue"),
			service.NewIntField(confFieldVisibilityTimeout).
				Description("The visibility timeout of messages received from the queue, in seconds. Visibility timeouts will automatically be extended to this amount periodically once they have been received but have not yet been acked.").
				ShortDescription("The visibility timeout of messages received from the queue, in seconds.").
				Default(30).
				Advanced(),
			service.NewIntField(confFieldMinReceiveBatchSize).
				Description("The minimum batch size to use when receiving new messages. There must be at least this much free space in the buffer of in-flight messages before any attempt is made to fetch more.").
				ShortDescription("The minimum batch size to use when receiving new messages.").
				Default(5).
				Advanced(),
			service.NewIntField(confFieldMaxReceiveBatchSize).
				Description("The maximum batch size to use when receiving new messages. Must be a value between 1 and 10.").
				ShortDescription("The maximum batch size to use when receiving new messages.").
				Default(10).
				Advanced(),
			service.NewIntField(confFieldMaxInFlightMessages).
				Description("The maximum number of messages that can be in-flight concurrently. A message is considered in-flight as soon as it is received from the queue and has not yet been deleted.").
				ShortDescription("The maximum number of messages that can be in-flight concurrently.").
				Default(30).
				Advanced(),
		)
}

func init() {
	service.MustRegisterInput("aws_sqs_fifo", sqsFifoInputSpec(),
		func(cConf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			// TODO build the actual AWS config
			// aConf := aws.NewConfig()
			iConf, err := inputConfigFromConnectConfig(cConf)

			if err != nil {
				return nil, err
			}

			// TODO inject logger to input (mgr.Logger())
			return NewSqsFifoInput(iConf), nil
		})
}
