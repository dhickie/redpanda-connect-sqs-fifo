// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqs_fifo

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// SQS Input Fields
	sqsiFieldURL                 = "url"
	sqsiFieldWaitTimeSeconds     = "wait_time_seconds"
	sqsiFieldDeleteMessage       = "delete_message"
	sqsiFieldResetVisibility     = "reset_visibility"
	sqsiFieldMaxNumberOfMessages = "max_number_of_messages"
	sqsiFieldMaxOutstanding      = "max_outstanding_messages"
	sqsiFieldMessageTimeout      = "message_timeout"
)

type sqsiConfig struct {
	URL                 string
	WaitTimeSeconds     int
	DeleteMessage       bool
	ResetVisibility     bool
	MaxNumberOfMessages int
	MaxOutstanding      int
	MessageTimeout      time.Duration
}

func sqsiConfigFromParsed(pConf *service.ParsedConfig) (conf sqsiConfig, err error) {
	if conf.URL, err = pConf.FieldString(sqsiFieldURL); err != nil {
		return
	}
	if conf.WaitTimeSeconds, err = pConf.FieldInt(sqsiFieldWaitTimeSeconds); err != nil {
		return
	}
	if conf.DeleteMessage, err = pConf.FieldBool(sqsiFieldDeleteMessage); err != nil {
		return
	}
	if conf.ResetVisibility, err = pConf.FieldBool(sqsiFieldResetVisibility); err != nil {
		return
	}
	if conf.MaxNumberOfMessages, err = pConf.FieldInt(sqsiFieldMaxNumberOfMessages); err != nil {
		return
	}
	if conf.MaxOutstanding, err = pConf.FieldInt(sqsiFieldMaxOutstanding); err != nil {
		return
	}
	if conf.MessageTimeout, err = pConf.FieldDuration(sqsiFieldMessageTimeout); err != nil {
		return
	}
	return
}

func sqsInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "AWS").
		Summary(`Consume messages from a AWS FIFO SQS URL.`).
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
			// TODO add details of new/changed configuration options
			service.NewURLField(sqsiFieldURL).
				Description("The SQS URL to consume from."),
			service.NewBoolField(sqsiFieldDeleteMessage).
				Description("Whether to delete the consumed message once it is acked. Disabling allows you to handle the deletion using a different mechanism.").
				ShortDescription("Delete the consumed message once it is acked.").
				Default(true).
				Advanced(),
			service.NewBoolField(sqsiFieldResetVisibility).
				Description("Whether to set the visibility timeout of the consumed message to zero once it is nacked. Disabling honors the preset visibility timeout specified for the queue.").
				ShortDescription("Set the visibility timeout of a consumed message to zero once it is nacked.").
				Version("3.58.0").
				Default(true).
				Advanced(),
			service.NewIntField(sqsiFieldMaxNumberOfMessages).
				Description("The maximum number of messages to return on one poll. Valid values: 1 to 10.").
				Default(10).
				Advanced(),
			service.NewIntField(sqsiFieldMaxOutstanding).
				Description("The maximum number of outstanding pending messages to be consumed at a given time.").
				Default(1000),
			service.NewIntField(sqsiFieldWaitTimeSeconds).
				Description("Whether to set the wait time. Enabling this activates long-polling. Valid values: 0 to 20.").
				Default(0).
				Advanced(),
			service.NewDurationField(sqsiFieldMessageTimeout).
				Description("The time to process messages before needing to refresh the receipt handle. Messages will be eligible for refresh when half of the timeout has elapsed. This sets MessageVisibility for each received message.").
				ShortDescription("How long to process messages before the receipt handle must be refreshed.").
				Default("30s").
				Advanced(),
		)
}

func init() {
	service.MustRegisterInput("aws_sqs_fifo", sqsInputSpec(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			// TODO build the actual AWS config
			aconf := aws.NewConfig()

			conf, err := sqsiConfigFromParsed(pConf)
			if err != nil {
				return nil, err
			}

			return newAWSSQSReader(conf, *aconf, mgr.Logger())
		})
}

func init() {

}
