package sqs_fifo

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type batchUpdateVisibilityError struct {
	entries []types.BatchResultErrorEntry
}

func (err *batchUpdateVisibilityError) Error() string {
	if len(err.entries) == 0 {
		return "(no failures)"
	}
	var msg strings.Builder
	msg.WriteString("failed to update visibility for messages: [")
	for i, fail := range err.entries {
		if i > 0 {
			msg.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&msg, "%q", *fail.Id)
	}
	msg.WriteByte(']')
	return msg.String()
}
