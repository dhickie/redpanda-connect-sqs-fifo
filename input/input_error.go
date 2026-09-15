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
