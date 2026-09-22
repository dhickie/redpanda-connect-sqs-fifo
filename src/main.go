package main

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "dhickie/redpanda-connect-sqs-fifo/src/input"

	_ "github.com/redpanda-data/connect/public/bundle/free/v4"
)

func main() {
	service.RunCLI(context.Background())
}
