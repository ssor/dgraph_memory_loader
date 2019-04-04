package dgraph_live_client

import (
	"context"
	"math"
)

func NewBatchMutaionOptions(batchSize, concurrent int) BatchMutationOptions {
	ctx := context.Background()
	options := BatchMutationOptions{
		Size:          batchSize,
		Pending:       concurrent,
		PrintCounters: true,
		Ctx:           ctx,
		MaxRetries:    math.MaxUint32,
	}
	return options
}

type BatchMutationOptions struct {
	Size          int
	Pending       int
	PrintCounters bool
	MaxRetries    uint32
	// User could pass a context so that we can stop retrying requests once context is done
	Ctx context.Context
}
