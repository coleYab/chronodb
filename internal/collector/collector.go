package collector

import (
	"context"

	"github.com/coleYab/chronodb/internal/core"
)

type Collector interface {
	Run(ctx context.Context, out chan<- core.Sample) error
	Stop() error
}
