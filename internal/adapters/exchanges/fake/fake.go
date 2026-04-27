package fake

import (
	"context"
	"market-data-gateway/internal/domain"
	"time"
)

type Adapter struct {
	name    string
	symbols []string
}

func NewAdapter(name string, symbols []string) *Adapter {
	return &Adapter{
		name:    name,
		symbols: symbols,
	}
}

func (a *Adapter) Name() string {
	return a.name
}

func (a *Adapter) Run(ctx context.Context) (<-chan domain.Update, error) {
	out := make(chan domain.Update, 16)
	go func() {
		defer close(out)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// TODO: emit
				update := domain.Update{
					UpdateType: "delta",
					Symbol:     a.symbols[0],
					Bids:       []domain.Level{{Price: "50000.00", Quantity: "1.5"}},
					Asks:       []domain.Level{{Price: "50001.00", Quantity: "2.0"}},
				}

				select {
				case out <- update:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
