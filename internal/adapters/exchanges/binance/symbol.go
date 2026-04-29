package binance

import (
	"context"
	"market-data-gateway/internal/domain"
)

type symbolWorker struct {
	symbol  string
}

func newSymbolWorker(symbol string) *symbolWorker {
	return &symbolWorker{
		symbol:  symbol,
	}
}

func (w *symbolWorker) run(ctx context.Context) <-chan domain.Update {
	out := make(chan domain.Update, 64)
	// sync logic here 


	// out channel have out put for this symbol
	return out
}