package binance

import (
	"context"
	"market-data-gateway/internal/domain"
	"sync"
)

type Adapter struct {
	symbols []string
	depth   int
	workers map[string]*symbolWorker
}

func NewAdapter(symbols []string, depth int) *Adapter {
	return &Adapter{
		symbols: symbols,
		depth:   depth,
		workers: make(map[string]*symbolWorker),
	}
}

func (a *Adapter) Name() string { return "binance" }

func (a *Adapter) Run(ctx context.Context) (<-chan domain.Update, error) {
	perSym := make([]<-chan domain.Update, 0, len(a.symbols))

	for _, sym := range a.symbols {
		w := newSymbolWorker(sym, a.depth)
		a.workers[sym] = w
		perSym = append(perSym, w.run(ctx))
	}

	out := make(chan domain.Update, len(a.symbols)*symChanBuffer)

	var wg sync.WaitGroup
	wg.Add(len(perSym))
	for _, src := range perSym {
		go func() {
			defer wg.Done()
			for u := range src {
				out <- u
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	// return channel that have all symbols update mergerd (fan-in)
	return out, nil
}
