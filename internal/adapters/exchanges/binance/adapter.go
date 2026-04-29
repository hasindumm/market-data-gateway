package binance

import (
	"context"
	"market-data-gateway/internal/domain"
	"sync"
)

type Adapter struct {
	symbols []string
	mu      sync.Mutex
	workers map[string]*symbolWorker
}

func NewAdapter(symbols []string) *Adapter {
	return &Adapter{
		symbols: symbols,
		workers: make(map[string]*symbolWorker),
	}
}

func (a *Adapter) Name() string { return "binance" }

func (a *Adapter) Run(ctx context.Context) (<-chan domain.Update, error) {
	perSym := make([]<-chan domain.Update, 0, len(a.symbols))

	a.mu.Lock()
	for _, sym := range a.symbols {
		w := newSymbolWorker(sym)
		a.workers[sym] = w
		perSym = append(perSym, w.run(ctx))
	}
	a.mu.Unlock()

	out := make(chan domain.Update, len(a.symbols))

	var wg sync.WaitGroup
	wg.Add(len(perSym))
	for _, src := range perSym {
		go func(ch <-chan domain.Update) {
			defer wg.Done()
			for {
				select {
				case u, ok := <-ch:
					if !ok {
						return
					}
					select {
					case out <- u:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(src)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	// return channel that have all symbols update mergerd (fan-in)
	return out, nil
}