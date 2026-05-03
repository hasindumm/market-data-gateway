package core

import (
	"context"
	"fmt"
	"market-data-gateway/internal/domain"
	"sync"
)

type Exchanger interface {
	Run(ctx context.Context) (<-chan domain.Update, error)
	Name() string
}

type Pipeline struct {
	exchanges []Exchanger
	store     *Store
}

func NewPipeline(exchanges []Exchanger, store *Store) *Pipeline {
	return &Pipeline{
		exchanges: exchanges,
		store:     store,
	}
}

// Run will create array of receive channel that lenth of number of exchanged
// then it will loop and run those exchanges and will get update channels and fill the array of recieve channels
// then create a merged channel where all exchange spesific channels get merged
// then loop the array of recevive channel and start go routine for every exchnage spsesific channel
// when something came over a channel get that and put into mergerd channel
// finally when something showed up in merged channel apply it to center store
func (p *Pipeline) Run(ctx context.Context) error {
	sources := make([]<-chan domain.Update, 0, len(p.exchanges))

	for _, exc := range p.exchanges {
		updateChan, err := exc.Run(ctx)
		if err != nil {
			return fmt.Errorf("pipeline: start %s: %w", exc.Name(), err)
		}
		sources = append(sources, updateChan)
	}
	// channel buffer size 64: at peak combined rate (~50 updates/sec from Binance + Kraken)
	// this absorbs ~1 second of consumer lag before backpressuring forwarders.
	merged := make(chan domain.Update, 64)
	var wg sync.WaitGroup
	wg.Add(len(sources))

	for _, src := range sources {
		go func() {
			defer wg.Done()
			for u := range src {
				merged <- u
			}
		}()
	}
	go func() {
		wg.Wait()
		close(merged)
	}()

	p.store.Run(ctx, merged)
	return nil
}
