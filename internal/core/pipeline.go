package core

import (
	"context"
	"fmt"
	"market-data-gateway/internal/domain"
	"sync"
)

const exchangeChanBuffer = 512

type Exchanger interface {
	Run(ctx context.Context) (<-chan domain.Update, error)
	Name() string
}

type pipeline struct {
	exchanges []Exchanger
	store     *Store
}

func NewPipeline(exchanges []Exchanger, store *Store) *pipeline {
	return &pipeline{
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
func (p *pipeline) Run(ctx context.Context) error {
	sources := make([]<-chan domain.Update, 0, len(p.exchanges))

	for _, exc := range p.exchanges {
		updateChan, err := exc.Run(ctx)
		if err != nil {
			return fmt.Errorf("pipeline: start %s: %w", exc.Name(), err)
		}
		sources = append(sources, updateChan)
	}
	// buffer 512: sized to exceed combined adapter throughput
	// each binance adapter  buffers len(symbols)*64, and kraken buffers 128 this covers fan-in of all adapters
	merged := make(chan domain.Update, exchangeChanBuffer)
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

	p.store.run(merged)
	return nil
}
