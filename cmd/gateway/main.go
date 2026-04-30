package main

import (
	"context"
	"log"
	"market-data-gateway/internal/adapters/exchanges/binance"
	"market-data-gateway/internal/adapters/exchanges/kraken"
	"market-data-gateway/internal/adapters/wshandler"
	"market-data-gateway/internal/core"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {

	var wg sync.WaitGroup
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer cancel()

	store := core.NewStore()

	// fakeexchange1 := fake.NewAdapter("abc", []string{"btc"})
	// fakeexchange2 := fake.NewAdapter("xyz", []string{"eth"})

	binance := binance.NewAdapter([]string{"BTCUSDT", "ETHUSDT"})
	kraken := kraken.NewAdapter([]string{"BTC/USD", "ETH/USD"})

	exchangers := []core.Exchanger{kraken,binance}

	pipeline := core.NewPipeline(exchangers, store)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := pipeline.Run(ctx); err != nil {
			log.Printf("pipeline error: %v", err)
		}
	}()

	manager := wshandler.NewManager(store)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		manager.ServeWS(ctx, w, r)
	})

	server := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("main: server error: %v", err)
		}
	}()

	<-ctx.Done()
	if err := server.Shutdown(context.Background()); err != nil {
		log.Printf("main: server shutdown error: %v", err)
	}
	wg.Wait()
	manager.Shutdown()
}
