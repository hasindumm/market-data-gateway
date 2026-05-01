package main

import (
	"context"
	"flag"
	"log"
	"market-data-gateway/internal/adapters/exchanges/binance"
	"market-data-gateway/internal/adapters/exchanges/kraken"
	"market-data-gateway/internal/adapters/wshandler"
	"market-data-gateway/internal/config"
	"market-data-gateway/internal/core"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {

	configPath := flag.String("config", "config.json", "path to config file")
	flag.Parse()

	cfg := config.MustLoad(*configPath)

	var exchangers []core.Exchanger
	if ec, ok := cfg.Exchanges["binance"]; ok && len(ec.Symbols) > 0 {
		exchangers = append(exchangers, binance.NewAdapter(ec.Symbols, ec.Depth))
	}
	if ec, ok := cfg.Exchanges["kraken"]; ok && len(ec.Symbols) > 0 {
		exchangers = append(exchangers, kraken.NewAdapter(ec.Symbols, ec.Depth))
	}
	if len(exchangers) == 0 {
		log.Fatal("main: no exchanges configured")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer cancel()

	store := core.NewStore()

	pipeline := core.NewPipeline(exchangers, store)

	var wg sync.WaitGroup
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
