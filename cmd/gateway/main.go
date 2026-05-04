package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"market-data-gateway/internal/adapters/adminserver"
	"market-data-gateway/internal/adapters/exchanges/binance"
	"market-data-gateway/internal/adapters/exchanges/kraken"
	"market-data-gateway/internal/adapters/wshandler"
	"market-data-gateway/internal/cli"
	"market-data-gateway/internal/config"
	"market-data-gateway/internal/core"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "book":
			cli.RunBook(os.Args[2:])
			return
		}
	}

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

	adminAddr := fmt.Sprintf(":%d", cfg.Server.AdminPort)
	adminSrv := adminserver.NewAdminServer(store)
	go func() {
		if err := adminSrv.ListenAndServe(adminAddr); err != nil && err != http.ErrServerClosed {
			log.Printf("admin: %v", err)
		}
	}()

	manager := wshandler.NewManager(store)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		manager.ServeWS(ctx, w, r)
	})

	wsAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	server := &http.Server{Addr: wsAddr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("main: server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("main: shutting down")

	if err := adminSrv.Shutdown(context.Background()); err != nil {
		log.Printf("admin: shutdown error: %v", err)
	}

	if err := server.Shutdown(context.Background()); err != nil {
		log.Printf("main: server shutdown error: %v", err)
	}

	manager.Shutdown()
	wg.Wait()
	log.Println("main: done")
}
