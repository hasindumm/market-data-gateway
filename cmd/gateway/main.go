package main

import (
	"context"
	"log"
	"market-data-gateway/internal/adapters/exchanges/fake"
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

	fakeexchange1 := fake.NewAdapter("abc", []string{"btc"})
	fakeexchange2 := fake.NewAdapter("xyz", []string{"eth"})

	exchangers := []core.Exchanger{fakeexchange1, fakeexchange2}

	pipeline := core.NewPipeline(exchangers, store)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := pipeline.Run(ctx); err != nil {
			log.Printf("pipeline error: %v", err)
		}
	}()

	manager := wshandler.NewManager()
	http.HandleFunc("/ws", manager.ServeWS)

	server := &http.Server{Addr: ":8080"}
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
