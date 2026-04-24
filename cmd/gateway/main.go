package main

import (
	"context"
	"log"
	"market-data-gateway/internal/adapters/wshandler"
	"net/http"
	"os"
	"os/signal"
)

func main() {

	manager := wshandler.NewManager()
	http.HandleFunc("/ws", manager.ServeWS)

	server := &http.Server{Addr: ":8080"}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("main: server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	<-sigCh
	if err := server.Shutdown(context.Background()); err != nil {
		log.Printf("main: server shutdown error: %v", err)
	}
	manager.Shutdown()
}
