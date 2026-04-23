package main

import (
	"context"
	"market-data-gateway/internal/adapters/wshandler"
	"net/http"
	"os"
	"os/signal"
)

func main() {

	manager := wshandler.NewManager()
	http.HandleFunc("/ws", manager.ServeWS)

	server := &http.Server{Addr: ":8080"}
    go server.ListenAndServe()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	<-sigCh
	server.Shutdown(context.Background())
	manager.Shutdown()
}
