package adminserver

import (
	"context"
	"encoding/json"
	"market-data-gateway/internal/core"
	"net/http"
)

// server exposes HTTP endpoints on an internal port.
type server struct {
    store  *core.Store
    server *http.Server
}

func NewAdminServer(store *core.Store) *server {
    return &server{store: store}
}

func (s *server) ListenAndServe(addr string) error {
    mux := http.NewServeMux()
    mux.HandleFunc("/admin/book", s.handleBook)
    s.server = &http.Server{Addr: addr, Handler: mux}
    return s.server.ListenAndServe()
}

func (s *server) Shutdown(ctx context.Context) error {
    return s.server.Shutdown(ctx)
}

func (s *server) handleBook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	exchange := r.URL.Query().Get("exchange")
	symbol := r.URL.Query().Get("symbol")
	snap := s.store.Snapshot(exchange, symbol)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(snap); err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
	}
}
