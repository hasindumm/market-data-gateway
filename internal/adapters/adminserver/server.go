package adminserver

import (
	"context"
	"encoding/json"
	"market-data-gateway/internal/core"
	"net/http"
)

// Server exposes HTTP endpoints on an internal port.
type Server struct {
    store  *core.Store
    server *http.Server
}

func NewAdminServer(store *core.Store) *Server {
    return &Server{store: store}
}

func (s *Server) ListenAndServe(addr string) error {
    mux := http.NewServeMux()
    mux.HandleFunc("/admin/book", s.handleBook)
    s.server = &http.Server{Addr: addr, Handler: mux}
    return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
    return s.server.Shutdown(ctx)
}

func (s *Server) handleBook(w http.ResponseWriter, r *http.Request) {
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
