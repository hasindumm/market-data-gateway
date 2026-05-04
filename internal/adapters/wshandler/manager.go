package wshandler

import (
	"context"
	"log"
	"market-data-gateway/internal/core"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	websocketUpgrader = websocket.Upgrader{
		ReadBufferSize:  1024, // ReadBufferSize 1024: client messages are small (subscription requests, pings)
		WriteBufferSize: 4096, // WriteBufferSize 4096: order book snapshots with 10-20 price levels serialize
	}
)

type manager struct {
	store   *core.Store
	clients map[*client]struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
}

func NewManager(store *core.Store) *manager {
	return &manager{
		store:   store,
		clients: make(map[*client]struct{}),
	}
}

func (m *manager) ServeWS(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	conn, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("serveWS: failed to upgrade connection: %v", err)
		return
	}

	for _, snap := range m.store.SnapshotAll() {
		if err := conn.WriteJSON(snap); err != nil {
			log.Printf("wshandler: initial snapshot: %v", err)
			conn.Close()
			return
		}
	}

	c := newClient(conn, m)
	m.store.Subscribe(c.send)
	m.mu.Lock()
	m.clients[c] = struct{}{}
	m.mu.Unlock()

	m.wg.Add(2)
	go func() {
		defer m.wg.Done()
		c.readMessage(m.store)
	}()
	go func() {
		defer m.wg.Done()
		c.writeMessage(ctx)
	}()

}

func (m *manager) removeClient(c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, c)
}

func (m *manager) Shutdown() {
	m.mu.Lock()
	// Clients are copied under lock to avoid deadlock since cleanup calls removeClient which also locks mu.
	clients := make([]*client, 0, len(m.clients))
	for c := range m.clients {
		clients = append(clients, c)
	}
	m.mu.Unlock()

	for _, c := range clients {
		c.cleanup()
	}
	m.wg.Wait()
}
