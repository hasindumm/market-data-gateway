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

type Manager struct {
	store   *core.Store
	clients map[*Client]struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
}

func NewManager(store *core.Store) *Manager {
	return &Manager{
		store:   store,
		clients: make(map[*Client]struct{}),
	}
}

func (m *Manager) ServeWS(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	conn, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("serveWS: failed to upgrade connection: %v", err)
		return
	}

	c := newClient(conn, m)

	snaps := m.store.SubscribeAndSnapshot(c.send)
	for _, snap := range snaps {
		if err := conn.WriteJSON(snap); err != nil {
			log.Printf("wshandler: initial snapshot: %v", err)
			m.store.Unsubscribe(c.send)
			conn.Close()
			return
		}
	}

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

func (m *Manager) removeClient(c *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, c)
}

func (m *Manager) Shutdown() {
	m.mu.Lock()
	// Clients are copied under lock to avoid deadlock since cleanup calls removeClient which also locks mu.
	clients := make([]*Client, 0, len(m.clients))
	for c := range m.clients {
		clients = append(clients, c)
	}
	m.mu.Unlock()

	for _, c := range clients {
		c.cleanup()
	}
	m.wg.Wait()
}
