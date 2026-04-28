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
		ReadBufferSize:  1024, // why its 1024
		WriteBufferSize: 1024, // why ??
	}
)

type Manager struct {
	store   *core.Store
	clients map[*client]struct{}
	sync.RWMutex
	wg sync.WaitGroup
	mu sync.Mutex
}

func NewManager(store *core.Store) *Manager {
	return &Manager{
		store:   store,
		clients: make(map[*client]struct{}),
	}
}

func (m *Manager) ServeWS(ctx context.Context, w http.ResponseWriter, r *http.Request) {

	conn, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("serveWS: failed to upgrade connection: %v", err)
		return
	}

	for _, snap := range m.store.SnapshotAll() {
		if err := conn.WriteJSON(snap); err != nil {
			log.Printf("wsserver: initial snapshot: %v", err)
			conn.Close()
			return
		}
	}

	c := newClient(conn, m)
	m.store.Subscribe(c.send)
	m.mu.Lock()
	m.clients[c] = struct{}{}
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		c.readMessage()
	}()
	go c.writeMessage(ctx)

}

func (m *Manager) removeClient(c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, c)
}

func (m *Manager) Shutdown() {
	m.Lock()
	for client := range m.clients {
		client.conn.Close()
	}
	m.Unlock()
	m.wg.Wait()
}

//Without WaitGroup:

// 10 clients connected, all chatting

// Ctrl+C hits
// server.Shutdown()  — no new connections
// manager.Shutdown() — closes all 10 conns, returns immediately
// main returns       — process exits

// meanwhile...
// client1 readMessage got error, starting cleanup... too late, process gone
// client3 removeClient trying to close send channel... process gone
// client7 writeMessage still writing... process gone

// send channels never closed
// some conns never properly closed
// OS forcefully kills everything mid-cleanup

//With WaitGroup:

// 10 clients connected, all chatting
// wg counter = 10

// Ctrl+C hits
// server.Shutdown()  — no new connections
// manager.Shutdown() — closes all 10 conns
// wg.Wait()          — blocks here, counter = 10

// client1 readMessage gets error → removeClient → close(send) → writeMessage exits → wg.Done()  // counter = 9
// client2 readMessage gets error → removeClient → close(send) → writeMessage exits → wg.Done()  // counter = 8
// client3 readMessage gets error → removeClient → close(send) → writeMessage exits → wg.Done()  // counter = 7
// ...
// client10 readMessage gets error → removeClient → close(send) → writeMessage exits → wg.Done() // counter = 0

// wg.Wait() unblocks
// main returns — process exits cleanly
// every connection properly closed
// every channel properly closed
// no goroutines left behind
