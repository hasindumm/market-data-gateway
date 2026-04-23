package wshandler

import (
	"log"
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

type manager struct {
	clients map[*client]bool
	sync.RWMutex
	wg sync.WaitGroup
}

func NewManager() *manager {
	return &manager{
		clients: make(map[*client]bool),
	}
}

func (m *manager) ServeWS(w http.ResponseWriter, r *http.Request) {

	conn, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade to websocket connection failed", err)
		return
	}

	client := newClient(conn, m)
	m.addClient(client)

	m.wg.Add(1)
	go func(){
		client.readMessage()
		defer m.wg.Done()
	}()
	go client.writeMessage()

}

func (m *manager) addClient(c *client) {
	m.Lock()
	defer m.Unlock()
	m.clients[c] = true
}

func (m *manager) removeClient(c *client) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.clients[c]; ok {
		close(c.send)
		c.conn.Close()
		delete(m.clients, c)
	}
}

func (m *manager) Shutdown(){
	m.Lock()
	for client := range m.clients{
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