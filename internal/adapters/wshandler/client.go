package wshandler

import (
	"context"
	"log"
	"market-data-gateway/internal/domain"
	"sync"

	"github.com/gorilla/websocket"
)

type client struct {
	conn    *websocket.Conn
	manager *Manager
	send chan domain.Update
	writeMu sync.Mutex
}

func newClient(conn *websocket.Conn, manager *Manager) *client {
	return &client{
		conn:    conn,
		manager: manager,
		send:   make(chan domain.Update, 64), // buffer size get to a const and
		//Binance + Kraken send roughly 10-50 updates/second per symbol
		//If you have 2 symbols, that's ~100 updates/second max
		//buffer = max producer burst rate × acceptable lag time.
	}
}


func (c *client) readMessage() {

	defer c.manager.removeClient(c)

	for {
		mt, payload, err := c.conn.ReadMessage()
		if err != nil {
			// We only want to log Strange errors,  not normal Disconnection
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("readMessage: unexpected close: %v", err)  
			}
			return
		}
		// just for testing
		log.Println("MessageType: ", mt)
		log.Println("Payload: ", string(payload))

		
	}

}

func (c *client) writeJSON(v interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(v)
}


func (c *client) writeMessage(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return
		case u := <-c.send:
			if err := c.writeJSON(u); err != nil {
				return
			}
		}
	}
}
