package wshandler

import (
	"log"

	"github.com/gorilla/websocket"
)

type client struct {
	conn    *websocket.Conn
	manager *manager
	send    chan []byte
}

func newClient(conn *websocket.Conn, manager *manager) *client {
	return &client{
		conn:    conn,
		manager: manager,
		send:    make(chan []byte, 256), // buffer size get to a const and
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
				log.Printf("error reading message: %v", err)
			}
			return
		}
		log.Println("MessageType: ", mt)
		log.Println("Payload: ", string(payload))

		//just for testing
		for client := range c.manager.clients {
			client.send <- payload
		}
	}

}

func (c *client) writeMessage() {

	for msg := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			log.Println(err)
			c.conn.Close()
			return
		}
	}
}
