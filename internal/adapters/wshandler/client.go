package wshandler

import (
	"context"
	"errors"
	"log"
	"market-data-gateway/internal/core"
	"market-data-gateway/internal/domain"
	"sort"
	"sync"

	"github.com/gorilla/websocket"
)

const clientChanBuffer = 64

type Client struct {
	conn      *websocket.Conn
	manager   *Manager
	send      chan domain.Update
	writeMu   sync.Mutex
	filterMu  sync.RWMutex
	filter    map[string]struct{}
	closeOnce sync.Once
	done      chan struct{}
}

type ack struct {
	Type    string   `json:"type"`
	Action  string   `json:"action"`
	Symbols []string `json:"symbols"`
	Filter  []string `json:"filter"`
	Error   string   `json:"error"`
}

func newClient(conn *websocket.Conn, manager *Manager) *Client {
	return &Client{
		conn:    conn,
		manager: manager,
		send:    make(chan domain.Update, clientChanBuffer),
		filter:  make(map[string]struct{}),
		done:    make(chan struct{}),
	}
}

func (c *Client) cleanup() {
	c.closeOnce.Do(func() {
		c.manager.store.Unsubscribe(c.send)
		close(c.done)
		c.conn.Close()
		c.manager.removeClient(c)
	})
}

func (c *Client) readMessage(store *core.Store) {

	defer c.cleanup()
	for {
		var msg struct {
			Action  string   `json:"action"`
			Symbols []string `json:"symbols"`
		}
		if err := c.conn.ReadJSON(&msg); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("wshandler: read: %v", err)
			}
			return
		}
		switch msg.Action {
		case "subscribe":
			added := c.addToFilter(msg.Symbols)
			c.sendAck("subscribe", msg.Symbols, nil)
			for _, sym := range added {
				for _, snap := range store.SnapshotsForSymbol(sym) {
					select {
					case c.send <- snap:
					default:
					}
				}
			}
		case "unsubscribe":
			c.removeFromFilter(msg.Symbols)
			c.sendAck("unsubscribe", msg.Symbols, nil)
		default:
			c.sendAck(msg.Action, msg.Symbols, errors.New("unknown action"))
		}
	}

}

func (c *Client) sendAck(action string, symbols []string, err error) {
	if symbols == nil {
		symbols = []string{}
	}
	filter := c.currentFilter()
	if filter == nil {
		filter = []string{}
	}
	ack := ack{
		Type:    "ack",
		Action:  action,
		Symbols: symbols,
		Filter:  filter,
	}
	if err != nil {
		ack.Error = err.Error()
	}
	if werr := c.writeJSON(ack); werr != nil {
		log.Printf("wshandler: sendAck: %v", werr)
	}
}

func (c *Client) addToFilter(symbols []string) []string {
	c.filterMu.Lock()
	defer c.filterMu.Unlock()
	var added []string
	for _, s := range symbols {
		if _, ok := c.filter[s]; !ok {
			c.filter[s] = struct{}{}
			added = append(added, s)
		}
	}
	return added
}

func (c *Client) removeFromFilter(symbols []string) {
	c.filterMu.Lock()
	defer c.filterMu.Unlock()
	for _, s := range symbols {
		delete(c.filter, s)
	}
}

func (c *Client) currentFilter() []string {
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	out := make([]string, 0, len(c.filter))
	for s := range c.filter {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func (c *Client) writeJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(v)
}

func (c *Client) writeMessage(ctx context.Context) {

	defer c.cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case u := <-c.send:
			if !c.wants(u.Symbol) {
				continue
			}
			if err := c.writeJSON(u); err != nil {
				return
			}
		}
	}
}

// check before writing is given symbol requeted
func (c *Client) wants(symbol string) bool {
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	if len(c.filter) == 0 {
		return true
	}
	_, ok := c.filter[symbol]
	return ok
}
