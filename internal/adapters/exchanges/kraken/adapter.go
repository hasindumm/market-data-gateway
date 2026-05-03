package kraken

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"market-data-gateway/internal/domain"
	"strconv"
	"time"
)

const wsURL = "wss://ws.kraken.com/v2"

type Adapter struct {
	symbols []string
	depth   int
}

func NewAdapter(symbols []string, depth int) *Adapter {
	return &Adapter{
		symbols: symbols,
		depth:   depth,
	}
}

func (a *Adapter) Name() string { return "kraken" }

func (a *Adapter) Run(ctx context.Context) (<-chan domain.Update, error) {
	out := make(chan domain.Update, 64)

	go func() {
		defer close(out)
		backoff := time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			if err := a.connect(ctx, out); err != nil && ctx.Err() == nil {
				log.Printf("kraken: %v; retry in %s", err, backoff)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
			} else {
				backoff = time.Second
				if ctx.Err() != nil {
					return
				}
			}
		}
	}()

	return out, nil
}

func (a *Adapter) connect(ctx context.Context, out chan<- domain.Update) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-stop:
		}
	}()

	sub := subscribeMsg{
		Method: "subscribe",
		Params: subscribeParams{
			Channel: "book",
			Symbol:  a.symbols,
			Depth:   a.depth,
		},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var env struct {
			Channel string `json:"channel"`
			Type    string `json:"type"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			log.Printf("kraken: unmarshal envelope: %v", err)
			continue
		}
		if env.Channel != "book" {
			continue //  ignore heartbeat, status
		}

		var msg bookMsg
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("kraken: unmarshal book: %v", err)
			continue
		}

		for _, entry := range msg.Data {
			u := toUpdate(entry, msg.Type)
			select {
			case out <- u:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func toUpdate(entry bookEntry, msgType string) domain.Update {
	var updateType domain.UpdateType
	if msgType == "snapshot" {
		updateType = domain.UpdateTypeSnapshot
	} else {
		updateType = domain.UpdateTypeDelta
	}

	ts, err := time.Parse(time.RFC3339Nano, entry.Timestamp)
	if err != nil {
		ts = time.Now()
	}

	return domain.Update{
		Type:      updateType,
		Exchange:  "kraken",
		Symbol:    entry.Symbol,
		Bids:      convertLevels(entry.Bids),
		Asks:      convertLevels(entry.Asks),
		Timestamp: ts,
	}
}

func convertLevels(levels []priceQty) []domain.Level {
	out := make([]domain.Level, 0, len(levels))
	for _, l := range levels {
		out = append(out, domain.Level{
			Price:    strconv.FormatFloat(l.Price, 'f', -1, 64),
			Quantity: strconv.FormatFloat(l.Qty, 'f', -1, 64),
		})
	}
	return out
}

// subscribeMsg is sent to the kraken
type subscribeMsg struct {
	Method string          `json:"method"`
	Params subscribeParams `json:"params"`
}

type subscribeParams struct {
	Channel string   `json:"channel"`
	Symbol  []string `json:"symbol"`
	Depth   int      `json:"depth"`
}

// response from kraken for snapshot or update
type bookMsg struct {
	Channel string      `json:"channel"`
	Type    string      `json:"type"` // snapshot or update
	Data    []bookEntry `json:"data"`
}

type bookEntry struct {
	Symbol    string     `json:"symbol"`
	Bids      []priceQty `json:"bids"`
	Asks      []priceQty `json:"asks"`
	Timestamp string     `json:"timestamp"`
}

type priceQty struct {
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}
