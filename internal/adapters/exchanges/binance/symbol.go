package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"market-data-gateway/internal/domain"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsBase      = "wss://stream.binance.com:9443/ws"
	restBase    = "https://api.binance.com/api/v3/depth"
	readTimeout = 60 * time.Second
	restTimeout = 10 * time.Second
)

type symbolWorker struct {
	symbol string
	depth  int
}

type depthEvent struct {
	EventType     string     `json:"e"`
	EventTime     int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"` // first update ID
	FinalUpdateID int64      `json:"u"` // final update ID
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

type depthSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

func newSymbolWorker(symbol string, depth int) *symbolWorker {
	return &symbolWorker{
		symbol: symbol,
		depth:  depth,
	}
}

func (w *symbolWorker) run(ctx context.Context) <-chan domain.Update {
	out := make(chan domain.Update, 64)
	// sync logic here

	go func() {
		defer close(out)
		backoff := time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			if err := w.sync(ctx, out); err != nil && ctx.Err() == nil {
				log.Printf("binance: %s: %v; retry in %s", w.symbol, err, backoff)
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

	// out channel have out put for this symbol
	return out
}

func (w *symbolWorker) sync(ctx context.Context, out chan<- domain.Update) error {

	url := fmt.Sprintf("%s/%s@depth", wsBase, strings.ToLower(w.symbol))
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("ws dial: %w", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(readTimeout))
	// heart beat checked
	conn.SetPingHandler(func(data string) error {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})

	msgCh := make(chan depthEvent, 1000)
	readErrCh := make(chan error, 1)

	// read from ws conn
	go func() {
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				readErrCh <- err
				return
			}
			conn.SetReadDeadline(time.Now().Add(readTimeout))
			var ev depthEvent
			if err := json.Unmarshal(raw, &ev); err != nil {
				readErrCh <- fmt.Errorf("unmarshal: %w", err)
				return
			}
			select {
			case msgCh <- ev:
			default:
				readErrCh <- fmt.Errorf("event buffer overflow - stream too fast")
				return
			}
		}
	}()

	// signal for buffering to stop when snapshot ready
	stopBuffering := make(chan struct{})
	bufferedCh := make(chan []depthEvent, 1)
	go func() {
		var buffered []depthEvent
		for {
			select {
			case ev := <-msgCh:
				buffered = append(buffered, ev)
			case <-stopBuffering:
				bufferedCh <- buffered
				return
			}
		}
	}()

	snap, err := w.fetchSnapshot(ctx)
	if err != nil {

		close(stopBuffering)
		<-bufferedCh
		return fmt.Errorf("rest snapshot: %w", err)
	}
	lastID := snap.LastUpdateID

	close(stopBuffering)
	buffered := <-bufferedCh

	// remove events from buffer if those are older than snapshot
	var kept []depthEvent
	for _, ev := range buffered {
		if ev.FinalUpdateID > lastID {
			kept = append(kept, ev)
		}
	}
	buffered = kept

	//Verify the bridge is solid
	if len(buffered) > 0 {
		first := buffered[0]
		if !(first.FirstUpdateID <= lastID+1 && lastID+1 <= first.FinalUpdateID) {
			return fmt.Errorf("snapshot stale: lastUpdateId=%d first.U=%d first.u=%d",
				lastID, first.FirstUpdateID, first.FinalUpdateID)
		}
	}

	// Build the book from the REST snapshot
	bids := make(map[string]string, len(snap.Bids))
	asks := make(map[string]string, len(snap.Asks))
	for _, l := range snap.Bids {
		if len(l) >= 2 {
			bids[l[0]] = l[1]
		}
	}
	for _, l := range snap.Asks {
		if len(l) >= 2 {
			asks[l[0]] = l[1]
		}
	}
	for _, ev := range buffered {
		applyRaw(bids, ev.Bids)
		applyRaw(asks, ev.Asks)
	}

	// send the fully synced snapshot
	select {
	case out <- makeSnapshot(w.symbol, bids, asks):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Track the final update id for sequence validation.
	var prevFinal int64
	if len(buffered) > 0 {
		prevFinal = buffered[len(buffered)-1].FinalUpdateID
	} else {
		prevFinal = lastID
	}

	// Stream deltas and do validations
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErrCh:
			return fmt.Errorf("ws read: %w", err)
		case ev := <-msgCh:
			if ev.FinalUpdateID <= prevFinal {
				continue
			}
			if ev.FirstUpdateID > prevFinal+1 {
				return fmt.Errorf("sequence gap: expected U<=%d got U=%d",
					prevFinal+1, ev.FirstUpdateID)
			}
			prevFinal = ev.FinalUpdateID
			select {
			case out <- makeDelta(w.symbol, ev):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

}

func (w *symbolWorker) fetchSnapshot(ctx context.Context) (*depthSnapshot, error) {
	reqCtx, cancel := context.WithTimeout(ctx, restTimeout)
	defer cancel()

	url := fmt.Sprintf("%s?symbol=%s&limit=%d", restBase, w.symbol, w.depth)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	var snap depthSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func applyRaw(side map[string]string, levels [][]string) {
	for _, l := range levels {
		if len(l) < 2 {
			continue
		}
		if isZero(l[1]) {
			delete(side, l[0])
		} else {
			side[l[0]] = l[1]
		}
	}
}

func isZero(qty string) bool {
	f, err := strconv.ParseFloat(qty, 64)
	return err == nil && f == 0
}

func makeSnapshot(symbol string, bids, asks map[string]string) domain.Update {
	return domain.Update{
		Type:      domain.UpdateTypeSnapshot,
		Exchange:  "binance",
		Symbol:    symbol,
		Bids:      mapToLevels(bids),
		Asks:      mapToLevels(asks),
		Timestamp: time.Now(),
	}
}

func mapToLevels(side map[string]string) []domain.Level {
	out := make([]domain.Level, 0, len(side))
	for price, qty := range side {
		out = append(out, domain.Level{Price: price, Quantity: qty})
	}
	return out
}

func makeDelta(symbol string, ev depthEvent) domain.Update {
	return domain.Update{
		Type:      domain.UpdateTypeDelta,
		Exchange:  "binance",
		Symbol:    symbol,
		Bids:      pairsToLevels(ev.Bids),
		Asks:      pairsToLevels(ev.Asks),
		Timestamp: time.UnixMilli(ev.EventTime),
	}
}

func pairsToLevels(pairs [][]string) []domain.Level {
	out := make([]domain.Level, 0, len(pairs))
	for _, p := range pairs {
		if len(p) >= 2 {
			out = append(out, domain.Level{Price: p[0], Quantity: p[1]})
		}
	}
	return out
}
