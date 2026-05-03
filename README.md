# Market Data Gateway

A real-time order book gateway written in Go. Connects to Binance and Kraken, normalizes their order book feeds into a unified format, and streams live updates to downstream clients over WebSocket.

---

## What it does

- Connects to **Binance** and **Kraken** WebSocket APIs simultaneously
- Maintains an in-memory order book for each configured symbol on each exchange
- Exposes a single **WebSocket endpoint** at `ws://localhost:8080/ws` — clients connect once and receive live order book data from all exchanges
- Exposes an **admin HTTP API** at `http://localhost:9090` for querying books and forcing resyncs
- Provides **CLI subcommands** to inspect books and trigger refreshes from the terminal

---


## Exchange sync details

**Binance** uses the official diff-depth stream sync algorithm:
1. Open WebSocket stream for the symbol
2. Buffer incoming events
3. Fetch REST snapshot (`/api/v3/depth`)
4. Drop buffered events already covered by the snapshot
5. Replay remaining buffered events onto the snapshot
6. Emit the synced book as a snapshot, then stream deltas
7. On sequence gap — restart the sync from step 1

**Kraken** uses the v2 WebSocket API:
1. Connect and subscribe to all symbols on one connection
2. First message per symbol is a full snapshot — emit directly
3. Subsequent messages are deltas — emit directly
4. No REST snapshot needed; the WS provides it

---

## Getting started

### Prerequisites

- Go 1.25

### Build

```bash
go build -o gateway ./cmd/gateway
```

### Configure

Edit `config.json`:

```json
{
  "server": {
    "port": 8080,
    "admin_port": 9090
  },
  "exchanges": {
    "binance": {
      "symbols": ["BTCUSDT", "ETHUSDT"]
    },
    "kraken": {
      "symbols": ["BTC/USD", "ETH/USD"]
    }
  }
}
```

Config uses JSON (standard library only — no YAML dependency).

### Run

```bash
./gateway --config config.json
```

The gateway starts two servers:
- `ws://localhost:8080/ws` — WebSocket endpoint for downstream clients
- `http://localhost:9090` — admin HTTP API

---

## WebSocket client protocol

Connect to `ws://localhost:8080/ws`.

On connect you immediately receive a full order book snapshot for every tracked symbol across all exchanges. After that, live deltas stream continuously.

### Message format

Every message has a `type` field:

**Order book update** (`type: snapshot` or `type: delta`):
```json
{
  "type": "snapshot",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "bids": [
    {"price": "67000.01", "qty": "1.234"},
    {"price": "66999.50", "qty": "0.500"}
  ],
  "asks": [
    {"price": "67001.00", "qty": "0.100"}
  ],
  "timestamp": "2024-01-01T12:00:00Z"
}
```

Bids are sorted descending by price. Asks are sorted ascending. A `delta` carries only changed levels — a level with `qty: "0"` means that price has been removed from the book.

**Ack** (`type: ack`) — response to subscribe/unsubscribe/list actions:
```json
{
  "type": "ack",
  "action": "subscribe",
  "symbols": ["BTCUSDT"],
  "filter": ["BTCUSDT"],
  "error": ""
}
```

### Filtering

By default a new client receives updates for all symbols. You can filter:

**Subscribe** — receive updates only for these symbols:
```json
{"action": "subscribe", "symbols": ["BTCUSDT"]}
```

**Unsubscribe** — stop receiving updates for these symbols:
```json
{"action": "unsubscribe", "symbols": ["BTCUSDT"]}
```

If unsubscribing empties your filter, you go back to receiving everything.


When you subscribe to a symbol you immediately receive its current snapshot 


## CLI subcommands

The same binary provides CLI commands that talk to the running gateway over the admin API.

### Print current order book

```bash
./gateway book --exchange binance --symbol BTCUSDT
./gateway book --exchange kraken  --symbol BTC/USD
```

---

## Admin HTTP API

The gateway exposes two endpoints on `:9090`:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/book?exchange=binance&symbol=BTCUSDT` | Returns current order book as JSON |


---

## Running tests

```bash
go test -race ./...
```

---



## Graceful shutdown

On `Ctrl+C` or `SIGTERM`:

1. Root context is cancelled — all exchange adapters stop reading and close their output channels
2. Pipeline drains and closes the merged channel
3. Store processes remaining updates and exits
4. WebSocket server stops accepting new connections
5. All active client connections are closed cleanly
6. Process exits only after every goroutine has finished
