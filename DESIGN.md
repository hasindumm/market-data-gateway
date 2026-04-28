# Structuring the codebase 

### code base structured according to the hexoganal pattern 
- core- holds bussiness logic
- adapters - holds how to deal with outside world
- domain - holds data shapes 

### Project Structure

```
market-data-gateway/
├── cmd/
│   └── gateway
|           └──main.go
├── internal/
│   ├── adapters/
│   │   ├── exchanges/
│   │   │   ├── binance/
│   │   │   │   └── 
│   │   │   └── kraken/
│   │   │       └── 
│   │   └── wsserver/
│   │       └── 
│   ├── core/
│   │   └── 
│   └── domain/
│       └── 
├── meta/
│   └── DIARY.md
├── DESIGN.md
├── go.mod
└── README.md


```

## WebSocket Server Design

### Component Structure
Two responsibilities, two types:
- `manager` — owns the client registry, handles upgrades, coordinates shutdown
- `client` — owns a single connection's read/write lifecycle

### Goroutine Model
Each connected client spawns two goroutines:
- `readMessage` — owns the client lifecycle
- `writeMessage` — follows, exits when signalled

`readMessage` is the single owner of cleanup. When it exits (error or shutdown),
it calls `removeClient` which closes the send channel and connection.
`writeMessage` exits naturally when the send channel is closed via `range`.

### Shutdown Ordering
On Ctrl+C:
1. `server.Shutdown` — stops accepting new connections
2. `manager.Shutdown` — closes all active connections, unblocking all `readMessage` goroutines
3. `wg.Wait()` — blocks until every `readMessage` goroutine has finished cleanup
4. process exits

Without this ordering: goroutines killed mid-cleanup, send channels unclosed,
connections orphaned.

### Channel Design
Each client has a buffered send channel `make(chan []byte, 256)`.

Buffer sizing rationale:
- this section depends on how binance and kraken together produce updates


## Domain and Core Design

### Domain Types

**Level**
Represents a single price point in the order book, holding a price and quantity as float64.

**OrderBook**
Represents the full state of an order book for a symbol, containing a symbol, bids, and asks.

**Update**
Represents an incremental change to an order book for a symbol. Although it has the same structure as OrderBook, the meaning is different — OrderBook is full state, Update is a change event.

### Core Store

Store holds a `map[string]domain.OrderBook` keyed by symbol. It uses `sync.RWMutex` because it can be modified by multiple goroutines concurrently. Adapters write updates while WS clients read snapshots. RWMutex allows concurrent reads without blocking each other, while writes get exclusive access.

### Binance Adapter

The Binance adapter is responsible for fetching data from Binance and converting it to domain types. Conversion happens at the adapter boundary so core never sees exchange-specific formats.





## Pipeline, interface and Fan-in design for Exchanges and wireup with dummy exchnage

### Pipeline

pipeline is responsible for get updates from different exchanges and merge into one 

### Fan-in design 

pipeline follows fan-in design  for get updates from different exchanges through channels. for each exchange it carries data using a channel and merge into a one channel and that channel have updates from each exchange 

### dummy exchange

developed a dummy exchange which mimics real exchanges behaviour to help with design in order to wire things up 

### interface design 

pipeline is the consumer of the exchanges . so Exchanger interface defined at pipeline and that interface has two methods, Run() and name() functions which every exchange should satisfy 



## Store, Order Book State and Broadcast to WebSocket Clients

### Store and Order Book State

Store is the single writer of order book. It get data through merged channel from the pipeline and applies every incoming update to an in-memory data structure keyed by (exchange, symbol) pair.

Two types of updates flow through the merged channel:

- **Snapshot** — replaces the entire order book for that (exchange, symbol). This happens when an adapter first connects or reconnects.
- **Delta** — update the existing order book.

### Apply

The Apply method is the only place in the system that touch the order book state. This single writer design avoids data races by construction — no locks are needed on the book .because only one goroutine ever writes to it.

### Broadcast to WebSocket Clients

After every Apply, the store broadcasts the incoming update to all registered subscribers. Each connected WebSocket client registers a channel with the store when it connects. The store holds a subscribers map of these channels.

Broadcast loops through the subscribers map and sends the update to each channel . 

### Per-client Channel

Each WebSocket client owns a  channel. The store writes to it via broadcast. The client's writeMessage goroutine reads from this channel and writes to the WebSocket connection.

### Snapshot on Connect

When a new client connects, the WS manager calls SnapshotAll on the store. This atomically returns the current state of all order books and registers the client's channel for future broadcasts. The manager writes the snapshots directly to the socket, then the client's writeMessage takes over for all incoming updates. This guarantees no updates are missed between the initial snapshot and the start of live streaming.
