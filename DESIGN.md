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

