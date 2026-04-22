# Structuring the codebase 

### code base structured according to the hexoganal pattern 
- core- holds bussiness logic
- adapters - holds how to deal with outside world
- domain - holds data shapes 

### Project Structure

```
market-data-gateway/
├── cmd/
│   └── main.go
├── internal/
│   ├── adapters/
│   │   ├── exchanges/
│   │   │   ├── binance/
│   │   │   │   └── adapter.go
│   │   │   └── kraken/
│   │   │       └── adapter.go
│   │   └── wsserver/
│   │       └── server.go
│   ├── core/
│   │   └── service.go
│   └── domain/
│       └── market.go
├── meta/
│   └── DIARY.md
├── DESIGN.md
├── go.mod
└── README.md