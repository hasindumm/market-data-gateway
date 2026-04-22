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
│   │   └── wshandler/
│   ├── core/
│   └── domain/
├── meta/
│   └── DIARY.md
├── DESIGN.md
├── go.mod
└── README.md
```