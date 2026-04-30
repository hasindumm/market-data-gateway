## 2026-04-22 — Structuring the codebase 

**Goal:** Structure the codebase 

**What worked:**
- Structured the code base according to hexagonal pattern

**What broke (and why):**
- there is not a breaking things at this stage since its a design decision

**Concept unlocked:**
- learned how to arrange the code base according to the hexagonal pattern 

**Still fuzzy:**
- as the research did found hexagonal as good way to arrange code in order to allign with go ideomatic way. but still not sure whether there are better ways to arrange the code than hexoganl
- even in hexogonal there is a confusion about the ports , that conflict with the idemotic way of go about the defining interfaces 
- in go ideomatic way interfaces are defined and uses at the consumer side when it wants , but hexogonal says inbound/outbound ports define as interface (as i found since hexoganal is broad pattern that can be used over many languages , when it comes to go , i decides not to use ports and use other parts of hexoganl to be allign with idematic way of go)


**Next:**
- need to decide where to start development 
- decided to first implemet for binance and then should be able to plug kraken seemlessly 

## 2026-04-23 — implementation of web socket server

**Goal:** implement simple websocker manager and client 

**What worked:**
- client read/write and manager functions serveWS and wireup with clients fucntions

**What broke (and why):**
- inside serveWS as a defer connection.close applied it. so it disconnect socket right after connects.

**Concept unlocked:**
- websocket connection should close at remove client or shut down or issue first occur in write massage
- closing same chanel twice panics

**Still fuzzy:**
- still not much clear about the shut down at ctrl+c should handle


**Next:**
- gracefull shut down (clear resources properly related to the websocket server implmentation)


## 2026-04-23 — implementation of web socket server

**Goal:** implement gracefull shutdown in websocket server 

**What worked:**
- added waitGroup into manager to track active go readMessage routines
- Shutdown() closing all conns first then wg.Wait()
- signal handling in main with os.Interrupt, running ListenAndServe in a goroutine
  so main stays free to wait on the signal channel


**What broke (and why):**
- initially didn't understand why ListenAndServe need to be in a goroutine it blocks forever, so without the goroutine the signal handling code never runs

**Concept unlocked:**
- waitGroup behavahiour , how conters works and when to use add , done and wait
- shutdown has an order, stop new connections first, then signal existing ones,
  then wait.

**Still fuzzy:**
- context propagation, understand the concept but not sure how it threads through
  when adapters come in


**Next:**
- add context to manager and wire it through when building exchange adapters



## 2026-04-24 — implementation of domain data shapes and binance adapter snapshot fetch

**Goal:** implement domain data shapes and binance adapter snapshot fetch

**What worked:**
- created domain types
- fetch snapshot data from binance
- covert binance response to unified model


**What broke (and why):**
- nothing broke today, but had confusion about where type conversion should happen — 
  initially thought core should convert, realised it's the adapter's responsibility

**Concept unlocked:**
- strconv.ParseFloat like methods behaviours
- http.NewRequestWithContext how to use
- Why conversion happens in adapter not core

**Still fuzzy:**
- How WS stream updates will flow into core?


**Next:**
- implement Binance WebSocket stream to receive live order book updates


## 2026-04-27 — implementation of pipeline/ dummy exchange /fan-in design /interface design and wireup testing at main

**Goal:** implement pipeline and use fan in design to data flow 
implemet proper interface design at the consumer of the exchanges 
implement a dummy exchange to test things and properly design (future exchnages should be able to plug and play)
test wiring up at the main 

**What worked:**
- data flows through dummy exchanges channels to the one merge channel
- wires things (store, pipeline and exchanges)


**What broke (and why):**
- applying changes into store seems broke

**Concept unlocked:**
- fan-in design patter
- interface usage at the consumer side

**Still fuzzy:**
- applying changes flows through the merge channel to store logic seems in correct and fuzzy 


**Next:**
- implement solid way to apply changes flows through the merge channel to store 


## 2026-04-28 — Store refactor, domain refactor, pipeline wiring, WebSocket manager refactor

**Goal:**
Implement a solid store that applies order book changes from the merged channel.
Refactor domain types to support the full update lifecycle.
Wire the pipeline correctly so merged channel data flows into the store and broadcasts to clients.
Refactor WebSocket client and manager to match the broadcast design.

**What worked:**
- Store now correctly applies snapshots and deltas from the merged channel
- Each WebSocket client has its own buffered channel, writeMessage go routine reads from it and writes to socket
- SnapshotAll atomically returns current book state and registers client for future broadcasts
- Pipeline now correctly feeds merged channel into store 
- Domain types refactored — Update and Level now have proper fields and JSON tags to match wire format
- WebSocket manager refactored — on connect sends snapshots directly then handover  updated writing to writeMessage

**What broke (and why):**
- Binance adapter commented out temporarily — adapter design needs to be validated against solid store and pipeline before wiring back in

**Concept unlocked:**
- Single-writer pattern :  one goroutine owns all mutations, eliminates races by design
- Non-blocking broadcast with per-client channels :  decouples store write speed from socket write speed
- Two paths for snapshot delivery :  initial snapshots go directly to socket on connect, future snapshots flow through client channel via broadcast from store

**Still fuzzy:**
- still bit confusing about design inside binance adapter (since per symbol need seperate ws connection)

**Next:**
- Plug Binance adapter back in and validate end to end with real exchange data and refactor design more with any identified changes with development


## 2026-04-29 — Binance adapter integration and testing

**Goal:**
Integrate the Binance adapter into the app and test the full end to end flow.

**What worked:**
- Binance adapter wired up and running end to end
- Data flows from Binance all the way through to the WebSocket client
- Full pipeline confirmed working  exchange to merged channel to store to broadcast to client

**What broke (and why):**
- used wrong design for drain and buffer the updates from binace. ended up with never ending for loop.
**Concept unlocked:**
-  Used a separate goroutine for buffering incoming messages and sent a signal after the snapshot was received. this helps app to flow with non blocking operation (app will not hand in for loops). having forever running for loops in main thread is blocking. should carefully handle that kind of scenarios

**Still fuzzy:**
- should app validate the configs we enter (ex: BTCUSDT is the valid symbol for API , if we enter wrong symbol for it API will return 400.)
- should we keep valid symbols with us and validate against it or its users resposibililty to enter valid symbols 

**Next:**
- Kraken adapter integration

## 2026-04-30 — kraken adapter integration and subscribe to symbols by client development 

**Goal:**
Integrate the kraken adapter into the app and subscribe to symbols by client development 

**What worked:**
- kraken adapter wired up and running end to end
- Data flows from kraken all the way through to the WebSocket client
- subribing to symbols works properly 

**What broke (and why):**
- - WaitGroup panic on client disconnect , m.wg.Add(1) was called but two goroutines (readMessage and writeMessage) were both calling defer m.wg.Done(). When both pumps exited the counter went negative and panicked. Fixed by changing to m.wg.Add(2) to match the two Done calls.
**Concept unlocked:**
- sync.Once  makes sure a function runs only one time, even if many goroutines call it. Good for cleanup code that can be called from more than one place (like both read/write message running cleanup on exit) so we don't crash by closing the same channel twice.

**Still fuzzy:**
- Symbol naming across exchanges , right now if a client wants all BTC updates they have to subscribe to both BTCUSDT and BTC/USD because each exchange uses a different name for the same coin. Not sure whether to convert these to one common name inside the adapters or leave it as it is and just write down the limitation.

**Next:**
- implementation of the CLI tool with CLI commands to view or rebuild the current order book state