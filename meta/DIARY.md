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

