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
