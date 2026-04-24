package domain

type OrderBook struct {

	Symbol string
	Bids []Level
	Asks []Level
}