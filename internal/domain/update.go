package domain

type Update struct {

	Symbol string
	Bids []Level
	Asks []Level
}