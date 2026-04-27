package domain

type Update struct {

	UpdateType string
	Symbol string
	Bids []Level
	Asks []Level
}