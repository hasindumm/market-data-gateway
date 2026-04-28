package domain

import "time"

type UpdateType string

const (
	UpdateTypeSnapshot UpdateType = "snapshot"
	UpdateTypeDelta    UpdateType = "delta"
)

type Update struct {
	Type      UpdateType `json:"type"`
	Exchange  string     `json:"exchange"`
	Symbol    string     `json:"symbol"`
	Bids      []Level    `json:"bids"`
	Asks      []Level    `json:"asks"`
	Timestamp time.Time  `json:"timestamp"`
}
