package core

import (
	"market-data-gateway/internal/domain"
	"sync"
)

type Store struct {
	orderbooks map[string]domain.OrderBook
	mu         sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		orderbooks: make(map[string]domain.OrderBook),
	}

}

func (s *Store) Apply(u domain.Update) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orderbooks[u.Symbol] = domain.OrderBook{
		Symbol: u.Symbol,
		Bids:   u.Bids,
		Asks:   u.Asks,
	}
}

func (s *Store) Get(symbol string) (domain.OrderBook, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	book, ok := s.orderbooks[symbol]
	if !ok {
		return domain.OrderBook{}, false
	}
	return book, true

}
