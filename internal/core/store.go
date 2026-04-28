package core

import (
	"context"
	"market-data-gateway/internal/domain"
	"sort"
	"strconv"
	"sync"
	"time"
)

type bookKey struct {
	Exchange string
	Symbol   string
}

type book struct {
	bids map[string]string // price → quantity
	asks map[string]string
}

type Store struct {
	mu          sync.RWMutex
	books       map[bookKey]*book
	subsMu      sync.Mutex
	subscribers map[chan<- domain.Update]struct{}
}

func NewStore() *Store {
	return &Store{
		books:       make(map[bookKey]*book),
		subscribers: make(map[chan<- domain.Update]struct{}),
	}
}

func (s *Store) Run(ctx context.Context, in <-chan domain.Update) {
	for {
		select {
		case <-ctx.Done():
			return
		case u, ok := <-in:
			if !ok {
				return
			}
			s.apply(u)
			s.broadcast(u)
		}
	}
}

func (s *Store) apply(u domain.Update) {
	key := bookKey{Exchange: u.Exchange, Symbol: u.Symbol}
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.books[key]
	if !ok || u.Type == domain.UpdateTypeSnapshot {
		b = &book{
			bids: make(map[string]string),
			asks: make(map[string]string),
		}
		s.books[key] = b
	}

	applyLevels(b.bids, u.Bids)
	applyLevels(b.asks, u.Asks)
}

func applyLevels(side map[string]string, levels []domain.Level) {
	for _, l := range levels {
		if isZeroQty(l.Quantity) {
			delete(side, l.Price)
		} else {
			side[l.Price] = l.Quantity
		}
	}
}

func isZeroQty(qty string) bool {
	f, err := strconv.ParseFloat(qty, 64)
	return err == nil && f == 0
}

func (s *Store) broadcast(u domain.Update) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for ch := range s.subscribers {
		select {
		case ch <- u:
		default:
			// subscriber too slow; drop rather than block the writer
		}
	}
}

func (s *Store) Subscribe(ch chan<- domain.Update) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	s.subscribers[ch] = struct{}{}
}

func (s *Store) Unsubscribe(ch chan<- domain.Update) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	delete(s.subscribers, ch)
}

func (s *Store) SnapshotAll() []domain.Update {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snaps := make([]domain.Update, 0, len(s.books))
	for key, b := range s.books {
		snaps = append(snaps, buildSnapshot(key.Exchange, key.Symbol, b))
	}
	return snaps
}

func buildSnapshot(exchange, symbol string, b *book) domain.Update {
	return domain.Update{
		Type:      domain.UpdateTypeSnapshot,
		Exchange:  exchange,
		Symbol:    symbol,
		Bids:      sortedLevels(b.bids, true),
		Asks:      sortedLevels(b.asks, false),
		Timestamp: time.Now(),
	}
}
func sortedLevels(side map[string]string, descending bool) []domain.Level {
	levels := make([]domain.Level, 0, len(side))
	for price, qty := range side {
		levels = append(levels, domain.Level{Price: price, Quantity: qty})
	}
	sort.Slice(levels, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(levels[i].Price, 64)
		pj, _ := strconv.ParseFloat(levels[j].Price, 64)
		if descending {
			return pi > pj
		}
		return pi < pj
	})
	return levels
}
