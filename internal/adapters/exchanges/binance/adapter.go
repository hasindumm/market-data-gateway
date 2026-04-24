package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"market-data-gateway/internal/domain"
	"net/http"
	"strconv"
)

func FetchSnapshot(ctx context.Context, symbol string) (domain.OrderBook, error) {

	url := "https://api.binance.com/api/v3/depth?symbol=" + symbol

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("binance: create request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("binance: fetching snapshot failed: %w", err)
	}
	defer resp.Body.Close()

	var binanceResp binanceDepthResponse
	if err := json.NewDecoder(resp.Body).Decode(&binanceResp); err != nil {
		return domain.OrderBook{}, fmt.Errorf("binance: fetching snapshot: response decode failed: %w", err)
	}

	bids, err := parseLevels(binanceResp.Bids)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("binance: parse bids: %w", err)
	}
	asks, err := parseLevels(binanceResp.Asks)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("binance: parse asks: %w", err)
	}
	
	return domain.OrderBook{
		Symbol: symbol,
		Bids:   bids,
		Asks:   asks,
	}, nil

}

type binanceDepthResponse struct {
	Bids [][]string `json:"bids"`
	Asks [][]string `json:"asks"`
}

func parseLevels(raw [][]string) ([]domain.Level, error) {
	levels := make([]domain.Level, 0, len(raw))
	for _, item := range raw {
		price, err := strconv.ParseFloat(item[0], 64)
		if err != nil {
			return nil, fmt.Errorf("parse price: %w", err)
		}
		qty, err := strconv.ParseFloat(item[1], 64)
		if err != nil {
			return nil, fmt.Errorf("parse quantity: %w", err)
		}
		levels = append(levels, domain.Level{Price: price, Quantity: qty})
	}
	return levels, nil
}
