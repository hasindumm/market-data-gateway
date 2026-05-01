package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"market-data-gateway/internal/config"
	"market-data-gateway/internal/domain"
	"net/http"
	"os"
)

func RunBook(args []string) {
	fs := flag.NewFlagSet("book", flag.ExitOnError)
	symbol := fs.String("symbol", "", "symbol e.g. BTCUSDT")
	exchange := fs.String("exchange", "", "exchange e.g. binance")
	cfgPath := fs.String("config", "config.json", "path to config file")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "book: parse flags: %v\n", err)
		os.Exit(1)
	}
	cfg := config.MustLoad(*cfgPath)

	url := fmt.Sprintf("http://localhost:%d/admin/book?exchange=%s&symbol=%s",
		cfg.Server.AdminPort, *exchange, *symbol)

	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: gateway not running? %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Status)
		os.Exit(1)
	}

	var snap domain.Update
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		fmt.Fprintf(os.Stderr, "decode: %v\n", err)
		os.Exit(1)
	}
	printBook(snap)
}

func printBook(snap domain.Update) {
	fmt.Printf("%s %s @ %s\n\n", snap.Exchange, snap.Symbol, snap.Timestamp.Format("15:04:05.000"))

	fmt.Printf("  %-16s  %-16s\n", "BID PRICE", "BID QTY")
	fmt.Printf("  %-16s  %-16s\n", "---------", "-------")
	for _, b := range snap.Bids {
		fmt.Printf("  %-16s  %-16s\n", b.Price, b.Quantity)
	}

	fmt.Println()

	fmt.Printf("  %-16s  %-16s\n", "ASK PRICE", "ASK QTY")
	fmt.Printf("  %-16s  %-16s\n", "---------", "-------")
	for _, a := range snap.Asks {
		fmt.Printf("  %-16s  %-16s\n", a.Price, a.Quantity)
	}
}
