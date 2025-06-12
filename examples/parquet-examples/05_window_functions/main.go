// Example 5: Complex analytics with window functions (simulated)
// This example demonstrates complex analytical queries on parquet data

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
	"github.com/parquet-go/parquet-go"
)

// StockPrice represents daily stock price data
type StockPrice struct {
	Date   int32   `parquet:"date,date"`
	Symbol string  `parquet:"symbol"`
	Open   float64 `parquet:"open"`
	High   float64 `parquet:"high"`
	Low    float64 `parquet:"low"`
	Close  float64 `parquet:"close"`
	Volume int64   `parquet:"volume"`
}

func main() {
	// Clean up any existing files
	os.Remove("stock_prices.parquet")

	// Create sample stock data
	if err := createStockData(); err != nil {
		log.Fatal("Failed to create stock data:", err)
	}
	defer os.Remove("stock_prices.parquet")

	// Open database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Setup database
	if err := setupDatabase(db); err != nil {
		log.Fatal("Failed to setup database:", err)
	}

	fmt.Println("=== Stock Market Analytics ===")

	// Analysis 1: Price movements
	fmt.Println("1. Recent price movements:")
	if err := showPriceMovements(db); err != nil {
		log.Fatal("Analysis 1 failed:", err)
	}

	// Analysis 2: Volume analysis
	fmt.Println("\n2. Volume analysis by symbol:")
	if err := analyzeVolume(db); err != nil {
		log.Fatal("Analysis 2 failed:", err)
	}

	// Analysis 3: Moving averages (simulated window function)
	fmt.Println("\n3. 5-day moving averages:")
	if err := calculateMovingAverages(db); err != nil {
		log.Fatal("Analysis 3 failed:", err)
	}

	// Analysis 4: Rank by performance
	fmt.Println("\n4. Stock performance ranking:")
	if err := rankByPerformance(db); err != nil {
		log.Fatal("Analysis 4 failed:", err)
	}

	fmt.Println("\nExample completed successfully!")
}

func createStockData() error {
	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	
	// Generate sample stock data for 3 symbols over 20 days
	var prices []StockPrice
	
	for day := 0; day < 20; day++ {
		date := int32(baseDate.AddDate(0, 0, day).Unix() / 86400)
		
		// AAPL
		basePrice := 150.0 + float64(day)*0.5
		prices = append(prices, StockPrice{
			Date:   date,
			Symbol: "AAPL",
			Open:   basePrice,
			High:   basePrice + 2.0,
			Low:    basePrice - 1.5,
			Close:  basePrice + 0.5,
			Volume: 10000000 + int64(day*100000),
		})
		
		// GOOGL
		basePrice = 130.0 + float64(day)*0.3
		prices = append(prices, StockPrice{
			Date:   date,
			Symbol: "GOOGL",
			Open:   basePrice,
			High:   basePrice + 3.0,
			Low:    basePrice - 2.0,
			Close:  basePrice + 1.0,
			Volume: 8000000 + int64(day*80000),
		})
		
		// MSFT
		basePrice = 380.0 - float64(day)*0.2
		prices = append(prices, StockPrice{
			Date:   date,
			Symbol: "MSFT",
			Open:   basePrice,
			High:   basePrice + 1.5,
			Low:    basePrice - 1.0,
			Close:  basePrice - 0.1,
			Volume: 12000000 - int64(day*50000),
		})
	}

	file, err := os.Create("stock_prices.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[StockPrice](file)
	if _, err := writer.Write(prices); err != nil {
		return err
	}

	return writer.Close()
}

func setupDatabase(db *sql.DB) error {
	// Create table
	_, err := db.Exec(`
		CREATE TABLE stock_prices (
			date DATE,
			symbol VARCHAR,
			open DOUBLE,
			high DOUBLE,
			low DOUBLE,
			close DOUBLE,
			volume BIGINT
		)
	`)
	if err != nil {
		return err
	}

	// Load data
	return loadStockData(db)
}

func loadStockData(db *sql.DB) error {
	file, err := os.Open("stock_prices.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[StockPrice](file)
	defer reader.Close()

	prices := make([]StockPrice, 100)
	for {
		n, err := reader.Read(prices)
		if err != nil && err.Error() == "EOF" {
			if n > 0 {
				goto ProcessRecords
			}
			break
		}
		if n == 0 {
			break
		}

	ProcessRecords:
		for i := 0; i < n; i++ {
			p := prices[i]
			sql := fmt.Sprintf("INSERT INTO stock_prices VALUES (%d, '%s', %f, %f, %f, %f, %d)",
				p.Date, p.Symbol, p.Open, p.High, p.Low, p.Close, p.Volume)
			if _, err := db.Exec(sql); err != nil {
				return err
			}
		}
		
		if err != nil && err.Error() == "EOF" {
			break
		}
	}

	return nil
}

func showPriceMovements(db *sql.DB) error {
	// Get last 5 days of data for each symbol
	symbols := []string{"AAPL", "GOOGL", "MSFT"}
	
	for _, symbol := range symbols {
		fmt.Printf("\n%s:\n", symbol)
		rows, err := db.Query(fmt.Sprintf("SELECT date, close FROM stock_prices WHERE symbol = '%s' LIMIT 5", symbol))
		if err != nil {
			return err
		}
		
		var prevClose float64
		first := true
		
		for rows.Next() {
			var date int32
			var close float64
			if err := rows.Scan(&date, &close); err != nil {
				rows.Close()
				return err
			}
			
			change := ""
			if !first {
				diff := close - prevClose
				pct := (diff / prevClose) * 100
				if diff > 0 {
					change = fmt.Sprintf("+%.2f (%.2f%%)", diff, pct)
				} else {
					change = fmt.Sprintf("%.2f (%.2f%%)", diff, pct)
				}
			}
			
			fmt.Printf("  Date: %d, Close: $%.2f %s\n", date, close, change)
			prevClose = close
			first = false
		}
		rows.Close()
	}
	
	return nil
}

func analyzeVolume(db *sql.DB) error {
	symbols := []string{"AAPL", "GOOGL", "MSFT"}
	
	fmt.Printf("\n%-8s %15s %15s %15s\n", "Symbol", "Total Volume", "Avg Volume", "Max Volume")
	fmt.Println(string(make([]byte, 60)))
	
	for _, symbol := range symbols {
		rows, err := db.Query(fmt.Sprintf("SELECT volume FROM stock_prices WHERE symbol = '%s'", symbol))
		if err != nil {
			return err
		}
		
		var total, max int64
		count := 0
		
		for rows.Next() {
			var volume int64
			if err := rows.Scan(&volume); err != nil {
				rows.Close()
				return err
			}
			total += volume
			if volume > max {
				max = volume
			}
			count++
		}
		rows.Close()
		
		avg := int64(0)
		if count > 0 {
			avg = total / int64(count)
		}
		
		fmt.Printf("%-8s %15d %15d %15d\n", symbol, total, avg, max)
	}
	
	return nil
}

func calculateMovingAverages(db *sql.DB) error {
	// Simulate window function for 5-day moving average
	symbols := []string{"AAPL", "GOOGL", "MSFT"}
	
	for _, symbol := range symbols {
		fmt.Printf("\n%s 5-day moving averages:\n", symbol)
		
		// Get all prices for the symbol
		rows, err := db.Query(fmt.Sprintf("SELECT date, close FROM stock_prices WHERE symbol = '%s'", symbol))
		if err != nil {
			return err
		}
		
		type pricePoint struct {
			date  int32
			close float64
		}
		
		var prices []pricePoint
		for rows.Next() {
			var p pricePoint
			if err := rows.Scan(&p.date, &p.close); err != nil {
				rows.Close()
				return err
			}
			prices = append(prices, p)
		}
		rows.Close()
		
		// Calculate 5-day moving average
		for i := 4; i < len(prices) && i < 10; i++ { // Show first 6 moving averages
			sum := 0.0
			for j := 0; j < 5; j++ {
				sum += prices[i-j].close
			}
			ma := sum / 5.0
			fmt.Printf("  Date: %d, MA(5): $%.2f\n", prices[i].date, ma)
		}
	}
	
	return nil
}

func rankByPerformance(db *sql.DB) error {
	// Calculate performance (last close - first close) / first close
	symbols := []string{"AAPL", "GOOGL", "MSFT"}
	
	type performance struct {
		symbol string
		change float64
		pct    float64
	}
	
	var perfs []performance
	
	for _, symbol := range symbols {
		rows, err := db.Query(fmt.Sprintf("SELECT close FROM stock_prices WHERE symbol = '%s'", symbol))
		if err != nil {
			return err
		}
		
		var closes []float64
		for rows.Next() {
			var close float64
			if err := rows.Scan(&close); err != nil {
				rows.Close()
				return err
			}
			closes = append(closes, close)
		}
		rows.Close()
		
		if len(closes) > 1 {
			first := closes[0]
			last := closes[len(closes)-1]
			change := last - first
			pct := (change / first) * 100
			
			perfs = append(perfs, performance{symbol, change, pct})
		}
	}
	
	// Sort by performance
	sort.Slice(perfs, func(i, j int) bool {
		return perfs[i].pct > perfs[j].pct
	})
	
	fmt.Printf("\n%-8s %15s %15s %8s\n", "Rank", "Symbol", "Change ($)", "Change (%)")
	fmt.Println(string(make([]byte, 50)))
	
	for i, p := range perfs {
		fmt.Printf("%-8d %-15s $%14.2f %7.2f%%\n", i+1, p.symbol, p.change, p.pct)
	}
	
	return nil
}