package benchmarks

import (
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
	
	// Import both drivers conditionally based on build tags
	_ "github.com/connerohnesorge/dukdb-go"
)

// BenchmarkConfig holds configuration for benchmark runs
type BenchmarkConfig struct {
	Driver      string // "duckdb-purego" or "duckdb-cgo"
	DataPath    string
	ResultsPath string
	Verbose     bool
}

// BenchmarkRunner manages benchmark execution and comparison
type BenchmarkRunner struct {
	config BenchmarkConfig
	db     *sql.DB
	mu     sync.Mutex
}

// NewBenchmarkRunner creates a new benchmark runner
func NewBenchmarkRunner(config BenchmarkConfig) (*BenchmarkRunner, error) {
	return &BenchmarkRunner{
		config: config,
	}, nil
}

// Connect opens a database connection
func (br *BenchmarkRunner) Connect(dsn string) error {
	br.mu.Lock()
	defer br.mu.Unlock()
	
	if br.db != nil {
		br.db.Close()
	}
	
	db, err := sql.Open(br.config.Driver, dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	
	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}
	
	br.db = db
	return nil
}

// Close closes the database connection
func (br *BenchmarkRunner) Close() error {
	br.mu.Lock()
	defer br.mu.Unlock()
	
	if br.db != nil {
		err := br.db.Close()
		br.db = nil
		return err
	}
	return nil
}

// Execute runs a SQL query and returns execution time
func (br *BenchmarkRunner) Execute(query string, args ...interface{}) (time.Duration, error) {
	start := time.Now()
	_, err := br.db.Exec(query, args...)
	duration := time.Since(start)
	
	if err != nil {
		return 0, err
	}
	return duration, nil
}

// Query runs a SQL query and returns rows and execution time
func (br *BenchmarkRunner) Query(query string, args ...interface{}) (*sql.Rows, time.Duration, error) {
	start := time.Now()
	rows, err := br.db.Query(query, args...)
	duration := time.Since(start)
	
	if err != nil {
		return nil, 0, err
	}
	return rows, duration, nil
}

// BenchmarkResult holds the result of a single benchmark
type BenchmarkResult struct {
	Name         string
	Driver       string
	Duration     time.Duration
	Operations   int
	MemoryAllocs uint64
	MemoryBytes  uint64
	Error        error
}

// BenchmarkComparison compares results between implementations
type BenchmarkComparison struct {
	Name           string
	PureGoResult   *BenchmarkResult
	CGOResult      *BenchmarkResult
	SpeedupFactor  float64 // Positive means pure-go is faster
	MemoryFactor   float64 // Positive means pure-go uses less memory
}

// CompareBenchmarks compares two benchmark results
func CompareBenchmarks(name string, pureGo, cgo *BenchmarkResult) *BenchmarkComparison {
	comp := &BenchmarkComparison{
		Name:         name,
		PureGoResult: pureGo,
		CGOResult:    cgo,
	}
	
	if pureGo != nil && cgo != nil && pureGo.Error == nil && cgo.Error == nil {
		// Calculate speedup factor
		comp.SpeedupFactor = float64(cgo.Duration) / float64(pureGo.Duration) - 1.0
		
		// Calculate memory factor
		if cgo.MemoryBytes > 0 {
			comp.MemoryFactor = float64(cgo.MemoryBytes) / float64(pureGo.MemoryBytes) - 1.0
		}
	}
	
	return comp
}

// RunBenchmark executes a benchmark function and captures metrics
func RunBenchmark(b *testing.B, br *BenchmarkRunner, name string, fn func() error) *BenchmarkResult {
	result := &BenchmarkResult{
		Name:   name,
		Driver: br.config.Driver,
	}
	
	// Capture memory statistics before
	runtime.GC()
	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)
	
	// Run the benchmark
	start := time.Now()
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		if err := fn(); err != nil {
			result.Error = err
			b.Fatal(err)
		}
	}
	
	b.StopTimer()
	result.Duration = time.Since(start)
	result.Operations = b.N
	
	// Capture memory statistics after
	runtime.GC()
	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)
	
	result.MemoryAllocs = memStatsAfter.Mallocs - memStatsBefore.Mallocs
	result.MemoryBytes = memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc
	
	return result
}

// FormatComparison formats a benchmark comparison for display
func FormatComparison(comp *BenchmarkComparison) string {
	if comp.PureGoResult == nil || comp.CGOResult == nil {
		return fmt.Sprintf("%s: incomplete comparison", comp.Name)
	}
	
	speedupStr := "slower"
	if comp.SpeedupFactor > 0 {
		speedupStr = fmt.Sprintf("%.2fx faster", 1+comp.SpeedupFactor)
	} else if comp.SpeedupFactor < 0 {
		speedupStr = fmt.Sprintf("%.2fx slower", 1-comp.SpeedupFactor)
	} else {
		speedupStr = "same speed"
	}
	
	memoryStr := "more memory"
	if comp.MemoryFactor > 0 {
		memoryStr = fmt.Sprintf("%.2fx less memory", 1+comp.MemoryFactor)
	} else if comp.MemoryFactor < 0 {
		memoryStr = fmt.Sprintf("%.2fx more memory", 1-comp.MemoryFactor)
	} else {
		memoryStr = "same memory"
	}
	
	return fmt.Sprintf("%s:\n  Pure-Go: %v (%d B)\n  CGO:     %v (%d B)\n  Comparison: %s, %s",
		comp.Name,
		comp.PureGoResult.Duration, comp.PureGoResult.MemoryBytes,
		comp.CGOResult.Duration, comp.CGOResult.MemoryBytes,
		speedupStr, memoryStr)
}

// BenchmarkSuite represents a collection of related benchmarks
type BenchmarkSuite struct {
	Name        string
	SetupFunc   func(*BenchmarkRunner) error
	TeardownFunc func(*BenchmarkRunner) error
	Benchmarks  []BenchmarkFunc
}

// BenchmarkFunc represents a single benchmark test
type BenchmarkFunc struct {
	Name string
	Func func(*testing.B, *BenchmarkRunner) *BenchmarkResult
}

// RunSuite executes a complete benchmark suite
func RunSuite(b *testing.B, suite *BenchmarkSuite, config BenchmarkConfig) []*BenchmarkResult {
	runner, err := NewBenchmarkRunner(config)
	if err != nil {
		b.Fatalf("Failed to create benchmark runner: %v", err)
	}
	defer runner.Close()
	
	// Connect to database
	if err := runner.Connect(":memory:"); err != nil {
		b.Fatalf("Failed to connect: %v", err)
	}
	
	// Run setup if provided
	if suite.SetupFunc != nil {
		if err := suite.SetupFunc(runner); err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}
	
	// Run all benchmarks
	results := make([]*BenchmarkResult, 0, len(suite.Benchmarks))
	for _, bench := range suite.Benchmarks {
		b.Run(bench.Name, func(b *testing.B) {
			result := bench.Func(b, runner)
			results = append(results, result)
		})
	}
	
	// Run teardown if provided
	if suite.TeardownFunc != nil {
		if err := suite.TeardownFunc(runner); err != nil {
			b.Errorf("Teardown failed: %v", err)
		}
	}
	
	return results
}

// Helper function to measure query execution time with result consumption
func MeasureQuery(br *BenchmarkRunner, query string, args ...interface{}) (time.Duration, int, error) {
	start := time.Now()
	rows, err := br.db.Query(query, args...)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	
	// Consume all rows
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	
	duration := time.Since(start)
	return duration, rowCount, rows.Err()
}

// Helper to create test data
func CreateTestTable(br *BenchmarkRunner, tableName string, rowCount int) error {
	// Drop table if exists
	br.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	
	// Create table
	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			value DOUBLE,
			created_at TIMESTAMP,
			category VARCHAR(50)
		)
	`, tableName)
	
	if _, err := br.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	
	// Insert test data
	tx, err := br.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (id, name, value, created_at, category) VALUES (?, ?, ?, ?, ?)",
		tableName))
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	categories := []string{"A", "B", "C", "D", "E"}
	baseTime := time.Now()
	
	for i := 0; i < rowCount; i++ {
		category := categories[i%len(categories)]
		timestamp := baseTime.Add(time.Duration(i) * time.Second)
		_, err := stmt.Exec(i, fmt.Sprintf("Item_%d", i), float64(i)*1.5, timestamp, category)
		if err != nil {
			return err
		}
	}
	
	return tx.Commit()
}