package benchmarks

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// BenchmarkReport represents a complete benchmark report
type BenchmarkReport struct {
	Timestamp   time.Time                       `json:"timestamp"`
	System      SystemInfo                      `json:"system"`
	Comparisons map[string]*BenchmarkComparison `json:"comparisons"`
	Summary     *ReportSummary                  `json:"summary"`
}

// SystemInfo contains system information
type SystemInfo struct {
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	CPUs         int    `json:"cpus"`
	GoVersion    string `json:"go_version"`
	PureGoCommit string `json:"purego_commit,omitempty"`
	CGOCommit    string `json:"cgo_commit,omitempty"`
}

// ReportSummary provides overall benchmark summary
type ReportSummary struct {
	TotalBenchmarks      int     `json:"total_benchmarks"`
	PureGoFaster         int     `json:"purego_faster"`
	CGOFaster            int     `json:"cgo_faster"`
	Similar              int     `json:"similar"`
	Failed               int     `json:"failed"`
	AverageSpeedup       float64 `json:"average_speedup"`
	AverageMemorySavings float64 `json:"average_memory_savings"`
}

// Reporter handles benchmark result reporting
type Reporter struct {
	results     map[string]*BenchmarkComparison
	systemInfo  SystemInfo
	outputPath  string
}

// NewReporter creates a new benchmark reporter
func NewReporter(outputPath string) *Reporter {
	return &Reporter{
		results:    make(map[string]*BenchmarkComparison),
		outputPath: outputPath,
	}
}

// AddComparison adds a benchmark comparison to the report
func (r *Reporter) AddComparison(comp *BenchmarkComparison) {
	r.results[comp.Name] = comp
}

// SetSystemInfo sets system information for the report
func (r *Reporter) SetSystemInfo(info SystemInfo) {
	r.systemInfo = info
}

// GenerateReport generates the final benchmark report
func (r *Reporter) GenerateReport() (*BenchmarkReport, error) {
	report := &BenchmarkReport{
		Timestamp:   time.Now(),
		System:      r.systemInfo,
		Comparisons: r.results,
		Summary:     r.calculateSummary(),
	}
	
	// Save to file if output path specified
	if r.outputPath != "" {
		if err := r.saveToFile(report); err != nil {
			return nil, err
		}
	}
	
	return report, nil
}

// PrintReport prints a human-readable report to the given writer
func (r *Reporter) PrintReport(w io.Writer) error {
	report, err := r.GenerateReport()
	if err != nil {
		return err
	}
	
	fmt.Fprintf(w, "\n=== DuckDB Go Benchmark Report ===\n")
	fmt.Fprintf(w, "Timestamp: %s\n", report.Timestamp.Format(time.RFC3339))
	fmt.Fprintf(w, "System: %s/%s, %d CPUs, Go %s\n\n",
		report.System.OS, report.System.Arch, report.System.CPUs, report.System.GoVersion)
	
	// Print summary
	fmt.Fprintf(w, "=== Summary ===\n")
	fmt.Fprintf(w, "Total Benchmarks: %d\n", report.Summary.TotalBenchmarks)
	fmt.Fprintf(w, "Pure-Go Faster: %d (%.1f%%)\n",
		report.Summary.PureGoFaster,
		float64(report.Summary.PureGoFaster)/float64(report.Summary.TotalBenchmarks)*100)
	fmt.Fprintf(w, "CGO Faster: %d (%.1f%%)\n",
		report.Summary.CGOFaster,
		float64(report.Summary.CGOFaster)/float64(report.Summary.TotalBenchmarks)*100)
	fmt.Fprintf(w, "Similar Performance: %d (%.1f%%)\n",
		report.Summary.Similar,
		float64(report.Summary.Similar)/float64(report.Summary.TotalBenchmarks)*100)
	
	if report.Summary.AverageSpeedup > 0 {
		fmt.Fprintf(w, "Average Speedup: %.2fx faster\n", 1+report.Summary.AverageSpeedup)
	} else {
		fmt.Fprintf(w, "Average Speedup: %.2fx slower\n", 1-report.Summary.AverageSpeedup)
	}
	
	fmt.Fprintf(w, "\n=== Detailed Results ===\n")
	
	// Sort benchmarks by name
	var names []string
	for name := range report.Comparisons {
		names = append(names, name)
	}
	sort.Strings(names)
	
	// Create table writer
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "Benchmark\tPure-Go\tCGO\tSpeedup\tMemory")
	fmt.Fprintln(tw, "---------\t-------\t---\t-------\t------")
	
	for _, name := range names {
		comp := report.Comparisons[name]
		if comp.PureGoResult == nil || comp.CGOResult == nil {
			continue
		}
		
		speedup := formatSpeedup(comp.SpeedupFactor)
		memory := formatMemory(comp.MemoryFactor)
		
		fmt.Fprintf(tw, "%s\t%v\t%v\t%s\t%s\n",
			name,
			comp.PureGoResult.Duration,
			comp.CGOResult.Duration,
			speedup,
			memory)
	}
	
	tw.Flush()
	
	// Print failures if any
	if report.Summary.Failed > 0 {
		fmt.Fprintf(w, "\n=== Failed Benchmarks ===\n")
		for _, name := range names {
			comp := report.Comparisons[name]
			if comp.PureGoResult != nil && comp.PureGoResult.Error != nil {
				fmt.Fprintf(w, "%s (Pure-Go): %v\n", name, comp.PureGoResult.Error)
			}
			if comp.CGOResult != nil && comp.CGOResult.Error != nil {
				fmt.Fprintf(w, "%s (CGO): %v\n", name, comp.CGOResult.Error)
			}
		}
	}
	
	return nil
}

// PrintCSV prints results in CSV format
func (r *Reporter) PrintCSV(w io.Writer) error {
	report, err := r.GenerateReport()
	if err != nil {
		return err
	}
	
	// Header
	fmt.Fprintln(w, "Benchmark,Pure-Go Duration (ns),CGO Duration (ns),Speedup Factor,Memory Factor,Pure-Go Memory (bytes),CGO Memory (bytes)")
	
	// Sort benchmarks
	var names []string
	for name := range report.Comparisons {
		names = append(names, name)
	}
	sort.Strings(names)
	
	// Data rows
	for _, name := range names {
		comp := report.Comparisons[name]
		if comp.PureGoResult == nil || comp.CGOResult == nil {
			continue
		}
		
		fmt.Fprintf(w, "%s,%d,%d,%.4f,%.4f,%d,%d\n",
			name,
			comp.PureGoResult.Duration.Nanoseconds(),
			comp.CGOResult.Duration.Nanoseconds(),
			comp.SpeedupFactor,
			comp.MemoryFactor,
			comp.PureGoResult.MemoryBytes,
			comp.CGOResult.MemoryBytes)
	}
	
	return nil
}

// Helper functions

func (r *Reporter) calculateSummary() *ReportSummary {
	summary := &ReportSummary{}
	
	var totalSpeedup, totalMemory float64
	validComparisons := 0
	
	for _, comp := range r.results {
		summary.TotalBenchmarks++
		
		if comp.PureGoResult == nil || comp.CGOResult == nil {
			summary.Failed++
			continue
		}
		
		if comp.PureGoResult.Error != nil || comp.CGOResult.Error != nil {
			summary.Failed++
			continue
		}
		
		// Count performance comparison
		const threshold = 0.05 // 5% threshold for "similar"
		if comp.SpeedupFactor > threshold {
			summary.PureGoFaster++
		} else if comp.SpeedupFactor < -threshold {
			summary.CGOFaster++
		} else {
			summary.Similar++
		}
		
		totalSpeedup += comp.SpeedupFactor
		totalMemory += comp.MemoryFactor
		validComparisons++
	}
	
	if validComparisons > 0 {
		summary.AverageSpeedup = totalSpeedup / float64(validComparisons)
		summary.AverageMemorySavings = totalMemory / float64(validComparisons)
	}
	
	return summary
}

func (r *Reporter) saveToFile(report *BenchmarkReport) error {
	// Create results directory if needed
	dir := filepath.Dir(r.outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	
	// Save JSON report
	jsonPath := strings.TrimSuffix(r.outputPath, filepath.Ext(r.outputPath)) + ".json"
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	
	encoder := json.NewEncoder(jsonFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	
	// Save human-readable report
	txtPath := strings.TrimSuffix(r.outputPath, filepath.Ext(r.outputPath)) + ".txt"
	txtFile, err := os.Create(txtPath)
	if err != nil {
		return err
	}
	defer txtFile.Close()
	
	if err := r.PrintReport(txtFile); err != nil {
		return err
	}
	
	// Save CSV report
	csvPath := strings.TrimSuffix(r.outputPath, filepath.Ext(r.outputPath)) + ".csv"
	csvFile, err := os.Create(csvPath)
	if err != nil {
		return err
	}
	defer csvFile.Close()
	
	if err := r.PrintCSV(csvFile); err != nil {
		return err
	}
	
	return nil
}

func formatSpeedup(factor float64) string {
	if factor > 0.01 {
		return fmt.Sprintf("%.2fx faster", 1+factor)
	} else if factor < -0.01 {
		return fmt.Sprintf("%.2fx slower", 1-factor)
	}
	return "~same"
}

func formatMemory(factor float64) string {
	if factor > 0.01 {
		return fmt.Sprintf("%.2fx less", 1+factor)
	} else if factor < -0.01 {
		return fmt.Sprintf("%.2fx more", 1-factor)
	}
	return "~same"
}

// LoadReport loads a benchmark report from file
func LoadReport(path string) (*BenchmarkReport, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	var report BenchmarkReport
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&report); err != nil {
		return nil, err
	}
	
	return &report, nil
}

// CompareReports compares two benchmark reports
func CompareReports(old, new *BenchmarkReport) {
	fmt.Println("\n=== Benchmark Comparison ===")
	fmt.Printf("Old: %s\n", old.Timestamp.Format(time.RFC3339))
	fmt.Printf("New: %s\n\n", new.Timestamp.Format(time.RFC3339))
	
	// Find regressions and improvements
	var regressions, improvements []string
	
	for name, newComp := range new.Comparisons {
		oldComp, exists := old.Comparisons[name]
		if !exists {
			continue
		}
		
		if newComp.PureGoResult == nil || oldComp.PureGoResult == nil {
			continue
		}
		
		// Compare Pure-Go performance
		oldDuration := oldComp.PureGoResult.Duration
		newDuration := newComp.PureGoResult.Duration
		change := float64(newDuration-oldDuration) / float64(oldDuration) * 100
		
		if change > 10 { // 10% regression threshold
			regressions = append(regressions, fmt.Sprintf("%s: %.1f%% slower", name, change))
		} else if change < -10 { // 10% improvement threshold
			improvements = append(improvements, fmt.Sprintf("%s: %.1f%% faster", name, -change))
		}
	}
	
	if len(regressions) > 0 {
		fmt.Println("Regressions:")
		for _, r := range regressions {
			fmt.Printf("  - %s\n", r)
		}
	}
	
	if len(improvements) > 0 {
		fmt.Println("\nImprovements:")
		for _, i := range improvements {
			fmt.Printf("  - %s\n", i)
		}
	}
	
	if len(regressions) == 0 && len(improvements) == 0 {
		fmt.Println("No significant performance changes detected.")
	}
}