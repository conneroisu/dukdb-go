// +build ignore

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	
	"github.com/connerohnesorge/dukdb-go/benchmarks"
)

var (
	outputDir   = flag.String("output", "results", "Output directory for benchmark results")
	testFilter  = flag.String("filter", "", "Filter benchmarks by name pattern")
	compareWith = flag.String("compare", "", "Path to previous report for comparison")
	verbose     = flag.Bool("v", false, "Verbose output")
	skipCGO     = flag.Bool("skip-cgo", false, "Skip CGO benchmarks")
	timeout     = flag.Duration("timeout", 30*time.Minute, "Timeout for benchmark execution")
)

func main() {
	flag.Parse()
	
	fmt.Println("=== DuckDB Go Benchmark Suite ===")
	fmt.Printf("Starting at: %s\n\n", time.Now().Format(time.RFC3339))
	
	// Create output directory
	timestamp := time.Now().Format("20060102_150405")
	outputPath := filepath.Join(*outputDir, fmt.Sprintf("benchmark_%s", timestamp))
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output directory: %v\n", err)
		os.Exit(1)
	}
	
	// System info
	systemInfo := benchmarks.SystemInfo{
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPUs:      runtime.NumCPU(),
		GoVersion: runtime.Version(),
	}
	
	// Get git commits if available
	if commit, err := getGitCommit("."); err == nil {
		systemInfo.PureGoCommit = commit
	}
	
	fmt.Printf("System: %s/%s, %d CPUs, %s\n", systemInfo.OS, systemInfo.Arch, systemInfo.CPUs, systemInfo.GoVersion)
	if systemInfo.PureGoCommit != "" {
		fmt.Printf("Pure-Go Commit: %s\n", systemInfo.PureGoCommit)
	}
	fmt.Println()
	
	// Create reporter
	reporter := benchmarks.NewReporter(filepath.Join(outputPath, "report"))
	reporter.SetSystemInfo(systemInfo)
	
	// Benchmark suites to run
	suites := []struct {
		name string
		pkg  string
	}{
		{"Basic Queries", "./basic"},
		{"Analytical Queries", "./analytical"},
		{"Data Loading", "./dataload"},
		// {"TPC-H", "./tpch"}, // TODO: Implement TPC-H benchmarks
	}
	
	// Run benchmarks
	totalStart := time.Now()
	
	for _, suite := range suites {
		fmt.Printf("\n=== Running %s Benchmarks ===\n", suite.name)
		
		// Build test arguments
		args := []string{"test", "-bench=.", "-benchmem", "-run=^$"}
		if *testFilter != "" {
			args = append(args, fmt.Sprintf("-bench=%s", *testFilter))
		}
		if *timeout > 0 {
			args = append(args, fmt.Sprintf("-timeout=%s", timeout.String()))
		}
		if *verbose {
			args = append(args, "-v")
		}
		
		// Set CGO_ENABLED based on flag
		env := os.Environ()
		if *skipCGO {
			env = setEnv(env, "CGO_ENABLED", "0")
			fmt.Println("Running with CGO disabled")
		} else {
			// Try to run with CGO enabled
			env = setEnv(env, "CGO_ENABLED", "1")
		}
		
		// Run benchmark
		cmd := exec.Command("go", append(args, suite.pkg)...)
		cmd.Env = env
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		
		start := time.Now()
		err := cmd.Run()
		duration := time.Since(start)
		
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: %s benchmarks failed: %v\n", suite.name, err)
			// Continue with other suites
		}
		
		fmt.Printf("Completed %s benchmarks in %v\n", suite.name, duration)
		
		// TODO: Parse benchmark output and add to reporter
		// This would require capturing and parsing the benchmark output
		// For now, the benchmarks write their own results
	}
	
	totalDuration := time.Since(totalStart)
	fmt.Printf("\n=== Benchmark Suite Completed ===\n")
	fmt.Printf("Total time: %v\n", totalDuration)
	
	// Generate report
	fmt.Printf("\nGenerating report in: %s\n", outputPath)
	report, err := reporter.GenerateReport()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate report: %v\n", err)
		os.Exit(1)
	}
	
	// Print summary to console
	if err := reporter.PrintReport(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to print report: %v\n", err)
	}
	
	// Compare with previous report if specified
	if *compareWith != "" {
		fmt.Println("\n=== Comparing with Previous Report ===")
		oldReport, err := benchmarks.LoadReport(*compareWith)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load comparison report: %v\n", err)
		} else {
			benchmarks.CompareReports(oldReport, report)
		}
	}
	
	fmt.Printf("\nResults saved to: %s\n", outputPath)
}

func getGitCommit(path string) (string, error) {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	cmd.Dir = path
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func setEnv(env []string, key, value string) []string {
	prefix := key + "="
	for i, e := range env {
		if strings.HasPrefix(e, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}