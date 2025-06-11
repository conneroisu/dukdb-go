package types

// Interval represents a time interval in DuckDB
type Interval struct {
	Months int32
	Days   int32
	Micros int64
}

// String returns a string representation of the interval
func (i Interval) String() string {
	// Simple representation - could be enhanced
	return "INTERVAL"
}