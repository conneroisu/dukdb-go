package purego

import (
	"time"
)

// DuckDB epoch for dates (2000-01-01)
var duckdbDateEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// duckdbDateToTime converts DuckDB date (days since 2000-01-01) to time.Time
func duckdbDateToTime(days int32) time.Time {
	return duckdbDateEpoch.AddDate(0, 0, int(days))
}

// timeToDuckDBDate converts time.Time to DuckDB date format
func timeToDuckDBDate(t time.Time) int32 {
	duration := t.Sub(duckdbDateEpoch)
	return int32(duration.Hours() / 24)
}

// duckdbTimeToTime converts DuckDB time (microseconds since midnight) to time.Time
func duckdbTimeToTime(microseconds int64) time.Time {
	// Create a time at midnight and add microseconds
	midnight := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	return midnight.Add(time.Duration(microseconds) * time.Microsecond)
}

// timeToDuckDBTime converts time.Time to DuckDB time format
func timeToDuckDBTime(t time.Time) int64 {
	// Get microseconds since midnight
	hour, min, sec := t.Clock()
	nanos := t.Nanosecond()
	
	totalMicros := int64(hour)*3600*1000000 +
		int64(min)*60*1000000 +
		int64(sec)*1000000 +
		int64(nanos)/1000
		
	return totalMicros
}

// duckdbTimestampToTime converts DuckDB timestamp (microseconds since epoch) to time.Time
func duckdbTimestampToTime(microseconds int64) time.Time {
	seconds := microseconds / 1000000
	nanos := (microseconds % 1000000) * 1000
	return time.Unix(seconds, nanos)
}

// timeToDuckDBTimestamp converts time.Time to DuckDB timestamp format
func timeToDuckDBTimestamp(t time.Time) int64 {
	return t.UnixNano() / 1000 // Convert nanoseconds to microseconds
}