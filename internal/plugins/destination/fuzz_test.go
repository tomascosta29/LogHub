package destination

import (
	"testing"
	"time"

	"loghub/internal/core"
)

// FuzzSentinelTimeGenerated checks the Sentinel timestamp mapping never
// panics on arbitrary event_time values or zero timestamps.
// Run with: go test ./internal/plugins/destination -fuzz=FuzzSentinelTimeGenerated -fuzztime=30s
func FuzzSentinelTimeGenerated(f *testing.F) {
	f.Add("2026-05-06T12:00:00Z", false)
	f.Add("", false)
	f.Add("not-a-time", false)
	f.Add("", true)
	f.Fuzz(func(t *testing.T, eventTime string, zeroTimestamp bool) {
		entry := &core.LogEntry{Fields: map[string]string{"event_time": eventTime}}
		if zeroTimestamp {
			entry.Timestamp = time.Now()
		}
		_, _ = sentinelTimeGenerated(entry)
	})
}
