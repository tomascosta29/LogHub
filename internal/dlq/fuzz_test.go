package dlq

import (
	"encoding/json"
	"testing"
)

// FuzzFailedBatchDecode checks the DLQ record decoder never panics on
// arbitrary journal payloads.
// Run with: go test ./internal/dlq -fuzz=FuzzFailedBatchDecode -fuzztime=30s
func FuzzFailedBatchDecode(f *testing.F) {
	f.Add([]byte(`{"timestamp":"2026-05-06T12:00:00Z","entries":[]}`))
	f.Add([]byte(`{"entries":[{"fields":{"id":"1"}}]}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`null`))
	f.Add([]byte("not json"))
	f.Fuzz(func(t *testing.T, data []byte) {
		var fb FailedBatch
		_ = json.Unmarshal(data, &fb)
	})
}
