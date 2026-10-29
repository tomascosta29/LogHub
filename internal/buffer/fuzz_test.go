package buffer

import (
	"encoding/json"
	"testing"
)

// FuzzDiskBatchDecode checks the disk-buffer record decoder never panics on
// arbitrary journal payloads, including the legacy and replacement shapes.
// Run with: go test ./internal/buffer -fuzz=FuzzDiskBatchDecode -fuzztime=30s
func FuzzDiskBatchDecode(f *testing.F) {
	f.Add([]byte(`{"entries":[],"next_cursor":"1"}`))
	f.Add([]byte(`{"replaces_seq":1,"batch":{"entries":[],"next_cursor":"2"}}`))
	f.Add([]byte(`{"delivered":1}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte("not json"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if delivered, ok := parseDeliveredMarker(data); ok && delivered <= 0 {
			t.Fatalf("accepted delivered marker %d", delivered)
		}
		var batch DiskBatch
		_ = json.Unmarshal(data, &batch)
		var rec replacementRecord
		_ = json.Unmarshal(data, &rec)
	})
}
