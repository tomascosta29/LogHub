package mssqlsentinel_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"loghub/internal/core"
	"loghub/internal/pipeline"
	"loghub/internal/plugins/destination"
)

type mockMSSQLSource struct {
	mu        sync.Mutex
	rows      []*core.LogEntry
	batchSize int
}

// mockMSSQLSource intentionally models MSSQL pagination by (event_time, id), so
// duplicate timestamp boundaries exercise the same correctness contract as the
// real source's cursor_column support.
func (s *mockMSSQLSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	parsed := parseMockCursor(cursor)
	start := sort.Search(len(s.rows), func(i int) bool {
		ts := s.rows[i].Fields["event_time"]
		id := s.rows[i].Fields["id"]
		return ts > parsed.Timestamp || (ts == parsed.Timestamp && id > parsed.Tie)
	})
	if start >= len(s.rows) {
		return nil, cursor, nil
	}

	end := start + s.batchSize
	if end > len(s.rows) {
		end = len(s.rows)
	}
	batch := cloneEntries(s.rows[start:end])
	last := batch[len(batch)-1]
	return batch, formatMockCursor(last.Fields["event_time"], last.Fields["id"]), nil
}

func (s *mockMSSQLSource) Close() error { return nil }
func (s *mockMSSQLSource) Name() string {
	return "mock-mssql-audit"
}

type mockCursor struct {
	Timestamp string `json:"timestamp"`
	Tie       string `json:"tie"`
}

func parseMockCursor(cursor string) mockCursor {
	if cursor == "" {
		return mockCursor{}
	}
	var parsed mockCursor
	if err := json.Unmarshal([]byte(cursor), &parsed); err == nil {
		return parsed
	}
	return mockCursor{Timestamp: cursor}
}

func formatMockCursor(timestamp, id string) string {
	data, _ := json.Marshal(mockCursor{Timestamp: timestamp, Tie: id})
	return string(data)
}

type memoryCheckpointer struct {
	mu     sync.Mutex
	cursor string
}

func (c *memoryCheckpointer) Get(pipelineID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cursor == "" {
		return "", core.ErrCursorNotFound
	}
	return c.cursor, nil
}

func (c *memoryCheckpointer) Set(pipelineID string, cursor string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursor = cursor
	return nil
}

func (c *memoryCheckpointer) Close() error { return nil }

type fakeSentinelTokenProvider struct {
	token string
}

func (p fakeSentinelTokenProvider) GetToken(ctx context.Context) (string, error) {
	return p.token, nil
}

type mockSentinel struct {
	server        *httptest.Server
	mu            sync.Mutex
	received      map[string]map[string]string
	duplicates    map[string]int
	batches       int
	failuresLeft  atomic.Int32
	maxBatchSize  int
	expectedPath  string
	expectedToken string
}

func newMockSentinel(t *testing.T, failures int32, maxBatchSize int) *mockSentinel {
	t.Helper()
	s := &mockSentinel{
		received:      make(map[string]map[string]string),
		duplicates:    make(map[string]int),
		maxBatchSize:  maxBatchSize,
		expectedPath:  "/dataCollectionRules/dcr-immutable-id/streams/Custom-MSSQLAudit_CL",
		expectedToken: "Bearer fake-token",
	}
	s.failuresLeft.Store(failures)
	s.server = httptest.NewServer(http.HandlerFunc(s.handle(t)))
	return s
}

func (s *mockSentinel) handle(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != s.expectedPath {
			t.Errorf("unexpected Sentinel ingestion path: %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("api-version"); got != "2023-01-01" {
			t.Errorf("unexpected api-version: %q", got)
		}
		if got := r.Header.Get("Authorization"); got != s.expectedToken {
			t.Errorf("unexpected Authorization header: %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("unexpected Content-Type: %q", got)
		}

		var entries []map[string]string
		if err := json.NewDecoder(r.Body).Decode(&entries); err != nil {
			t.Errorf("failed to decode request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if s.maxBatchSize > 0 && len(entries) > s.maxBatchSize {
			t.Errorf("batch exceeded mock Sentinel limit: got %d, max %d", len(entries), s.maxBatchSize)
		}

		if s.failuresLeft.Add(-1) >= 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		s.batches++
		for _, entry := range entries {
			id := entry["id"]
			if entry["TimeGenerated"] == "" {
				t.Errorf("missing TimeGenerated for id %q", id)
			}
			if _, exists := s.received[id]; exists {
				s.duplicates[id]++
			}
			s.received[id] = entry
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *mockSentinel) close() {
	s.server.Close()
}

func (s *mockSentinel) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

func (s *mockSentinel) duplicateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, count := range s.duplicates {
		total += count
	}
	return total
}

func (s *mockSentinel) hasAllIDs(total int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.received) != total {
		return false
	}
	for i := 1; i <= total; i++ {
		if _, ok := s.received[fmt.Sprintf("%06d", i)]; !ok {
			return false
		}
	}
	return true
}

func TestMockMSSQLToMockSentinelContract(t *testing.T) {
	sentinel := newMockSentinel(t, 0, 100)
	defer sentinel.close()

	dest, err := destination.NewSentinelWithTokenProvider(sentinel.server.URL, "dcr-immutable-id", "Custom-MSSQLAudit_CL", "2s", fakeSentinelTokenProvider{token: "fake-token"})
	if err != nil {
		t.Fatalf("failed to create mock Sentinel destination: %v", err)
	}

	cp := &memoryCheckpointer{}
	p := pipeline.NewPipeline(
		"mock-mssql-to-sentinel",
		&mockMSSQLSource{
			rows:      makeMockRows(2, 1),
			batchSize: 2,
		},
		nil,
		[]pipeline.DestinationInfo{{Destination: dest, MaxRetries: 1}},
		cp,
		&pipeline.PipelineSettings{
			PollInterval:       10 * time.Millisecond,
			BufferSize:         1,
			MaxRetries:         1,
			RetryBaseBackoff:   time.Millisecond,
			CheckpointInterval: 0,
			ShutdownTimeout:    time.Second,
		},
		nil,
		nil,
	)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("pipeline start failed: %v", err)
	}
	defer p.Stop()

	waitFor(t, 2*time.Second, func() bool {
		return sentinel.count() == 2
	}, "mock Sentinel did not receive the expected batch")

	sentinel.mu.Lock()
	first := sentinel.received["000001"]
	second := sentinel.received["000002"]
	sentinel.mu.Unlock()
	if first["user_name"] != "user-000001" || second["action"] != "login" {
		t.Fatalf("unexpected MSSQL fields in Sentinel payload: first=%+v second=%+v", first, second)
	}

	waitFor(t, 2*time.Second, func() bool {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		cursor := cp.cursor
		return cursor == formatMockCursor("2026-05-06T12:00:01Z", "000002")
	}, "checkpoint was not saved with MSSQL cursor")
}

func TestMockMSSQLToMockSentinelLargePagedNoDuplicates(t *testing.T) {
	const totalRows = 2500
	const sourceBatchSize = 200

	sentinel := newMockSentinel(t, 0, sourceBatchSize)
	defer sentinel.close()

	dest, err := destination.NewSentinelWithTokenProvider(sentinel.server.URL, "dcr-immutable-id", "Custom-MSSQLAudit_CL", "3s", fakeSentinelTokenProvider{token: "fake-token"})
	if err != nil {
		t.Fatalf("failed to create mock Sentinel destination: %v", err)
	}

	cp := &memoryCheckpointer{}
	p := pipeline.NewPipeline(
		"mock-mssql-large-to-sentinel",
		&mockMSSQLSource{
			rows:      makeMockRows(totalRows, 25),
			batchSize: sourceBatchSize,
		},
		nil,
		[]pipeline.DestinationInfo{{Destination: dest, MaxRetries: 2}},
		cp,
		&pipeline.PipelineSettings{
			PollInterval:       time.Millisecond,
			BufferSize:         4,
			MaxRetries:         2,
			RetryBaseBackoff:   time.Millisecond,
			CheckpointInterval: 0,
			ShutdownTimeout:    5 * time.Second,
		},
		nil,
		nil,
	)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("pipeline start failed: %v", err)
	}
	defer p.Stop()

	waitFor(t, 5*time.Second, func() bool {
		return sentinel.hasAllIDs(totalRows)
	}, "mock Sentinel did not receive all unique MSSQL rows")

	if dupes := sentinel.duplicateCount(); dupes != 0 {
		t.Fatalf("expected no duplicate IDs at Sentinel, got %d", dupes)
	}
}

func TestMockMSSQLToMockSentinelRetriesWithoutDuplicates(t *testing.T) {
	const totalRows = 400
	const sourceBatchSize = 100

	sentinel := newMockSentinel(t, 1, sourceBatchSize)
	defer sentinel.close()

	dest, err := destination.NewSentinelWithTokenProvider(sentinel.server.URL, "dcr-immutable-id", "Custom-MSSQLAudit_CL", "3s", fakeSentinelTokenProvider{token: "fake-token"})
	if err != nil {
		t.Fatalf("failed to create mock Sentinel destination: %v", err)
	}

	p := pipeline.NewPipeline(
		"mock-mssql-retry-to-sentinel",
		&mockMSSQLSource{
			rows:      makeMockRows(totalRows, 10),
			batchSize: sourceBatchSize,
		},
		nil,
		[]pipeline.DestinationInfo{{Destination: dest, MaxRetries: 4}},
		&memoryCheckpointer{},
		&pipeline.PipelineSettings{
			PollInterval:       time.Millisecond,
			BufferSize:         2,
			MaxRetries:         4,
			RetryBaseBackoff:   time.Millisecond,
			CheckpointInterval: 0,
			ShutdownTimeout:    5 * time.Second,
		},
		nil,
		nil,
	)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("pipeline start failed: %v", err)
	}
	defer p.Stop()

	waitFor(t, 5*time.Second, func() bool {
		return sentinel.hasAllIDs(totalRows)
	}, "mock Sentinel did not receive all rows after retry")

	if dupes := sentinel.duplicateCount(); dupes != 0 {
		t.Fatalf("expected no duplicate IDs after transient retry, got %d", dupes)
	}
}

func makeMockRows(total, duplicateTimestampGroupSize int) []*core.LogEntry {
	rows := make([]*core.LogEntry, 0, total)
	base := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= total; i++ {
		group := (i - 1) / duplicateTimestampGroupSize
		ts := base.Add(time.Duration(group) * time.Second)
		id := fmt.Sprintf("%06d", i)
		action := "login"
		level := "INFO"
		message := "login succeeded"
		if i%7 == 0 {
			action = "denied"
			level = "WARN"
			message = "permission denied"
		}
		rows = append(rows, &core.LogEntry{
			Timestamp: ts,
			Source:    "mock-mssql-audit",
			Level:     level,
			Message:   message,
			Fields: map[string]string{
				"id":         id,
				"event_time": ts.Format(time.RFC3339),
				"user_name":  "user-" + id,
				"action":     action,
			},
		})
	}
	return rows
}

func cloneEntries(entries []*core.LogEntry) []*core.LogEntry {
	cloned := make([]*core.LogEntry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, core.CopyLogEntry(entry))
	}
	return cloned
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if condition() {
			return
		}
		select {
		case <-deadline:
			t.Fatal(message)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
