package system_test

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"loghub/internal/core"
)

// The oracle is the end-to-end accounting reference for the system tests.
// A generator writes records with a unique sequence number, a checksum of
// the payload and a generation timestamp. Every destination output, the
// DLQ and the drop counters are fed back through Verify, which reports
// loss, corruption, unauthorised duplication, reordering and metric drift.
//
// Tests must not treat delivery counts alone as proof: the checker compares
// field values and the per-record checksum, so a destination that receives
// the right number of wrong records fails.

// oracleRecord is one generated record.
type oracleRecord struct {
	Seq         int64
	ID          string
	Checksum    string
	Payload     string
	GeneratedAt time.Time
}

// observedRecord is one record as seen at a destination or in the DLQ.
type observedRecord struct {
	Dest        string
	ID          string
	Seq         int64
	Checksum    string
	Payload     string
	GeneratedAt string
	Fields      map[string]string
}

// oracleGenerator produces records and remembers what it produced.
type oracleGenerator struct {
	mu      sync.Mutex
	records []oracleRecord
	byID    map[string]oracleRecord
	seq     int64
	base    time.Time
	large   int
}

func newOracleGenerator() *oracleGenerator {
	return &oracleGenerator{
		byID: make(map[string]oracleRecord),
		base: time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC),
	}
}

// next builds the next record. Sizes deliberately vary so field handling,
// unicode and multi-kilobyte values are exercised by every campaign.
func (g *oracleGenerator) next() oracleRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seq++
	payload := fmt.Sprintf("payload-%06d", g.seq)
	switch {
	case g.seq%97 == 0:
		payload += "-héllo-世界-🚀"
	case g.seq%53 == 0:
		payload += "-" + strings.Repeat("L", 8192)
	}
	sum := sha256.Sum256([]byte(payload))
	rec := oracleRecord{
		Seq:         g.seq,
		ID:          fmt.Sprintf("%08d", g.seq),
		Checksum:    hex.EncodeToString(sum[:]),
		Payload:     payload,
		GeneratedAt: g.base.Add(time.Duration(g.seq) * time.Millisecond).UTC(),
	}
	g.records = append(g.records, rec)
	g.byID[rec.ID] = rec
	return rec
}

// generate appends n records and returns them.
func (g *oracleGenerator) generate(n int) []oracleRecord {
	out := make([]oracleRecord, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, g.next())
	}
	return out
}

func (g *oracleGenerator) all() []oracleRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]oracleRecord(nil), g.records...)
}

func (g *oracleGenerator) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.records)
}

func (g *oracleGenerator) lookup(id string) (oracleRecord, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r, ok := g.byID[id]
	return r, ok
}

// fields returns the record as a LogEntry field map, the shape both the
// SQLite source columns and the json_parse processor produce.
func (r oracleRecord) fields() map[string]string {
	return map[string]string{
		"id":           r.ID,
		"seq":          strconv.FormatInt(r.Seq, 10),
		"checksum":     r.Checksum,
		"payload":      r.Payload,
		"generated_at": r.GeneratedAt.Format(time.RFC3339Nano),
		"event_time":   r.GeneratedAt.Format(time.RFC3339),
	}
}

// createSQLiteTable creates the oracle table in an existing SQLite
// database. The column names match what the pipeline source configuration
// expects.
func (g *oracleGenerator) createSQLiteTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`
		CREATE TABLE oracle_events (
			id TEXT PRIMARY KEY,
			seq INTEGER NOT NULL,
			event_time TEXT NOT NULL,
			generated_at TEXT NOT NULL,
			checksum TEXT NOT NULL,
			payload TEXT NOT NULL
		)`); err != nil {
		t.Fatalf("create oracle_events: %v", err)
	}
}

// insertSQLite writes records to an oracle_events table in one
// transaction. The rows become visible atomically, like a committed audit
// batch.
func (g *oracleGenerator) insertSQLite(t *testing.T, db *sql.DB, n int) []oracleRecord {
	t.Helper()
	recs := g.generate(n)
	if len(recs) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin oracle insert: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO oracle_events (id, seq, event_time, generated_at, checksum, payload) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("prepare oracle insert: %v", err)
	}
	defer stmt.Close()
	for _, r := range recs {
		if _, err := stmt.Exec(r.ID, r.Seq, r.GeneratedAt.Format(time.RFC3339), r.GeneratedAt.Format(time.RFC3339Nano), r.Checksum, r.Payload); err != nil {
			tx.Rollback()
			t.Fatalf("insert oracle row %s: %v", r.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit oracle insert: %v", err)
	}
	return recs
}

// appendFile writes n records as JSON lines to a file source. The file
// source turns each line into an entry whose raw field is parsed by the
// json_parse processor in the pipeline under test.
func (g *oracleGenerator) appendFile(t *testing.T, path string, n int) []oracleRecord {
	t.Helper()
	recs := g.generate(n)
	if len(recs) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open oracle file: %v", err)
	}
	defer f.Close()
	for _, r := range recs {
		line, err := json.Marshal(r.fields())
		if err != nil {
			t.Fatalf("marshal oracle line: %v", err)
		}
		if _, err := fmt.Fprintf(f, "[INFO] %s\n", line); err != nil {
			t.Fatalf("append oracle line: %v", err)
		}
	}
	return recs
}

// path helpers for the oracle fixtures.
func oracleSQLiteSource(dbPath string, fetchSize int) map[string]any {
	return sourceSQLite(dbPath, "oracle_events", "event_time", "id", fetchSize, map[string]any{
		"name": "oracle-input",
	})
}

func oracleFileSource(path string, fetchSize int) map[string]any {
	return sourceFile(path, "oracle-input", fetchSize)
}

func oracleJSONParse() map[string]any {
	return map[string]any{"type": "json_parse", "json_parse": map[string]any{
		"target_field": "fields",
	}}
}

// observedFromLogEntries converts drained LogEntry values to observed
// records. Fields the pipeline did not carry through are left empty, and
// the checker reports them as corruption instead of skipping them.
func observedFromLogEntries(dest string, entries []core.LogEntry) []observedRecord {
	out := make([]observedRecord, 0, len(entries))
	for _, e := range entries {
		out = append(out, observedRecordFromFields(dest, e.Fields))
	}
	return out
}

func observedFromSentinel(dest string, records []map[string]string) []observedRecord {
	out := make([]observedRecord, 0, len(records))
	for _, r := range records {
		out = append(out, observedRecordFromFields(dest, r))
	}
	return out
}

func observedRecordFromFields(dest string, fields map[string]string) observedRecord {
	seq, _ := strconv.ParseInt(fields["seq"], 10, 64)
	return observedRecord{
		Dest:        dest,
		ID:          fields["id"],
		Seq:         seq,
		Checksum:    fields["checksum"],
		Payload:     fields["payload"],
		GeneratedAt: fields["generated_at"],
		Fields:      fields,
	}
}

// oracleCheck describes what a scenario permits.
type oracleCheck struct {
	// Destinations lists destinations every record must reach. Empty means
	// the union of Dest names in Observed is accepted.
	Destinations []string

	// Observed holds records read back from destination outputs, grouped
	// arbitrarily by Dest.
	Observed []observedRecord

	// DLQ holds records read from the dead-letter queue. A record found
	// here is preserved even if no destination delivered it.
	DLQ []observedRecord

	// Dropped is the number of records the pipeline is allowed to drop,
	// with a documented reason. The checker requires that exactly this
	// many generated records are neither delivered nor dead-lettered.
	Dropped int

	// AllowDuplicates permits replay, as after a crash or a failed
	// checkpoint. MaxDuplicates caps the number of extra copies in total;
	// zero means only a bounded-page allowance is applied by the caller
	// (the checker still reports every extra copy).
	AllowDuplicates bool
	MaxDuplicates   int

	// SkipOrdering disables the per-destination ordering check. Only use
	// this where the delivery contract explicitly has no ordering promise.
	SkipOrdering bool
}

// oracleResult is the accounting summary the checker computed.
type oracleResult struct {
	Generated   int
	Delivered   int
	Duplicates  int
	InDLQ       int
	Dropped     int
	Missing     []string
	Unknown     []string
	Corrupt     []string
	OrderErrors []string
	Errors      []string
}

func (r oracleResult) ok() bool { return len(r.Errors) == 0 }

// verify performs the accounting check. It returns every violation; callers
// fail the test when the result is not ok.
func (g *oracleGenerator) verify(check oracleCheck) oracleResult {
	res := oracleResult{Generated: g.count()}
	generated := g.all()
	genByID := make(map[string]oracleRecord, len(generated))
	for _, rec := range generated {
		genByID[rec.ID] = rec
	}

	destSet := make(map[string]bool, len(check.Destinations))
	for _, d := range check.Destinations {
		destSet[d] = true
	}
	if len(destSet) == 0 {
		for _, o := range check.Observed {
			destSet[o.Dest] = true
		}
	}

	// arrival records, in observation order per destination.
	arrivals := make(map[string][]observedRecord, len(destSet))
	for _, o := range check.Observed {
		if _, err := g.checkObserved(o); err != nil {
			res.Corrupt = append(res.Corrupt, err.Error())
			res.Errors = append(res.Errors, err.Error())
		}
		res.Delivered++
		arrivals[o.Dest] = append(arrivals[o.Dest], o)
	}
	for _, o := range check.DLQ {
		if _, err := g.checkObserved(o); err != nil {
			res.Corrupt = append(res.Corrupt, "DLQ: "+err.Error())
			res.Errors = append(res.Errors, "DLQ: "+err.Error())
		}
		res.InDLQ++
	}

	// count records per destination and in the DLQ.
	counts := make(map[string]int, len(genByID))
	perDest := make(map[string]map[string]int, len(destSet))
	for _, o := range check.Observed {
		counts[o.ID]++
		if perDest[o.Dest] == nil {
			perDest[o.Dest] = make(map[string]int)
		}
		perDest[o.Dest][o.ID]++
	}
	for _, o := range check.DLQ {
		counts[o.ID]++
	}
	for _, rec := range generated {
		if counts[rec.ID] == 0 {
			res.Missing = append(res.Missing, rec.ID)
		}
	}

	if len(res.Missing) != check.Dropped {
		if len(res.Missing) > check.Dropped {
			res.Errors = append(res.Errors, fmt.Sprintf("lost %d records (%s) but only %d drop(s) were expected",
				len(res.Missing)-check.Dropped, sampleIDs(res.Missing, 10), check.Dropped))
		} else {
			res.Errors = append(res.Errors, fmt.Sprintf("%d records were neither delivered, dead-lettered nor dropped as expected",
				check.Dropped-len(res.Missing)))
		}
	}
	for id := range counts {
		if _, ok := genByID[id]; !ok {
			res.Unknown = append(res.Unknown, id)
		}
	}
	// Duplicates are counted per destination: fan-out to several
	// destinations is one copy each, not a duplicate.
	extra := 0
	for dest, ids := range perDest {
		for id, n := range ids {
			if n <= 1 {
				continue
			}
			extra += n - 1
			if !check.AllowDuplicates {
				res.Duplicates += n - 1
				res.Errors = append(res.Errors, fmt.Sprintf("duplicate delivery of id %s to %s (%d copies) where duplicates are not allowed", id, dest, n))
			}
		}
	}
	if check.AllowDuplicates {
		res.Duplicates = extra
		if check.MaxDuplicates > 0 && extra > check.MaxDuplicates {
			res.Errors = append(res.Errors, fmt.Sprintf("%d duplicate copies, more than the allowed %d", extra, check.MaxDuplicates))
		}
	}
	if len(res.Unknown) > 0 {
		res.Errors = append(res.Errors, fmt.Sprintf("%d delivered records were never generated: %s", len(res.Unknown), sampleIDs(res.Unknown, 10)))
	}

	// Every configured destination must have seen every record that was
	// not dead-lettered for it or dropped. This catches a fan-out that
	// silently skips a destination.
	dlqIDs := make(map[string]bool, len(check.DLQ))
	for _, o := range check.DLQ {
		dlqIDs[o.ID] = true
	}
	for dest := range destSet {
		seen := make(map[string]bool, len(arrivals[dest]))
		for _, o := range arrivals[dest] {
			seen[o.ID] = true
		}
		var missing []string
		for _, rec := range generated {
			if !seen[rec.ID] && !dlqIDs[rec.ID] {
				missing = append(missing, rec.ID)
			}
		}
		if len(missing) > check.Dropped {
			res.Errors = append(res.Errors, fmt.Sprintf("destination %s is missing %d records (%s)", dest, len(missing), sampleIDs(missing, 10)))
		}
	}

	if !check.SkipOrdering {
		for dest, list := range arrivals {
			last := int64(-1)
			seen := make(map[string]bool, len(list))
			for _, o := range list {
				if seen[o.ID] {
					// A replay may repeat an already-seen record; ordering
					// of first appearances is what the contract promises.
					continue
				}
				seen[o.ID] = true
				if o.Seq <= last {
					res.OrderErrors = append(res.OrderErrors, fmt.Sprintf("destination %s delivered seq %d after %d", dest, o.Seq, last))
				}
				last = o.Seq
			}
			if len(res.OrderErrors) > 0 {
				res.Errors = append(res.Errors, res.OrderErrors...)
			}
		}
	}

	return res
}

// checkObserved validates one observed record against the generated set.
func (g *oracleGenerator) checkObserved(o observedRecord) (oracleRecord, error) {
	rec, ok := g.lookup(o.ID)
	if !ok {
		return oracleRecord{}, fmt.Errorf("delivered unknown id %q at %s", o.ID, o.Dest)
	}
	if o.Seq != rec.Seq {
		return rec, fmt.Errorf("id %s has seq %d, want %d", o.ID, o.Seq, rec.Seq)
	}
	sum := sha256.Sum256([]byte(o.Payload))
	if got := hex.EncodeToString(sum[:]); got != rec.Checksum {
		return rec, fmt.Errorf("id %s payload checksum %s does not match generated %s", o.ID, o.Checksum, rec.Checksum)
	}
	if o.Checksum != rec.Checksum {
		return rec, fmt.Errorf("id %s checksum field %q, want %q", o.ID, o.Checksum, rec.Checksum)
	}
	if o.Payload != rec.Payload {
		return rec, fmt.Errorf("id %s payload differs from generated payload", o.ID)
	}
	if o.GeneratedAt != rec.GeneratedAt.Format(time.RFC3339Nano) {
		return rec, fmt.Errorf("id %s generated_at %q, want %q", o.ID, o.GeneratedAt, rec.GeneratedAt.Format(time.RFC3339Nano))
	}
	return rec, nil
}

func inObserved(list []observedRecord, dest, id string) bool {
	for _, o := range list {
		if o.ID == id && (dest == "" || o.Dest == dest) {
			return true
		}
	}
	return false
}

func sampleIDs(ids []string, n int) string {
	sort.Strings(ids)
	if len(ids) > n {
		return strings.Join(ids[:n], ", ") + ", ..."
	}
	return strings.Join(ids, ", ")
}

// verifyOracle fails the test with the checker's violations.
func verifyOracle(t *testing.T, g *oracleGenerator, check oracleCheck, context string) oracleResult {
	t.Helper()
	res := g.verify(check)
	if !res.ok() {
		t.Fatalf("%s: oracle failed: generated=%d delivered=%d dlq=%d duplicates=%d\n%s",
			context, res.Generated, res.Delivered, res.InDLQ, res.Duplicates,
			strings.Join(res.Errors, "\n"))
	}
	t.Logf("%s: oracle ok: generated=%d delivered=%d dlq=%d duplicates=%d",
		context, res.Generated, res.Delivered, res.InDLQ, res.Duplicates)
	return res
}

// oracleMetrics is the counter snapshot the accounting check uses.
type oracleMetrics struct {
	Collected float64
	Sent      float64
	Failed    float64
	DLQ       float64
	Dropped   float64
	Buffered  float64
}

// reconcile checks collected = sent + DLQ + dropped + still buffered for
// one destination. The caller samples after quiescence, so buffered is
// normally zero. Failed is reported separately: it counts attempts that may
// have been retried successfully, so it is not a loss term.
func reconcile(m oracleMetrics) []string {
	var errs []string
	if m.Collected < 0 || m.Sent < 0 || m.DLQ < 0 || m.Dropped < 0 || m.Buffered < 0 {
		errs = append(errs, "negative metric value")
	}
	want := m.Sent + m.DLQ + m.Dropped + m.Buffered
	if m.Collected != want {
		errs = append(errs, fmt.Sprintf("metric reconciliation: collected %v != sent %v + DLQ %v + dropped %v + buffered %v = %v",
			m.Collected, m.Sent, m.DLQ, m.Dropped, m.Buffered, want))
	}
	if m.Sent > m.Collected+m.DLQ {
		errs = append(errs, fmt.Sprintf("sent %v exceeds collected %v + DLQ %v", m.Sent, m.Collected, m.DLQ))
	}
	return errs
}

// pipelineOracleMetrics reads the counters for a single-destination
// pipeline. buffered is the sum of the in-memory and disk backlog in
// entries, which callers pass explicitly because the queues expose batch
// counts, not entry counts.
func pipelineOracleMetrics(t *testing.T, fams metricFamilies, pipelineID, sourceName, destType string, bufferedEntries float64) oracleMetrics {
	t.Helper()
	return oracleMetrics{
		Collected: counterValue(fams, "loghub_entries_collected_total", map[string]string{"pipeline_id": pipelineID, "source_name": sourceName}),
		Sent:      counterValue(fams, "loghub_entries_sent_total", map[string]string{"pipeline_id": pipelineID, "destination_type": destType}),
		Failed:    counterValue(fams, "loghub_entries_failed_total", map[string]string{"pipeline_id": pipelineID, "destination_type": destType}),
		DLQ:       counterValue(fams, "loghub_dlq_entries_total", map[string]string{"pipeline_id": pipelineID}),
		Dropped:   sumDropped(fams, pipelineID),
		Buffered:  bufferedEntries,
	}
}

func sumDropped(fams metricFamilies, pipelineID string) float64 {
	fam, ok := fams["loghub_entries_dropped_total"]
	if !ok {
		return 0
	}
	var total float64
	for _, m := range fam.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "pipeline_id" && lp.GetValue() == pipelineID {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}

// TestOracleDetectsInjectedLossDuplicationCorruption self-tests the
// checker. A checker that only counts records cannot tell a correct run
// from one that lost a record and duplicated another, so all three faults
// are injected and each must be reported.
func TestOracleDetectsInjectedLossDuplicationCorruption(t *testing.T) {
	g := newOracleGenerator()
	recs := g.generate(10)
	var observed []observedRecord
	for _, r := range recs {
		observed = append(observed, observedRecordFromFields("dest", r.fields()))
	}

	clean := g.verify(oracleCheck{Destinations: []string{"dest"}, Observed: observed})
	if !clean.ok() {
		t.Fatalf("clean run rejected: %v", clean.Errors)
	}

	lossy := append([]observedRecord(nil), observed[1:]...)
	if res := g.verify(oracleCheck{Destinations: []string{"dest"}, Observed: lossy}); res.ok() || len(res.Missing) == 0 {
		t.Fatalf("checker did not detect loss: %+v", res)
	}

	duped := append([]observedRecord(nil), observed...)
	duped = append(duped, observed[3])
	if res := g.verify(oracleCheck{Destinations: []string{"dest"}, Observed: duped}); res.ok() {
		t.Fatalf("checker did not detect duplication: %+v", res)
	}

	corrupt := append([]observedRecord(nil), observed...)
	corrupt[2].Payload = corrupt[2].Payload + "x"
	if res := g.verify(oracleCheck{Destinations: []string{"dest"}, Observed: corrupt}); res.ok() {
		t.Fatalf("checker did not detect corruption: %+v", res)
	}

	reordered := append([]observedRecord(nil), observed...)
	reordered[1], reordered[2] = reordered[2], reordered[1]
	if res := g.verify(oracleCheck{Destinations: []string{"dest"}, Observed: reordered}); res.ok() {
		t.Fatalf("checker did not detect reordering: %+v", res)
	}

	dropped := append([]observedRecord(nil), observed[:8]...)
	if res := g.verify(oracleCheck{Destinations: []string{"dest"}, Observed: dropped, Dropped: 2}); !res.ok() {
		t.Fatalf("documented drop rejected: %v", res.Errors)
	}
	if res := g.verify(oracleCheck{Destinations: []string{"dest"}, Observed: dropped, Dropped: 1}); res.ok() {
		t.Fatalf("checker accepted an undocumented drop: %+v", res)
	}
}

// TestOracleReconcilesMetrics self-tests the metric accounting rule.
func TestOracleReconcilesMetrics(t *testing.T) {
	if errs := reconcile(oracleMetrics{Collected: 10, Sent: 8, DLQ: 1, Dropped: 1}); len(errs) != 0 {
		t.Fatalf("balanced metrics rejected: %v", errs)
	}
	if errs := reconcile(oracleMetrics{Collected: 10, Sent: 8, DLQ: 1}); len(errs) == 0 {
		t.Fatal("unbalanced metrics accepted")
	}
	if errs := reconcile(oracleMetrics{Collected: 10, Sent: 11}); len(errs) == 0 {
		t.Fatal("sent greater than collected accepted")
	}
}

// oracleFilePipeline builds a file-source pipeline that parses the
// oracle's JSON lines and delivers to a local_jsonl destination.
func oracleFilePipeline(id, path, output string, fetchSize int) map[string]any {
	p := newPipeline(id, oracleFileSource(path, fetchSize), destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     fetchSize,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "5s",
	}
	p["processors"] = []any{oracleJSONParse()}
	return p
}

// oracleSQLitePipeline builds a SQLite-source pipeline over oracle_events.
func oracleSQLitePipeline(id, dbPath, output string, fetchSize int) map[string]any {
	p := newPipeline(id, oracleSQLiteSource(dbPath, fetchSize), destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     fetchSize,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "5s",
	}
	return p
}

// oracleOutputDir is a small convenience for tests that keep every state
// directory under one temporary tree.
func oracleOutputDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "run")
}
