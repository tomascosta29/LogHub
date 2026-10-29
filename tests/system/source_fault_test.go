package system_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFileSourceReadsRecordsWrittenAfterTruncation truncates a file and
// immediately writes more data than its old length before the next poll.
// The source must notice that the bytes at the old offset belong to a new
// file generation and read them, not resume at a stale offset.
func TestFileSourceReadsRecordsWrittenAfterTruncation(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 40)

	c := newTestConfig(t)
	p := oracleFilePipeline("truncate-regrow", input, output, 20)
	p["runtime"].(map[string]any)["poll_interval"] = "100ms"
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "truncate-regrow")
	waitForJSONLCount(t, output, 40, 20*time.Second)

	// Truncate to zero and write a new generation large enough that the
	// file is longer than the old offset before the next poll.
	if err := os.Truncate(input, 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	g.appendFile(t, input, 80)

	waitFor(t, 20*time.Second, "new generation delivered", func() bool {
		have := make(map[string]bool)
		for _, e := range readLogEntriesTolerant(t, output) {
			have[e.Fields["id"]] = true
		}
		for _, rec := range g.all() {
			if !have[rec.ID] {
				return false
			}
		}
		return true
	})
	proc.stop()

	observed := observedFromLogEntries("local_jsonl", readLogEntriesTolerant(t, output))
	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"local_jsonl"},
		Observed:     observed,
	}, "truncate and regrow")
}

// TestFileSourceDeliversBinaryLinesUnchanged checks that a line containing
// bytes that are not valid UTF-8 arrives byte-for-byte. JSON encodes
// invalid UTF-8 as U+FFFD, so the destination must not silently rewrite
// input it was given.
func TestFileSourceDeliversBinaryLinesUnchanged(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "binary.log")
	output := filepath.Join(dir, "out.jsonl")
	raw := string([]byte{'[', 'I', 'N', 'F', 'O', ']', ' ', 'a', 0xff, 0xfe, 'b', '\n'})
	if err := os.WriteFile(input, []byte(raw), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	c := newTestConfig(t)
	p := newPipeline("binary-lines", sourceFile(input, "binary-input", 10), destLocalJSONL(output, ""))
	p["runtime"].(map[string]any)["poll_interval"] = "10ms"
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "binary-lines")
	waitForJSONLCount(t, output, 1, 15*time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	want := "a\xff\xfeb"
	if entries[0].Message != want {
		t.Errorf("message = %q, want %q: invalid UTF-8 was rewritten", entries[0].Message, want)
	}
	if entries[0].Fields["raw"] != want {
		t.Errorf("raw = %q, want %q: invalid UTF-8 was rewritten", entries[0].Fields["raw"], want)
	}
}

// TestFileSourceHandlesCRLFAndEmptyLines covers the line formatting cases
// that a log shipper meets in practice: CRLF endings, an empty line, a
// one-megabyte line and a final line without a newline.
func TestFileSourceHandlesCRLFAndEmptyLines(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "mixed.log")
	output := filepath.Join(dir, "out.jsonl")
	big := strings.Repeat("b", 1<<20)
	content := "[INFO] first\r\n\r\n[INFO] " + big + "\r\n[INFO] last-no-newline"
	if err := os.WriteFile(input, []byte(content), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	c := newTestConfig(t)
	p := newPipeline("line-shapes", sourceFile(input, "line-input", 10), destLocalJSONL(output, ""))
	p["runtime"].(map[string]any)["poll_interval"] = "10ms"
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "line-shapes")
	// The unterminated final line is held until the writer finishes it, so
	// only the three terminated lines are delivered while the file can
	// still grow.
	waitForJSONLCount(t, output, 3, 15*time.Second)
	proc.stop()

	// Finishing the last line must deliver it on the next start, without
	// re-reading the earlier ones.
	f, err := os.OpenFile(input, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		t.Fatalf("finish last line: %v", err)
	}
	f.Close()

	proc = startProc(t, cfg)
	waitForPipelineUp(t, proc, "line-shapes")
	waitForJSONLCount(t, output, 4, 15*time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	if len(entries) != 4 {
		t.Fatalf("entries = %d, want 4", len(entries))
	}
	if entries[0].Message != "first" {
		t.Errorf("CRLF line message = %q, want %q", entries[0].Message, "first")
	}
	if entries[1].Message != "" {
		t.Errorf("empty line message = %q, want empty", entries[1].Message)
	}
	if entries[2].Message != big {
		t.Errorf("large line length = %d, want %d", len(entries[2].Message), len(big))
	}
	if entries[3].Message != "last-no-newline" {
		t.Errorf("unterminated line message = %q", entries[3].Message)
	}
}

// TestFileSourceResumesFromLegacyPlainOffset checks the documented legacy
// cursor format: a bare offset still applies to the file at the path.
func TestFileSourceResumesFromLegacyPlainOffset(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	first := "[INFO] already-read\n"
	if err := os.WriteFile(input, []byte(first+"[INFO] new-line\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	c := newTestConfig(t)
	p := newPipeline("legacy-offset", sourceFile(input, "legacy-input", 10), destLocalJSONL(output, ""))
	p["runtime"].(map[string]any)["poll_interval"] = "10ms"
	c.add(p)
	cfg := c.write(t)

	// Seed the checkpoint with the legacy bare offset.
	cpDir := filepath.Join(c.dir, "checkpoints")
	if err := os.MkdirAll(cpDir, 0o755); err != nil {
		t.Fatalf("mkdir checkpoints: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cpDir, "legacy-offset.cursor"), []byte(fmt.Sprintf("%d", len(first))), 0o600); err != nil {
		t.Fatalf("write legacy cursor: %v", err)
	}

	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "legacy-offset")
	waitForJSONLCount(t, output, 1, 15*time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	if len(entries) != 1 || entries[0].Message != "new-line" {
		t.Fatalf("entries = %+v, want the one line after the legacy offset", entries)
	}
	if _, err := os.Stat(filepath.Join(cpDir, "legacy-offset.cursor.migrated")); err != nil {
		t.Errorf("legacy cursor was not migrated: %v", err)
	}
}

// TestFileSourceFollowsRenameRotation covers the ordinary logrotate path:
// the writer renames the current file and starts a new one, and the source
// must finish the old file before reading the new one.
func TestFileSourceFollowsRenameRotation(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 10)

	c := newTestConfig(t)
	p := oracleFilePipeline("rename-rotation", input, output, 50)
	p["runtime"].(map[string]any)["poll_interval"] = "20ms"
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "rename-rotation")
	waitForJSONLCount(t, output, 10, 15*time.Second)

	if err := os.Rename(input, input+".1"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	g.appendFile(t, input, 10)
	waitForJSONLCount(t, output, 20, 20*time.Second)
	proc.stop()

	verifyOracle(t, g, oracleCheck{
		Destinations: []string{"local_jsonl"},
		Observed:     observedFromLogEntries("local_jsonl", readLogEntriesTolerant(t, output)),
	}, "rename rotation")
}

// TestFileSourceDeletionWhileStoppedReadsTheReplacement checks the inode
// identity in the file cursor: after the file is deleted and recreated
// while LogHub is stopped, the source must start at the new file's
// beginning instead of resuming at a byte offset that now belongs to
// different data.
func TestFileSourceDeletionWhileStoppedReadsTheReplacement(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	g := newOracleGenerator()
	g.appendFile(t, input, 20)

	c := newTestConfig(t)
	p := oracleFilePipeline("deleted-while-stopped", input, output, 10)
	p["runtime"].(map[string]any)["poll_interval"] = "10ms"
	c.add(p)
	cfg := c.write(t)
	proc := startProc(t, cfg)
	waitForPipelineUp(t, proc, "deleted-while-stopped")
	waitForJSONLCount(t, output, 20, 15*time.Second)
	proc.stop()

	// Replace the file with a shorter one that reuses the same path.
	if err := os.Remove(input); err != nil {
		t.Fatalf("remove: %v", err)
	}
	second := newOracleGenerator()
	second.appendFile(t, input, 10)

	proc = startProc(t, cfg)
	waitForPipelineUp(t, proc, "deleted-while-stopped")
	waitForJSONLCount(t, output, 30, 20*time.Second)
	proc.stop()

	verifyOracle(t, second, oracleCheck{
		Destinations: []string{"local_jsonl"},
		Observed:     observedFromLogEntries("local_jsonl", readLogEntriesTolerant(t, output)[20:]),
	}, "replacement file after deletion")
}
