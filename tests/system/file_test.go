//go:build !windows

package system_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeLogFile(t *testing.T, path string, lines ...string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestFileSourceBaseline(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	lines := make([]string, 50)
	for i := range lines {
		lines[i] = fmt.Sprintf("[INFO] file-line-%02d", i)
	}
	writeLogFile(t, input, lines...)

	c := newTestConfig(t)
	p := newPipeline("file-baseline", sourceFile(input, "file-input", 10), destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "10ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "file-baseline")
	waitForJSONLCount(t, output, 50, 10*time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	if len(entries) != 50 {
		t.Fatalf("entries = %d, want 50", len(entries))
	}
	seen := make(map[string]bool, 50)
	for i, e := range entries {
		if e.Message != fmt.Sprintf("file-line-%02d", i) {
			t.Fatalf("entry %d message = %q", i, e.Message)
		}
		if seen[e.Message] {
			t.Fatalf("duplicate message %q", e.Message)
		}
		seen[e.Message] = true
	}
}

func TestFilePartialLineContinuation(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	if err := os.WriteFile(input, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newTestConfig(t)
	p := newPipeline("file-partial", sourceFile(input, "file-input", 10), destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "file-partial")
	// Give the source several polls: the unfinished line must not be
	// emitted on its own.
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(output); err == nil {
		if n := len(readLogEntries(t, output)); n != 0 {
			t.Fatalf("%d entries emitted before the line was finished", n)
		}
	}
	f, err := os.OpenFile(input, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(" rest\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	time.Sleep(300 * time.Millisecond)
	proc.stop()

	entries := readLogEntries(t, output)
	var messages []string
	for _, e := range entries {
		messages = append(messages, e.Message)
	}
	if len(entries) != 1 || entries[0].Message != "partial rest" {
		t.Fatalf("partial line handling = %d entries %v, want one entry %q", len(entries), messages, "partial rest")
	}
}

func TestFileTruncation(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	writeLogFile(t, input, "[INFO] before-1", "[INFO] before-2")

	c := newTestConfig(t)
	p := newPipeline("file-truncate", sourceFile(input, "file-input", 10), destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "file-truncate")
	waitForJSONLCount(t, output, 2, 5*time.Second)

	if err := os.Truncate(input, 0); err != nil {
		t.Fatal(err)
	}
	writeLogFile(t, input, "[INFO] after-1", "[INFO] after-2")
	waitForJSONLCount(t, output, 4, 10*time.Second)
	proc.stop()

	entries := readLogEntries(t, output)
	got := make(map[string]bool)
	for _, e := range entries {
		got[e.Message] = true
	}
	for _, want := range []string{"before-1", "before-2", "after-1", "after-2"} {
		if !got[want] {
			t.Fatalf("missing %q after truncation; got %v", want, got)
		}
	}
}

func TestFileRotation(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "app.log")
	output := filepath.Join(dir, "out.jsonl")
	writeLogFile(t, input, "[INFO] old-1", "[INFO] old-2")

	c := newTestConfig(t)
	p := newPipeline("file-rotate", sourceFile(input, "file-input", 10), destLocalJSONL(output, ""))
	p["runtime"] = map[string]any{
		"poll_interval":       "20ms",
		"send_batch_size":     10,
		"buffer_size":         4,
		"checkpoint_interval": "0",
		"shutdown_timeout":    "3s",
	}
	c.add(p)
	cfg := c.write(t)

	proc := startProc(t, cfg)
	waitForRunning(t, proc, "file-rotate")
	waitForJSONLCount(t, output, 2, 5*time.Second)

	if err := os.Rename(input, input+".1"); err != nil {
		t.Fatal(err)
	}
	writeLogFile(t, input,
		"[INFO] new-1-with-a-longer-line",
		"[INFO] new-2-with-a-longer-line",
		"[INFO] new-3-with-a-longer-line")
	waitForJSONLCount(t, output, 5, 10*time.Second)
	proc.stop()
}
