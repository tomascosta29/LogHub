package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"loghub/internal/core"
	"loghub/internal/dlq"
	_ "loghub/internal/schemas"
)

func writeDLQConfig(t *testing.T, dataDir string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "loghub.yaml")
	cfg := fmt.Sprintf(`
global:
  checkpoints_dir: %q
  data_dir: %q
pipelines:
  - id: "p1"
    durability:
      dlq:
        enabled: true
    source:
      type: "file"
      file:
        path: "/dev/null"
    destinations:
      - type: "dummy"
  - id: "p2"
    source:
      type: "file"
      file:
        path: "/dev/null"
    destinations:
      - type: "dummy"
`, filepath.Join(dataDir, "checkpoints"), dataDir)
	if err := os.WriteFile(path, []byte(cfg), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func TestDLQCommand(t *testing.T) {
	dataDir := t.TempDir()
	cfgPath := writeDLQConfig(t, dataDir)

	q, err := dlq.New(filepath.Join(dataDir, "dlq", "p1"), "p1", 0)
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}
	q.Write([]*core.LogEntry{{Message: "archived"}}, "sentinel#0", "400")
	if err := q.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	q.Write([]*core.LogEntry{{Message: "a"}, {Message: "b"}}, "sentinel#0", "400")
	q.Write([]*core.LogEntry{{Message: "c"}}, "processor", "bad json")
	q.Close()

	var out, errOut bytes.Buffer
	if err := runDLQ([]string{"export", "-config", cfgPath}, &out, &errOut); err != nil {
		t.Fatalf("export: %v (%s)", err, errOut.String())
	}
	var msgs []string
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var fb dlq.FailedBatch
		if err := json.Unmarshal([]byte(line), &fb); err != nil {
			t.Fatalf("export line %q is not a FailedBatch: %v", line, err)
		}
		msgs = append(msgs, fb.Entries[0].Message)
	}
	if strings.Join(msgs, ",") != "a,c" {
		t.Errorf("export = %v, want [a c] (archives only with -archives)", msgs)
	}

	out.Reset()
	if err := runDLQ([]string{"export", "-config", cfgPath, "-pipeline", "p1", "-archives"}, &out, &errOut); err != nil {
		t.Fatalf("export -archives: %v", err)
	}
	if n := strings.Count(out.String(), "\n"); n != 3 {
		t.Errorf("export -archives wrote %d records, want 3", n)
	}

	out.Reset()
	if err := runDLQ([]string{"list", "-config", cfgPath}, &out, &errOut); err != nil {
		t.Fatalf("list: %v", err)
	}
	list := out.String()
	var rows []string
	for _, line := range strings.Split(strings.TrimSpace(list), "\n")[1:] {
		f := strings.Fields(line)
		rows = append(rows, strings.Join(f[:4], " "))
	}
	if strings.Join(rows, "|") != "p1 processor 1 1|p1 sentinel#0 1 2" {
		t.Errorf("list rows = %q, want p1/processor 1 record 1 entry and p1/sentinel#0 1 record 2 entries:\n%s", rows, list)
	}
	if strings.Contains(list, "p2") {
		t.Errorf("list includes p2, which has no DLQ:\n%s", list)
	}

	if err := runDLQ([]string{"list", "-config", cfgPath, "-pipeline", "p2"}, &out, &errOut); err == nil {
		t.Error("list -pipeline p2: want an error (no DLQ enabled)")
	}
	if err := runDLQ([]string{"replay"}, &out, &errOut); err == nil {
		t.Error("unknown subcommand: want an error")
	}
}
