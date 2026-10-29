package locale2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestDesktopBinaryE2E runs the actual CLI using local resources only. It
// exercises bootstrap wiring, HTTP retry, endpoints, SIGTERM and durable resume.
func TestDesktopBinaryE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the CLI")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "loghub")
	build := exec.Command("go", "build", "-o", binary, "./cmd/loghub")
	build.Dir = "../.."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}

	var mu sync.Mutex
	seen := make(map[string]bool)
	duplicates, attempts := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var entries []struct {
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&entries); err != nil {
			t.Errorf("decode: %v", err)
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts == 1 {
			w.WriteHeader(429)
			return
		}
		for _, entry := range entries {
			if seen[entry.Message] {
				duplicates++
			}
			seen[entry.Message] = true
		}
		io.WriteString(w, strings.Repeat("x", 8192))
	}))
	defer server.Close()

	input := filepath.Join(dir, "input.log")
	var lines strings.Builder
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&lines, "[INFO] desktop-%04d\n", i)
	}
	writeFile(t, input, lines.String())
	cfg := filepath.Join(dir, "loghub.yaml")
	writeFile(t, cfg, fmt.Sprintf(`global:
  checkpoints_dir: %q
  log_level: warn
  metrics_enabled: true
  http_max_idle_conns_per_host: 17
pipelines:
  - id: desktop-e2e
    runtime:
      poll_interval: 1ms
      send_batch_size: 50
      checkpoint_interval: "0"
      shutdown_timeout: 3s
    retry:
      max_attempts: 3
      base_backoff: 1ms
    source:
      type: file
      file:
        path: %q
        fetch_size: 100
    destinations:
      - type: http
        http:
          url: %q
`, filepath.Join(dir, "checkpoints"), input, server.URL))

	for run := 0; run < 2; run++ {
		// Reserve an ephemeral address for the CLI's own endpoint server.
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := listener.Addr().String()
		listener.Close()
		logFile, err := os.Create(filepath.Join(dir, fmt.Sprintf("run-%d.log", run)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { logFile.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "--config", cfg, "--metrics", addr)
		cmd.Stdout, cmd.Stderr = logFile, logFile
		started := time.Now()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				cmd.Process.Kill()
				cmd.Wait()
			}
		})
		client := &http.Client{Timeout: time.Second}
		defer client.CloseIdleConnections()
		waitFor(t, 5*time.Second, func() bool {
			resp, err := client.Get("http://" + addr + "/health")
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			var health struct {
				Running bool `json:"running"`
			}
			return json.NewDecoder(resp.Body).Decode(&health) == nil && health.Running && resp.StatusCode == 200
		}, "CLI health did not become ready")
		if run == 0 {
			waitFor(t, 5*time.Second, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(seen) == 1000
			}, "CLI did not deliver 1000 records")
			t.Logf("1000 records delivered including startup and one 429 retry in %s", time.Since(started))
		} else {
			// Give the restarted source several polls to expose a lost cursor.
			time.Sleep(50 * time.Millisecond)
			f, err := os.OpenFile(input, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = fmt.Fprintln(f, "[INFO] desktop-after-restart")
			closeErr := f.Close()
			if err != nil {
				t.Fatal(err)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			waitFor(t, 5*time.Second, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return seen["desktop-after-restart"]
			}, "restarted CLI did not deliver appended record")
		}
		resp, err := client.Get("http://" + addr + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || !strings.Contains(string(data), "loghub_entries_sent_total") {
			t.Fatalf("metrics unavailable: status=%d err=%v", resp.StatusCode, err)
		}
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Wait(); err != nil {
			logs, _ := os.ReadFile(logFile.Name())
			t.Fatalf("CLI stop: %v\n%s", err, logs)
		}
		stopped = true
		cancel()
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1001 || duplicates != 0 {
		t.Fatalf("got %d unique records, %d duplicates", len(seen), duplicates)
	}
}
