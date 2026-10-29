package config

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzLoad checks that arbitrary YAML never panics the config loader.
// Run with: go test ./internal/config -fuzz=FuzzLoad -fuzztime=30s
func FuzzLoad(f *testing.F) {
	f.Add([]byte("global:\n  checkpoints_dir: /tmp/cp\npipelines: []\n"))
	f.Add([]byte("pipelines:\n  - id: p\n    source:\n      type: file\n      file:\n        path: /tmp/x\n    destinations:\n      - type: dummy\n"))
	f.Add([]byte("global: [\n"))
	f.Add([]byte{0x00, 0xff, 0xfe})
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "loghub.yaml")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}
		_, _ = Load(path)
	})
}
