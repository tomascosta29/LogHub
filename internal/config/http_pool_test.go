package config

import (
	"fmt"
	"testing"
)

func TestHTTPPoolConfig(t *testing.T) {
	for _, value := range []int{0, 17, -1} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			cfg, err := loadConfigFromStringErr(t, fmt.Sprintf(`
global:
  http_max_idle_conns_per_host: %d
pipelines:
  - id: test
    source:
      type: file
      file:
        path: test.log
    destinations:
      - type: dummy
`, value))
			if value < 0 {
				if err == nil {
					t.Fatal("negative limit accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := value
			if want == 0 {
				want = 100
			}
			if cfg.Global.HTTPMaxIdleConnsPerHost != want {
				t.Fatalf("got %d, want %d", cfg.Global.HTTPMaxIdleConnsPerHost, want)
			}
		})
	}
}
