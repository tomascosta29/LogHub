package destination

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHTTPPoolReuseAcrossDestinationsAndClose(t *testing.T) {
	for _, status := range []int{200, 400, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.WriteHeader(status)
				io.WriteString(w, strings.Repeat("x", 8192))
			}))
			defer server.Close()
			first, err := NewHTTP(server.URL, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			second, err := NewHTTP(server.URL, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			if first.client.Transport != second.client.Transport {
				t.Fatal("transports are not shared")
			}
			first.SendRaw(context.Background(), []byte(`[]`), 1, nil)
			first.Close()
			first.Close() // Must not release the second destination's ownership.
			reused := false
			ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
				GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
			})
			second.SendRaw(ctx, []byte(`[]`), 1, nil)
			if !reused {
				t.Fatal("connection was not reused after response drain and peer close")
			}
		})
	}
}

func TestCreateWithHTTPPool(t *testing.T) {
	for _, name := range []string{"http", "sentinel", "azure_monitor"} {
		t.Run(name, func(t *testing.T) {
			props := map[string]interface{}{"url": "http://localhost"}
			if name != "http" {
				props = map[string]interface{}{"dce": "https://example.com", "dcr": "dcr", "stream": "stream"}
			}
			d, err := CreateWithHTTPPool(name, props, 17)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			var h *HTTP
			if s, ok := d.(*sentinelDestination); ok {
				h = s.httpDest
			} else {
				h = d.(*HTTP)
			}
			tr := h.client.Transport.(*http.Transport)
			if tr.MaxIdleConnsPerHost != 17 || tr.IdleConnTimeout != 90*time.Second || tr.DisableKeepAlives {
				t.Fatalf("unexpected transport: %+v", tr)
			}
		})
	}
	if _, err := CreateWithHTTPPool("dummy", nil, -1); err == nil {
		t.Fatal("negative pool limit accepted")
	}
}

func TestHTTPPoolConcurrentLifecycle(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := NewHTTP("http://localhost", time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			d.Close()
			d.Close()
		}()
	}
	wg.Wait()
}
