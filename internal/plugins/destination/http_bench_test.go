package destination

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkHTTPSendRaw measures connection reuse, including responses larger
// than the diagnostic body prefix retained by the destination.
func BenchmarkHTTPSendRaw(b *testing.B) {
	for _, size := range []int{0, 8192} {
		b.Run(fmt.Sprintf("response_%d", size), func(b *testing.B) {
			body := strings.Repeat("x", size)
			var connections atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				io.WriteString(w, body)
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			server.Start()
			defer server.Close()
			dest, err := NewHTTP(server.URL, 5*time.Second)
			if err != nil {
				b.Fatal(err)
			}
			defer dest.Close()
			payload := []byte(`[{"message":"benchmark"}]`)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := dest.SendRaw(context.Background(), payload, 1, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(connections.Load())/float64(b.N), "conns/op")
		})
	}
}
