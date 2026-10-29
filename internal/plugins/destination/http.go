package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"loghub/internal/core"
	"loghub/internal/util"
)

type httpConfig struct {
	URL     string            `mapstructure:"url"`
	Timeout string            `mapstructure:"timeout"`
	Headers map[string]string `mapstructure:"headers"`
}

func init() {
	Register("http", func(props map[string]interface{}) (core.Destination, error) {
		var cfg httpConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		timeout := 30 * time.Second
		if cfg.Timeout != "" {
			d, err := time.ParseDuration(cfg.Timeout)
			if err != nil {
				return nil, fmt.Errorf("http destination: invalid timeout: %w", err)
			}
			timeout = d
		}
		return NewHTTPWithHeaders(cfg.URL, timeout, cfg.Headers)
	})

}

var (
	ErrRateLimited = errors.New("rate limited")
	ErrBadRequest  = errors.New("bad request")
	ErrServerError = errors.New("server error")
	ErrTimeout     = errors.New("timeout")
)

type HTTP struct {
	url              string
	client           *http.Client
	sourceName       string
	headers          map[string]string
	releaseTransport func()
	closeOnce        sync.Once
}

func NewHTTP(url string, timeout time.Duration) (*HTTP, error) {
	return NewHTTPWithHeaders(url, timeout, nil)
}

func NewHTTPWithHeaders(url string, timeout time.Duration, headers map[string]string) (*HTTP, error) {
	if url == "" {
		return nil, fmt.Errorf("http destination: url is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("http destination: timeout must be positive, got %s", timeout)
	}
	for name := range headers {
		if name == "" {
			return nil, fmt.Errorf("http destination: header name cannot be empty")
		}
	}
	copiedHeaders := make(map[string]string, len(headers))
	for k, v := range headers {
		copiedHeaders[k] = v
	}
	transport, release := acquireTransport(100)
	return &HTTP{
		url:              url,
		client:           &http.Client{Timeout: timeout, Transport: transport},
		headers:          copiedHeaders,
		releaseTransport: release,
	}, nil
}

// tunedTransport retains the existing direct-connection policy while bounding
// connection setup and keeping a large idle pool for ingestion traffic.
func tunedTransport(maxIdle int) *http.Transport {
	return &http.Transport{
		MaxIdleConns:        maxIdle,
		MaxIdleConnsPerHost: maxIdle,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		TLSHandshakeTimeout: 10 * time.Second,
	}
}

type transportPool struct {
	transport *http.Transport
	users     int
}

var sharedTransports = struct {
	sync.Mutex
	pools map[int]*transportPool
}{pools: make(map[int]*transportPool)}

// acquireTransport shares connections between destinations with identical
// settings. Only the last owner closes the idle pool, including across reloads.
func acquireTransport(maxIdle int) (*http.Transport, func()) {
	sharedTransports.Lock()
	defer sharedTransports.Unlock()
	pool := sharedTransports.pools[maxIdle]
	if pool == nil {
		pool = &transportPool{transport: tunedTransport(maxIdle)}
		sharedTransports.pools[maxIdle] = pool
	}
	pool.users++
	return pool.transport, func() {
		sharedTransports.Lock()
		defer sharedTransports.Unlock()
		pool.users--
		if pool.users == 0 {
			delete(sharedTransports.pools, maxIdle)
			pool.transport.CloseIdleConnections()
		}
	}
}

// SendRaw POSTs a pre-marshaled body to the destination URL with the
// configured headers, returning the per-entry statuses (size n) and a
// batch-level error. The n parameter is the expected number of input
// entries; it is used to size the success/rejected result slices when
// the response is interpreted per-entry. Used by the Sentinel
// destination, which marshals its own payload via MarshalSentinelPayload.
func (h *HTTP) SendRaw(ctx context.Context, body []byte, n int, extraHeaders map[string]string) ([]core.EntryStatus, error) {
	if len(body) == 0 && n == 0 {
		return nil, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-LogSource", h.sourceName)
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return h.do(ctx, req, n)
}

func (h *HTTP) Send(ctx context.Context, logs []*core.LogEntry) ([]core.EntryStatus, error) {
	if len(logs) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(logs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal logs: %w", err)
	}
	return h.SendRaw(ctx, data, len(logs), nil)
}

func (h *HTTP) do(ctx context.Context, req *http.Request, n int) ([]core.EntryStatus, error) {
	resp, err := h.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, ErrTimeout
		}
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		slog.Warn("Failed to read response body", "error", err)
	} else if len(body) == 1024 {
		// Retain only the diagnostic prefix, but consume bounded response
		// bodies to EOF so HTTP/1 connections can return to the idle pool.
		// Huge or streaming responses are closed rather than drained forever.
		if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024)); err != nil {
			slog.Warn("Failed to drain response body", "error", err)
		}
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return allSuccess(n), nil
	case resp.StatusCode == http.StatusBadRequest:
		return allRejected(n, errors.Join(ErrBadRequest, fmt.Errorf("bad request: %s: %w", string(body), core.ErrPoisonPill))), nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, ErrRateLimited
	case resp.StatusCode >= 500:
		return nil, errors.Join(ErrServerError, fmt.Errorf("server error %d: %s", resp.StatusCode, string(body)))
	default:
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
}

func allSuccess(n int) []core.EntryStatus {
	out := make([]core.EntryStatus, n)
	for i := range out {
		out[i] = core.EntryStatus{Index: i}
	}
	return out
}

func allRejected(n int, err error) []core.EntryStatus {
	out := make([]core.EntryStatus, n)
	for i := range out {
		out[i] = core.EntryStatus{Index: i, Error: err}
	}
	return out
}

func (h *HTTP) Close() error {
	h.closeOnce.Do(h.releaseTransport)
	return nil
}

func (h *HTTP) Type() string {
	return "http"
}

func (h *HTTP) SetSourceName(name string) {
	h.sourceName = name
}
