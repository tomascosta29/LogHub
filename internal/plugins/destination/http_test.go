package destination

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"loghub/internal/core"
)

func TestHTTPSendSuccess(t *testing.T) {
	var received []*core.LogEntry
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dest, err := NewHTTP(server.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("NewHTTP failed: %v", err)
	}

	logs := []*core.LogEntry{
		{Message: "test1"},
		{Message: "test2"},
	}

	_, err = dest.Send(context.Background(), logs)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 {
		t.Errorf("Expected 2 logs received, got %d", len(received))
	}
}

func TestHTTPSendEmptyBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Should not receive request for empty batch")
	}))
	defer server.Close()

	dest, err := NewHTTP(server.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("NewHTTP failed: %v", err)
	}

	_, err = dest.Send(context.Background(), []*core.LogEntry{})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
}

func TestHTTPSendBadRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid format"))
	}))
	defer server.Close()

	dest, err := NewHTTP(server.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("NewHTTP failed: %v", err)
	}

	logs := []*core.LogEntry{{Message: "test"}}

	statuses, err := dest.Send(context.Background(), logs)
	if err != nil {
		t.Errorf("400 should be per-entry rejection, not batch error; got batch err %v", err)
	}
	if len(statuses) != 1 || statuses[0].Error == nil {
		t.Errorf("expected per-entry error status, got %+v", statuses)
	}
}

func TestHTTPSendServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dest, err := NewHTTP(server.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("NewHTTP failed: %v", err)
	}

	logs := []*core.LogEntry{{Message: "test"}}

	_, err = dest.Send(context.Background(), logs)
	if err == nil {
		t.Error("Expected error for server error")
	}
}

func TestHTTPSendRateLimited(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	dest, err := NewHTTP(server.URL, 5*time.Second)
	if err != nil {
		t.Fatalf("NewHTTP failed: %v", err)
	}

	logs := []*core.LogEntry{{Message: "test"}}

	_, err = dest.Send(context.Background(), logs)
	if err == nil {
		t.Error("Expected error for rate limited")
	}
}

func TestHTTPSendTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dest, err := NewHTTP(server.URL, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("NewHTTP failed: %v", err)
	}

	logs := []*core.LogEntry{{Message: "test"}}

	_, err = dest.Send(context.Background(), logs)
	if err == nil {
		t.Error("Expected error for timeout")
	}
}

func TestHTTPConstructorValidation(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		timeout time.Duration
		headers map[string]string
		wantErr string
	}{
		{
			name:    "empty url",
			url:     "",
			timeout: 30 * time.Second,
			wantErr: "http destination: url is required",
		},
		{
			name:    "zero timeout",
			url:     "http://example.com",
			timeout: 0,
			wantErr: "http destination: timeout must be positive, got 0s",
		},
		{
			name:    "negative timeout",
			url:     "http://example.com",
			timeout: -1 * time.Second,
			wantErr: "http destination: timeout must be positive, got -1s",
		},
		{
			name:    "empty header name",
			url:     "http://example.com",
			timeout: 30 * time.Second,
			headers: map[string]string{"": "value"},
			wantErr: "http destination: header name cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewHTTPWithHeaders(tt.url, tt.timeout, tt.headers)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestHTTPSendWithHeaders(t *testing.T) {
	var receivedHeaders http.Header
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		receivedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	headers := map[string]string{
		"Authorization":   "Bearer secret-token",
		"X-Custom-Header": "custom-value",
		"X-Request-ID":    "12345",
	}
	dest, err := NewHTTPWithHeaders(server.URL, 5*time.Second, headers)
	if err != nil {
		t.Fatalf("NewHTTPWithHeaders failed: %v", err)
	}

	logs := []*core.LogEntry{{Message: "test"}}
	_, err = dest.Send(context.Background(), logs)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	expectedHeaders := map[string]string{
		"Authorization":   "Bearer secret-token",
		"X-Custom-Header": "custom-value",
		"X-Request-ID":    "12345",
	}
	for name, expectedValue := range expectedHeaders {
		if got := receivedHeaders.Get(name); got != expectedValue {
			t.Errorf("Header %s = %q, want %q", name, got, expectedValue)
		}
	}
}

func TestHTTPEmptyHeaderName(t *testing.T) {
	headers := map[string]string{
		"Authorization": "Bearer token",
		"":              "empty-name-value",
	}
	_, err := NewHTTPWithHeaders("http://localhost", 5*time.Second, headers)
	if err == nil {
		t.Error("Expected error for empty header name")
	}
}

func TestHTTPDoesNotFollowRedirects(t *testing.T) {
	var gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/moved" {
			http.Redirect(w, r, "/ok", http.StatusFound)
			return
		}
		gets.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h, err := NewHTTP(srv.URL+"/moved", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := h.Send(context.Background(), []*core.LogEntry{{Message: "m"}}); err == nil {
		t.Fatal("Send through a redirect succeeded; the POST body would have been dropped")
	}
	if gets.Load() != 0 {
		t.Fatalf("redirect target was requested %d times, want 0", gets.Load())
	}
}

func TestHTTPErrorsOmitResponseBody(t *testing.T) {
	const secret = "SECRET-TOKEN-123"
	for _, status := range []int{400, 401, 403, 404, 413, 500, 418} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Ms-Request-Id", "req-42")
			w.WriteHeader(status)
			w.Write([]byte(`{"error":{"code":"InvalidStream","message":"echo ` + secret + `"}}`))
		}))
		h, err := NewHTTP(srv.URL, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		statuses, err := h.Send(context.Background(), []*core.LogEntry{{Message: "m"}})
		if err == nil && len(statuses) == 1 {
			err = statuses[0].Error
		}
		h.Close()
		srv.Close()
		if err == nil {
			t.Fatalf("status %d: no error", status)
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("status %d: error contains the response body: %v", status, err)
		}
		if !strings.Contains(err.Error(), "code InvalidStream") || !strings.Contains(err.Error(), "request id req-42") {
			t.Errorf("status %d: error %q lacks the error code and request id", status, err)
		}
	}
}
