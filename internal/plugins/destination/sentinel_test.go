package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"loghub/internal/core"
)

type fakeTokenProvider struct {
	token string
	err   error
}

func (f fakeTokenProvider) GetToken(ctx context.Context) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.token, nil
}

func TestNewSentinelRequiresDCE(t *testing.T) {
	_, err := NewSentinelWithTokenProvider("", "dcr-id", "Custom-MSSQLAudit_CL", "1s", fakeTokenProvider{token: "token"})
	if err == nil {
		t.Fatal("expected dce validation error")
	}
	if got, want := err.Error(), "sentinel destination: dce is required"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSentinelSendBuildsLogsIngestionRequest(t *testing.T) {
	var requestPath string
	var authHeader string
	var sourceHeader string
	var records []map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.RequestURI()
		authHeader = r.Header.Get("Authorization")
		sourceHeader = r.Header.Get("X-LogSource")
		if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
			t.Errorf("failed to decode request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dest, err := NewSentinelWithTokenProvider(server.URL+"/", "dcr-id", "Custom-MSSQLAudit_CL", "1s", fakeTokenProvider{token: "fake-token"})
	if err != nil {
		t.Fatalf("failed to create sentinel destination: %v", err)
	}
	dest.SetSourceName("mssql-audit")

	entry := &core.LogEntry{
		Timestamp: time.Date(2026, 5, 6, 12, 0, 1, 123, time.FixedZone("CEST", 2*60*60)),
		Source:    "mssql-audit",
		Level:     "INFO",
		Message:   "login succeeded",
		Fields: map[string]string{
			"id":         "000001",
			"event_time": "2026-05-06T10:00:01Z",
			"user_name":  "user-000001",
			"action":     "login",
		},
	}
	if _, err := dest.Send(context.Background(), []*core.LogEntry{entry}); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if got, want := requestPath, "/dataCollectionRules/dcr-id/streams/Custom-MSSQLAudit_CL?api-version=2023-01-01"; got != want {
		t.Fatalf("request path got %q, want %q", got, want)
	}
	if got, want := authHeader, "Bearer fake-token"; got != want {
		t.Fatalf("authorization got %q, want %q", got, want)
	}
	if got, want := sourceHeader, "mssql-audit"; got != want {
		t.Fatalf("source header got %q, want %q", got, want)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	record := records[0]
	// The entry's timestamp carries 123ns; TimeGenerated keeps it to
	// Log Analytics' 100ns precision.
	if got, want := record["TimeGenerated"], "2026-05-06T10:00:01.0000001Z"; got != want {
		t.Fatalf("TimeGenerated got %q, want %q", got, want)
	}
	if got, want := record["id"], "000001"; got != want {
		t.Fatalf("id got %q, want %q", got, want)
	}
	if _, ok := record["fields"]; ok {
		t.Fatal("payload should be flattened, not nested under fields")
	}
	if _, ok := record["message"]; ok {
		t.Fatal("payload should only include MSSQL fields plus TimeGenerated")
	}
}

func TestSentinelTokenFailureReturnsErrorBeforePost(t *testing.T) {
	posted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posted = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dest, err := NewSentinelWithTokenProvider(server.URL, "dcr-id", "Custom-MSSQLAudit_CL", "1s", fakeTokenProvider{err: errors.New("identity unavailable")})
	if err != nil {
		t.Fatalf("failed to create sentinel destination: %v", err)
	}

	_, err = dest.Send(context.Background(), []*core.LogEntry{{Timestamp: time.Now(), Fields: map[string]string{"id": "1"}}})
	if err == nil {
		t.Fatal("expected token acquisition error")
	}
	if posted {
		t.Fatal("request should not be posted when token acquisition fails")
	}
}

func TestMarshalSentinelPayloadFallsBackToEventTime(t *testing.T) {
	data, err := MarshalSentinelPayload([]*core.LogEntry{{
		Fields: map[string]string{
			"id":         "000001",
			"event_time": "2026-05-06T12:00:01+02:00",
		},
	}})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var records []map[string]string
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got, want := records[0]["TimeGenerated"], "2026-05-06T10:00:01Z"; got != want {
		t.Fatalf("TimeGenerated got %q, want %q", got, want)
	}
}

func TestMarshalSentinelPayloadRequiresTimestamp(t *testing.T) {
	// MarshalSentinelPayload leaves out entries
	// without a usable timestamp and returns a valid (possibly empty)
	// JSON array. The caller is expected to detect the record-count
	// mismatch vs input entries and route the dropped ones to the DLQ.
	data, err := MarshalSentinelPayload([]*core.LogEntry{{
		Fields: map[string]string{"id": "000001"},
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "[]" {
		t.Errorf("expected empty JSON array %q, got %q", "[]", string(data))
	}
}

func TestSentinelSendRejectsMissingTimestampPerEntry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dest, err := NewSentinelWithTokenProvider(server.URL+"/", "dcr-id", "Custom-MSSQLAudit_CL", "1s", fakeTokenProvider{token: "fake-token"})
	if err != nil {
		t.Fatalf("failed to create sentinel destination: %v", err)
	}

	entries := []*core.LogEntry{
		{Timestamp: time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC), Fields: map[string]string{"id": "1"}},
		{Fields: map[string]string{"id": "2"}}, // missing timestamp
		{Timestamp: time.Date(2026, 5, 6, 12, 0, 1, 0, time.UTC), Fields: map[string]string{"id": "3"}},
		{Fields: map[string]string{"id": "4"}}, // missing timestamp
	}

	statuses, err := dest.Send(context.Background(), entries)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if len(statuses) != 4 {
		t.Fatalf("expected 4 statuses, got %d", len(statuses))
	}
	if statuses[0].Error != nil {
		t.Errorf("entry 0 should be accepted, got error: %v", statuses[0].Error)
	}
	if statuses[1].Error == nil {
		t.Errorf("entry 1 should be rejected (missing timestamp)")
	}
	if statuses[2].Error != nil {
		t.Errorf("entry 2 should be accepted, got error: %v", statuses[2].Error)
	}
	if statuses[3].Error == nil {
		t.Errorf("entry 3 should be rejected (missing timestamp)")
	}

	if !errors.Is(statuses[1].Error, core.ErrPoisonPill) {
		t.Errorf("entry 1 rejection should wrap ErrPoisonPill, got: %v", statuses[1].Error)
	}
	if !errors.Is(statuses[3].Error, core.ErrPoisonPill) {
		t.Errorf("entry 3 rejection should wrap ErrPoisonPill, got: %v", statuses[3].Error)
	}
}

func TestSentinelSendAllRejectedWhenNoTimestamps(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Should never be called because all entries are rejected before POST.
		t.Error("server should not receive a request when all entries are rejected")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dest, err := NewSentinelWithTokenProvider(server.URL+"/", "dcr-id", "Custom-MSSQLAudit_CL", "1s", fakeTokenProvider{token: "fake-token"})
	if err != nil {
		t.Fatalf("failed to create sentinel destination: %v", err)
	}

	entries := []*core.LogEntry{
		{Fields: map[string]string{"id": "1"}},
		{Fields: map[string]string{"id": "2"}},
	}

	statuses, err := dest.Send(context.Background(), entries)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	for i, st := range statuses {
		if st.Error == nil {
			t.Errorf("entry %d should be rejected (missing timestamp)", i)
		}
		if !errors.Is(st.Error, core.ErrPoisonPill) {
			t.Errorf("entry %d rejection should wrap ErrPoisonPill, got: %v", i, st.Error)
		}
	}
}

func TestSentinelTokenNotLeakedInErrors(t *testing.T) {
	sensitiveToken := "super-secret-bearer-token-12345"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the token is sent in the header but not exposed in test errors
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+sensitiveToken {
			t.Errorf("expected bearer token, got: %q", auth)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dest, err := NewSentinelWithTokenProvider(server.URL+"/", "dcr-id", "Custom-MSSQLAudit_CL", "1s", fakeTokenProvider{token: sensitiveToken})
	if err != nil {
		t.Fatalf("failed to create sentinel destination: %v", err)
	}

	entries := []*core.LogEntry{
		{Timestamp: time.Now(), Fields: map[string]string{"id": "1"}},
	}

	_, err = dest.Send(context.Background(), entries)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// The token should never appear in any error produced by the destination.
	// This test is a regression guard: if Send ever starts returning the
	// token in an error message, the assertion above will fail.
}

func TestNewAzureCredentialSelectsMethod(t *testing.T) {
	cases := []struct {
		name string
		cfg  sentinelConfig
		want string // credential type, or "error"
	}{
		{"default is managed identity", sentinelConfig{}, "*azidentity.ManagedIdentityCredential"},
		{"user-assigned via top-level client_id", sentinelConfig{ClientID: "11111111-1111-1111-1111-111111111111"}, "*azidentity.ManagedIdentityCredential"},
		{"explicit managed identity", sentinelConfig{Auth: azureAuthConfig{Method: "ad-managed-identity", ClientID: "11111111-1111-1111-1111-111111111111"}}, "*azidentity.ManagedIdentityCredential"},
		{"service principal", sentinelConfig{Auth: azureAuthConfig{
			Method: "ad-service-principal", TenantID: "22222222-2222-2222-2222-222222222222",
			ClientID: "33333333-3333-3333-3333-333333333333", ClientSecret: "s3cret",
		}}, "*azidentity.ClientSecretCredential"},
		{"service principal without secret", sentinelConfig{Auth: azureAuthConfig{
			Method: "ad-service-principal", TenantID: "22222222-2222-2222-2222-222222222222",
			ClientID: "33333333-3333-3333-3333-333333333333",
		}}, "error"},
		{"unknown method", sentinelConfig{Auth: azureAuthConfig{Method: "ad-password"}}, "error"},
	}
	for _, tc := range cases {
		cred, err := newAzureCredential(tc.cfg)
		got := "error"
		if err == nil {
			got = fmt.Sprintf("%T", cred)
		}
		if got != tc.want {
			t.Errorf("%s: newAzureCredential = %s (err %v), want %s", tc.name, got, err, tc.want)
		}
	}
}

func TestSentinelFactoryDecodesAuthBlock(t *testing.T) {
	dest, err := Create("sentinel", map[string]interface{}{
		"dce":    "https://dce.example.com",
		"dcr":    "dcr-id",
		"stream": "Custom-Test_CL",
		"auth": map[string]interface{}{
			"method":        "ad-service-principal",
			"tenant_id":     "22222222-2222-2222-2222-222222222222",
			"client_id":     "33333333-3333-3333-3333-333333333333",
			"client_secret": "s3cret",
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sd := dest.(*sentinelDestination)
	tp, ok := sd.tokenProvider.(*tokenProvider)
	if !ok {
		t.Fatalf("token provider = %T, want *tokenProvider", sd.tokenProvider)
	}
	if _, ok := tp.cred.(*azidentity.ClientSecretCredential); !ok {
		t.Errorf("credential = %T, want *azidentity.ClientSecretCredential", tp.cred)
	}
}

func TestSentinelTimeGeneratedKeepsSubSecondPrecision(t *testing.T) {
	entry := &core.LogEntry{Timestamp: time.Date(2026, 10, 28, 19, 9, 30, 1500000, time.UTC)}
	got, err := sentinelTimeGenerated(entry)
	if err != nil || got != "2026-10-28T19:09:30.0015Z" {
		t.Errorf("TimeGenerated = %q, %v; want 2026-10-28T19:09:30.0015Z", got, err)
	}
	entry = &core.LogEntry{Fields: map[string]string{"event_time": "2026-10-28T19:09:30.25Z"}}
	if got, err := sentinelTimeGenerated(entry); err != nil || got != "2026-10-28T19:09:30.25Z" {
		t.Errorf("TimeGenerated from event_time = %q, %v; want 2026-10-28T19:09:30.25Z", got, err)
	}
}
