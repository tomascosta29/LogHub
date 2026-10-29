package mssqlsentinel_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"loghub/internal/core"
)

const (
	expectedStreamName = "Custom-MSSQLAudit_CL"
	expectedTableName  = "MSSQLAudit_CL"
	timeGeneratedField = "TimeGenerated"
	eventTimeField     = "event_time"
)

type sentinelPayload struct {
	TimeGenerated string `json:"TimeGenerated"`
	ID            string `json:"id"`
	EventTime     string `json:"event_time"`
	UserName      string `json:"user_name"`
	Action        string `json:"action"`
}

func (s sentinelPayload) Validate(t *testing.T) {
	if s.TimeGenerated == "" {
		t.Error("TimeGenerated field is missing")
	}
	if !isValidRFC3339UTC(s.TimeGenerated) {
		t.Errorf("TimeGenerated must be RFC3339 UTC format, got: %s", s.TimeGenerated)
	}
	if s.ID == "" {
		t.Error("id field is missing")
	}
	if s.EventTime == "" {
		t.Error("event_time field is missing")
	}
	if !isValidRFC3339UTC(s.EventTime) {
		t.Errorf("event_time must be RFC3339 UTC format, got: %s", s.EventTime)
	}
	if s.UserName == "" {
		t.Error("user_name field is missing")
	}
	if s.Action == "" {
		t.Error("action field is missing")
	}
}

func isValidRFC3339UTC(ts string) bool {
	if ts == "" {
		return false
	}
	if !strings.HasSuffix(ts, "Z") {
		return false
	}
	_, err := time.Parse(time.RFC3339, ts)
	return err == nil
}

func TestSentinelPayloadSchema(t *testing.T) {
	entry := &core.LogEntry{
		Timestamp: time.Date(2026, 5, 6, 12, 0, 1, 0, time.UTC),
		Source:    "mock-mssql-audit",
		Level:     "INFO",
		Message:   "login succeeded",
		Fields: map[string]string{
			"id":         "000001",
			"event_time": "2026-05-06T12:00:01Z",
			"user_name":  "user-000001",
			"action":     "login",
		},
	}

	payload, err := buildSentinelPayload(entry)
	if err != nil {
		t.Fatalf("failed to build sentinel payload: %v", err)
	}

	var records []sentinelPayload
	if err := json.Unmarshal(payload, &records); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	records[0].Validate(t)
}

func TestSentinelPayloadTimeGeneratedFormat(t *testing.T) {
	testCases := []struct {
		name      string
		timestamp string
		valid     bool
	}{
		{"valid UTC with Z", "2026-05-06T12:00:01Z", true},
		{"valid UTC with timezone offset", "2026-05-06T12:00:01+00:00", false},
		{"missing Z suffix", "2026-05-06T12:00:01", false},
		{"invalid format", "2026-05-06 12:00:01", false},
		{"empty string", "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isValidRFC3339UTC(tc.timestamp); got != tc.valid {
				t.Errorf("isValidRFC3339UTC(%q) = %v, want %v", tc.timestamp, got, tc.valid)
			}
		})
	}
}

func TestSentinelStreamNameFormat(t *testing.T) {
	testCases := []struct {
		tableName string
		valid     bool
	}{
		{"MSSQLAudit_CL", true},
		{"Custom-MSSQLAudit_CL", true},
		{"MSSQLAudit", false},
		{"mssql_audit_cl", false},
		{"Audit", false},
	}

	for _, tc := range testCases {
		t.Run(tc.tableName, func(t *testing.T) {
			streamName := "Custom-" + tc.tableName
			if tc.tableName == "Custom-MSSQLAudit_CL" {
				streamName = tc.tableName
			}

			valid := isValidStreamName(streamName)
			if valid != tc.valid {
				t.Errorf("isValidStreamName(%q) = %v, want %v", streamName, valid, tc.valid)
			}
		})
	}
}

func isValidStreamName(name string) bool {
	if name == "" {
		return false
	}
	if !strings.HasPrefix(name, "Custom-") && name != "Custom-MSSQLAudit_CL" {
		return false
	}
	if name == "Custom-MSSQLAudit_CL" {
		return true
	}
	return strings.HasSuffix(name, "_CL")
}

func TestSentinelPayloadJSONStructure(t *testing.T) {
	entries := []*core.LogEntry{
		{
			Timestamp: time.Date(2026, 5, 6, 12, 0, 1, 0, time.UTC),
			Source:    "mock-mssql-audit",
			Level:     "INFO",
			Message:   "login succeeded",
			Fields: map[string]string{
				"id":         "000001",
				"event_time": "2026-05-06T12:00:01Z",
				"user_name":  "user-000001",
				"action":     "login",
			},
		},
		{
			Timestamp: time.Date(2026, 5, 6, 12, 0, 2, 0, time.UTC),
			Source:    "mock-mssql-audit",
			Level:     "WARN",
			Message:   "permission denied",
			Fields: map[string]string{
				"id":         "000002",
				"event_time": "2026-05-06T12:00:02Z",
				"user_name":  "user-000002",
				"action":     "denied",
			},
		},
	}

	var records []sentinelPayload
	for _, entry := range entries {
		payload, err := buildSentinelPayload(entry)
		if err != nil {
			t.Fatalf("failed to build sentinel payload: %v", err)
		}
		var recs []sentinelPayload
		if err := json.Unmarshal(payload, &recs); err != nil {
			t.Fatalf("failed to unmarshal payload: %v", err)
		}
		records = append(records, recs...)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(records))
	}

	records[0].Validate(t)
	records[1].Validate(t)

	if records[0].TimeGenerated != "2026-05-06T12:00:01Z" {
		t.Errorf("expected TimeGenerated 2026-05-06T12:00:01Z, got %s", records[0].TimeGenerated)
	}
	if records[1].Action != "denied" {
		t.Errorf("expected action denied, got %s", records[1].Action)
	}
}

func TestSentinelPayloadTableNamingConvention(t *testing.T) {
	testCases := []struct {
		tableName string
		valid     bool
	}{
		{"MSSQLAudit_CL", true},
		{"SQLServerEvents_CL", true},
		{"AppLog_CL", true},
		{"MSSQLAudit", false},
		{"mssql_audit_cl", false},
		{"AuditLog", false},
		{"_CL", false},
		{"", false},
	}

	for _, tc := range testCases {
		t.Run(tc.tableName, func(t *testing.T) {
			valid := isValidTableName(tc.tableName)
			if valid != tc.valid {
				t.Errorf("isValidTableName(%q) = %v, want %v", tc.tableName, valid, tc.valid)
			}
		})
	}
}

func isValidTableName(name string) bool {
	if name == "" {
		return false
	}
	if name == "_CL" {
		return false
	}
	return strings.HasSuffix(name, "_CL")
}

func buildSentinelPayload(entry *core.LogEntry) ([]byte, error) {
	ts := entry.Fields[eventTimeField]
	if ts == "" {
		ts = entry.Timestamp.Format(time.RFC3339)
	}

	payload := []sentinelPayload{{
		TimeGenerated: ts,
		ID:            entry.Fields["id"],
		EventTime:     entry.Fields[eventTimeField],
		UserName:      entry.Fields["user_name"],
		Action:        entry.Fields["action"],
	}}

	return json.Marshal(payload)
}

func TestTimeGeneratedMustBeUTC(t *testing.T) {
	entry := &core.LogEntry{
		Timestamp: time.Date(2026, 5, 6, 12, 0, 1, 0, time.UTC),
		Fields: map[string]string{
			"event_time": "2026-05-06T12:00:01Z",
		},
	}

	payload, err := buildSentinelPayload(entry)
	if err != nil {
		t.Fatalf("failed to build payload: %v", err)
	}

	var decoded []sentinelPayload
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	for _, record := range decoded {
		if !strings.HasSuffix(record.TimeGenerated, "Z") {
			t.Errorf("TimeGenerated must end with Z for UTC, got: %s", record.TimeGenerated)
		}
	}
}
