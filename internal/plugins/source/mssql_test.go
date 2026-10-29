package source

import (
	"context"
	"database/sql"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestMSSQLSourceConstructorValidation(t *testing.T) {
	tests := []struct {
		name         string
		auth         MSSQLAuth
		table        string
		query        string
		timestampCol string
		cursorCol    string
		sourceName   string
		batchSize    int
		wantErr      string
	}{
		{
			name: "empty server",
			auth: MSSQLAuth{
				Server:   "",
				Database: "testdb",
				Method:   "sql",
				User:     "user",
				Password: "password",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: server is required",
		},
		{
			name: "empty database",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "",
				Method:   "sql",
				User:     "user",
				Password: "password",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: database is required",
		},
		{
			name: "empty query with empty table",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "testdb",
				Method:   "sql",
				User:     "user",
				Password: "password",
			},
			table:        "",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: table is required when query is empty",
		},
		{
			name: "empty query with empty timestamp_column",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "testdb",
				Method:   "sql",
				User:     "user",
				Password: "password",
			},
			table:        "events",
			query:        "",
			timestampCol: "",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: timestamp_column is required when query is empty",
		},
		{
			name: "empty auth method",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "testdb",
				Method:   "",
				User:     "user",
				Password: "password",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: auth method is required",
		},
		{
			name: "sql auth missing user",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "testdb",
				Method:   "sql",
				User:     "",
				Password: "password",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: sql auth: user is required",
		},
		{
			name: "sql auth missing password",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "testdb",
				Method:   "sql",
				User:     "user",
				Password: "",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: sql auth: password is required",
		},
		{
			name: "ad-password auth missing user",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "testdb",
				Method:   "ad-password",
				User:     "",
				Password: "password",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: ad-password auth: user is required",
		},
		{
			name: "ad-password auth missing password",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "testdb",
				Method:   "ad-password",
				User:     "user",
				Password: "",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: ad-password auth: password is required",
		},
		{
			name: "ad-service-principal auth missing tenant_id",
			auth: MSSQLAuth{
				Server:    "localhost",
				Database:  "testdb",
				Method:    "ad-service-principal",
				TenantID:  "",
				ClientID:  "client-id",
				ClientSec: "client-secret",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: ad-service-principal auth: tenant_id is required",
		},
		{
			name: "ad-service-principal auth missing client_id",
			auth: MSSQLAuth{
				Server:    "localhost",
				Database:  "testdb",
				Method:    "ad-service-principal",
				TenantID:  "tenant-id",
				ClientID:  "",
				ClientSec: "client-secret",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: ad-service-principal auth: client_id is required",
		},
		{
			name: "ad-service-principal auth missing client_secret",
			auth: MSSQLAuth{
				Server:    "localhost",
				Database:  "testdb",
				Method:    "ad-service-principal",
				TenantID:  "tenant-id",
				ClientID:  "client-id",
				ClientSec: "",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: ad-service-principal auth: client_secret is required",
		},
		{
			name: "unsupported auth method",
			auth: MSSQLAuth{
				Server:   "localhost",
				Database: "testdb",
				Method:   "unsupported",
			},
			table:        "events",
			query:        "",
			timestampCol: "event_time",
			sourceName:   "test",
			batchSize:    10,
			wantErr:      "mssql source: unsupported auth method: unsupported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewMSSQLSourceWithCursor(tt.auth, tt.table, tt.query, tt.timestampCol, tt.cursorCol, tt.sourceName, tt.batchSize, "", "", nil)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestMSSQLSourceValidAuth(t *testing.T) {
	auth := MSSQLAuth{
		Server:   "localhost",
		Database: "testdb",
		Method:   "sql",
		User:     "user",
		Password: "password",
	}

	src, err := NewMSSQLSourceWithCursor(auth, "events", "SELECT * FROM events", "event_time", "", "test", 10, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src == nil {
		t.Fatal("expected non-nil source")
	}
	src.Close()
}

func TestMSSQLSourceADManagedIdentityValid(t *testing.T) {
	auth := MSSQLAuth{
		Server:   "localhost",
		Database: "testdb",
		Method:   "ad-managed-identity",
	}

	src, err := NewMSSQLSourceWithCursor(auth, "events", "SELECT * FROM events", "event_time", "", "test", 10, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src == nil {
		t.Fatal("expected non-nil source")
	}
	src.Close()
}

func TestMSSQLSourceADServicePrincipalValid(t *testing.T) {
	auth := MSSQLAuth{
		Server:    "localhost",
		Database:  "testdb",
		Method:    "ad-service-principal",
		TenantID:  "tenant-id",
		ClientID:  "client-id",
		ClientSec: "client-secret",
	}

	src, err := NewMSSQLSourceWithCursor(auth, "events", "SELECT * FROM events", "event_time", "", "test", 10, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src == nil {
		t.Fatal("expected non-nil source")
	}
	src.Close()
}

type mockSecretResolver struct{}

func (m *mockSecretResolver) Resolve(ctx context.Context, key string) (string, error) {
	return key, nil
}

func TestMSSQLBuildQueryWithCursorColumn(t *testing.T) {
	driver := &mssqlDriver{}
	query := driver.BuildQuery("events", "", "event_time", "id", 100)
	expected := `
			SELECT TOP (100) * FROM events
			WHERE event_time > @ts_cursor OR (event_time = @ts_cursor AND id > @tie_cursor)
			ORDER BY event_time ASC, id ASC`
	if query != expected {
		t.Errorf("unexpected query:\ngot:  %s\nwant: %s", query, expected)
	}
}

func TestMSSQLBuildQueryWithoutCursorColumn(t *testing.T) {
	driver := &mssqlDriver{}
	query := driver.BuildQuery("events", "", "event_time", "", 100)
	if query == "" {
		t.Fatal("expected non-empty query")
	}
	if !containsString(query, "SELECT TOP (100)") {
		t.Error("query missing SELECT TOP clause")
	}
	if !containsString(query, "event_time > @cursor") {
		t.Error("query missing cursor comparison")
	}
	if !containsString(query, "ORDER BY event_time ASC") {
		t.Error("query missing ORDER BY clause")
	}
}

func TestMSSQLBuildQueryWithCustomQuery(t *testing.T) {
	driver := &mssqlDriver{}
	customQuery := "SELECT * FROM events WHERE event_type = 'login'"
	query := driver.BuildQuery("events", customQuery, "event_time", "id", 50)
	if query != customQuery {
		t.Errorf("expected custom query to be returned unchanged, got: %s", query)
	}
}

func TestMSSQLBindCursorWithTieBreaker(t *testing.T) {
	driver := &mssqlDriver{}
	cursor := sqlCursor{Timestamp: "2026-05-06T12:00:00Z", Tie: "000123"}
	bindings := driver.BindCursor(cursor, true)
	if len(bindings) != 2 {
		t.Fatalf("expected 2 bindings, got %d", len(bindings))
	}
	tsBinding := bindings[0].(sql.NamedArg)
	if tsBinding.Name != "ts_cursor" {
		t.Errorf("expected ts_cursor, got %s", tsBinding.Name)
	}
	if tsBinding.Value != "2026-05-06T12:00:00Z" {
		t.Errorf("unexpected timestamp value: %v", tsBinding.Value)
	}
	tieBinding := bindings[1].(sql.NamedArg)
	if tieBinding.Name != "tie_cursor" {
		t.Errorf("expected tie_cursor, got %s", tieBinding.Name)
	}
	if tieBinding.Value != "000123" {
		t.Errorf("unexpected tie value: %v", tieBinding.Value)
	}
}

func TestMSSQLBindCursorWithoutTieBreaker(t *testing.T) {
	driver := &mssqlDriver{}
	cursor := sqlCursor{Timestamp: "2026-05-06T12:00:00Z"}
	bindings := driver.BindCursor(cursor, false)
	if len(bindings) != 1 {
		t.Fatalf("expected 1 binding, got %d", len(bindings))
	}
	binding := bindings[0].(sql.NamedArg)
	if binding.Name != "cursor" {
		t.Errorf("expected cursor, got %s", binding.Name)
	}
	if binding.Value != "2026-05-06T12:00:00Z" {
		t.Errorf("unexpected value: %v", binding.Value)
	}
}

func TestMSSQLDuplicateTimestampTieBreakerQuery(t *testing.T) {
	driver := &mssqlDriver{}
	query := driver.BuildQuery("MSSQLAudit", "", "event_time", "id", 200)

	if !containsString(query, "event_time > @ts_cursor") {
		t.Error("query missing timestamp comparison")
	}
	if !containsString(query, "event_time = @ts_cursor AND id > @tie_cursor") {
		t.Error("query missing duplicate timestamp tie-breaker logic")
	}
	if !containsString(query, "ORDER BY event_time ASC, id ASC") {
		t.Error("query missing required ORDER BY clause")
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStringHelper(s, substr))
}

func containsStringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestMSSQLSourceColumnsAllowlist(t *testing.T) {
	auth := MSSQLAuth{
		Server:   "localhost",
		Database: "testdb",
		Method:   "sql",
		User:     "user",
		Password: "password",
	}

	// 1. Invalid columns are rejected
	_, err := NewMSSQLSourceWithCursor(
		auth,
		"events",
		"",
		"event_time",
		"",
		"test",
		10,
		"",
		"",
		[]string{"message", "id; DROP TABLE events"},
	)
	if err == nil {
		t.Error("expected error with invalid column identifier, got nil")
	}

	// 2. Valid columns projection query build
	driver := &mssqlDriver{columns: []string{"event_time", "message"}}
	query := driver.BuildQuery("events", "", "event_time", "", 10)
	if !containsString(query, "SELECT TOP (10) event_time, message FROM events") {
		t.Errorf("query does not contain expected columns projection, got: %q", query)
	}
}

func TestMSSQLConnectionStringEncryptDefault(t *testing.T) {
	auth := MSSQLAuth{
		Server:   "localhost",
		Database: "testdb",
		Method:   "sql",
		User:     "user",
		Password: "password",
	}
	src, err := NewMSSQLSourceWithCursor(auth, "events", "", "event_time", "", "test", 10, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer src.Close()

	params := dsnParams(t, src.sqlSource.driver.(*mssqlDriver).dsn())
	if params["encrypt"] != "true" {
		t.Errorf("connection string should default to encrypt=true, got params %v", params)
	}
}

func TestMSSQLConnectionStringTrustServerCertificateWarning(t *testing.T) {
	auth := MSSQLAuth{
		Server:                 "localhost",
		Database:               "testdb",
		Method:                 "sql",
		User:                   "user",
		Password:               "password",
		TrustServerCertificate: true,
	}
	src, err := NewMSSQLSourceWithCursor(auth, "events", "", "event_time", "", "test", 10, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer src.Close()

	params := dsnParams(t, src.sqlSource.driver.(*mssqlDriver).dsn())
	if params["trustservercertificate"] != "true" || params["encrypt"] != "true" {
		t.Errorf("connection string should set TrustServerCertificate=true and encrypt=true, got params %v", params)
	}
}

// dsnParams parses dsn the way the driver does and returns its
// parameters, keyed in lower case.
func dsnParams(t *testing.T, dsn string) map[string]string {
	t.Helper()
	cfg, err := msdsn.Parse(dsn)
	if err != nil {
		t.Fatalf("msdsn.Parse(%q): %v", dsn, err)
	}
	return cfg.Parameters
}

func TestMSSQLDSNEscapesCredentials(t *testing.T) {
	d := &mssqlDriver{auth: MSSQLAuth{
		Method: "sql", Server: "db.example.com,1433", Database: "logs;x",
		User: "loghub@corp", Password: "p;w=d@ x}{", Encrypt: true,
	}}
	params := dsnParams(t, d.dsn())
	want := map[string]string{
		"user id": "loghub@corp", "password": "p;w=d@ x}{", "database": "logs;x",
		"server": "db.example.com", "port": "1433", "encrypt": "true",
	}
	for k, v := range want {
		if params[k] != v {
			t.Errorf("param %q = %q, want %q", k, params[k], v)
		}
	}
}

func TestMSSQLDSNNamedInstance(t *testing.T) {
	d := &mssqlDriver{auth: MSSQLAuth{Method: "sql", Server: `sqlhost\AUDIT`, Database: "db", User: "u", Password: "p"}}
	if got := dsnParams(t, d.dsn())["server"]; got != `sqlhost\AUDIT` {
		t.Errorf("server = %q, want %q", got, `sqlhost\AUDIT`)
	}
}

func TestMSSQLDSNEntraMethodsCarryNoCredentials(t *testing.T) {
	for _, method := range []string{"ad-managed-identity", "ad-service-principal", "ad-password"} {
		d := &mssqlDriver{auth: MSSQLAuth{
			Method: method, Server: "db.example.com", Database: "db",
			User: "u", Password: "secret", TenantID: "t", ClientID: "c", ClientSec: "secret",
		}}
		params := dsnParams(t, d.dsn())
		if params["user id"] != "" || params["password"] != "" {
			t.Errorf("%s: DSN carries credentials %v; the token connector authenticates instead", method, params)
		}
	}
}

// Open must build a token-based pool for each Entra method without
// contacting Azure (sql.OpenDB connects lazily). The old code passed
// ADO-style keywords the driver ignored, so these methods never worked.
func TestMSSQLOpenEntraMethods(t *testing.T) {
	cases := map[string]MSSQLAuth{
		"system-assigned managed identity": {Method: "ad-managed-identity"},
		"user-assigned managed identity":   {Method: "ad-managed-identity", ClientID: "33333333-3333-3333-3333-333333333333"},
		"service principal": {Method: "ad-service-principal", TenantID: "22222222-2222-2222-2222-222222222222",
			ClientID: "33333333-3333-3333-3333-333333333333", ClientSec: "s"},
		"password": {Method: "ad-password", ClientID: "33333333-3333-3333-3333-333333333333", User: "u@example.com", Password: "p"},
	}
	for name, auth := range cases {
		auth.Server, auth.Database, auth.Encrypt = "db.example.com", "db", true
		db, err := (&mssqlDriver{auth: auth}).Open(context.Background())
		if err != nil {
			t.Errorf("%s: Open: %v", name, err)
			continue
		}
		db.Close()
	}
	if _, err := (&mssqlDriver{auth: MSSQLAuth{Method: "ad-service-principal", Server: "s", Database: "d"}}).Open(context.Background()); err == nil {
		t.Error("Open with an incomplete service principal: want an error")
	}
}
