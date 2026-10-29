package source

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Live tests against a real SQL Server. They run only when
// LOGHUB_MSSQL_TEST_SERVER is set (host:port), with SQL auth from
// LOGHUB_MSSQL_TEST_USER (default "sa") and LOGHUB_MSSQL_TEST_PASSWORD.
// CI runs them against a SQL Server container; locally:
//
//	docker run -d -e ACCEPT_EULA=Y -e 'MSSQL_SA_PASSWORD=<password>' -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest
//	LOGHUB_MSSQL_TEST_SERVER=localhost:1433 LOGHUB_MSSQL_TEST_PASSWORD='<password>' go test ./internal/plugins/source -run Live
func liveMSSQLAuth(t *testing.T) MSSQLAuth {
	t.Helper()
	server := os.Getenv("LOGHUB_MSSQL_TEST_SERVER")
	if server == "" {
		t.Skip("LOGHUB_MSSQL_TEST_SERVER not set")
	}
	user := os.Getenv("LOGHUB_MSSQL_TEST_USER")
	if user == "" {
		user = "sa"
	}
	return MSSQLAuth{
		Method:   "sql",
		Server:   server,
		Database: "master",
		User:     user,
		Password: os.Getenv("LOGHUB_MSSQL_TEST_PASSWORD"),
		// The container's certificate is self-signed.
		TrustServerCertificate: true,
	}
}

// liveMSSQLTable creates a uniquely named table in master, dropped when
// the test ends, and returns its name and a connection to it.
func liveMSSQLTable(t *testing.T, auth MSSQLAuth, columns string) (string, *sql.DB) {
	t.Helper()
	db, err := (&mssqlDriver{auth: auth}).Open(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	table := fmt.Sprintf("loghub_live_%d", time.Now().UnixNano())
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (%s)", table, columns)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })
	return table, db
}

// The driver connects over TLS (encrypt is forced on) and pages through
// rows that share a sub-second DATETIME2(7) timestamp using the cursor
// column, each row once.
func TestLiveMSSQLCollectPagesWithCursorColumn(t *testing.T) {
	auth := liveMSSQLAuth(t)
	table, db := liveMSSQLTable(t, auth, "id INT IDENTITY PRIMARY KEY, event_time DATETIME2(7) NOT NULL, message NVARCHAR(100) NOT NULL")
	for _, row := range []struct{ ts, msg string }{
		{"2024-01-01T10:00:00.1234567", "a"},
		{"2024-01-01T10:00:00.1234567", "b"},
		{"2024-01-01T10:00:00.1234568", "c"},
		{"2024-01-01T10:00:01", "d"},
		{"2024-01-02T09:00:00", "e"},
	} {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (event_time, message) VALUES (@p1, @p2)", table), row.ts, row.msg); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	src, err := NewMSSQLSourceWithCursor(auth, table, "", "event_time", "id", "live", 2, "", "", []string{"id", "event_time", "message"})
	if err != nil {
		t.Fatalf("NewMSSQLSourceWithCursor: %v", err)
	}
	defer src.Close()

	var got []string
	cursor := ""
	for i := 0; i < 10; i++ {
		entries, next, err := src.Collect(context.Background(), cursor)
		if err != nil {
			t.Fatalf("Collect(%q): %v", cursor, err)
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			got = append(got, e.Fields["message"])
		}
		cursor = next
	}
	if strings.Join(got, ",") != "a,b,c,d,e" {
		t.Fatalf("delivered %v, want [a b c d e]", got)
	}

	// The source's own connection: the constructor forces encrypt on.
	var encrypted string
	if err := src.db.QueryRow("SELECT CAST(encrypt_option AS NVARCHAR(10)) FROM sys.dm_exec_connections WHERE session_id = @@SPID").Scan(&encrypted); err != nil {
		t.Fatalf("query encrypt_option: %v", err)
	}
	if encrypted != "TRUE" {
		t.Errorf("source connection encrypt_option = %q, want TRUE", encrypted)
	}
}

// Microsoft Entra auth needs Azure; this only checks that a wrong SQL
// password fails with the server's login error rather than hanging.
func TestLiveMSSQLWrongPasswordFails(t *testing.T) {
	auth := liveMSSQLAuth(t)
	table, _ := liveMSSQLTable(t, auth, "event_time DATETIME2 NOT NULL")
	auth.Password += "-wrong"
	src, err := NewMSSQLSourceWithCursor(auth, table, "", "event_time", "", "live", 1, "", "", nil)
	if err != nil {
		t.Fatalf("NewMSSQLSourceWithCursor: %v", err)
	}
	defer src.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, err = src.Collect(ctx, "")
	if err == nil || !strings.Contains(err.Error(), "Login failed") {
		t.Fatalf("Collect error = %v, want a login failure", err)
	}
}
