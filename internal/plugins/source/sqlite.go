package source

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"loghub/internal/core"
	"loghub/internal/util"
)

type sqliteConfig struct {
	Database        string   `mapstructure:"database"`
	Table           string   `mapstructure:"table"`
	Query           string   `mapstructure:"query"`
	TimestampColumn string   `mapstructure:"timestamp_column"`
	CursorColumn    string   `mapstructure:"cursor_column"`
	Name            string   `mapstructure:"name"`
	FetchSize       int      `mapstructure:"fetch_size"`
	BatchSize       int      `mapstructure:"batch_size"`
	MaxLookback     string   `mapstructure:"max_lookback"`
	MaxOpenConns    int      `mapstructure:"max_open_conns"`
	Columns         []string `mapstructure:"columns"`
	WaitForChange   string   `mapstructure:"wait_for_change"`
	SourceTimezone  string   `mapstructure:"source_timezone"`
}

func init() {
	Register("sqlite", func(_ context.Context, props map[string]interface{}, _ SecretResolver) (core.Source, error) {
		var cfg sqliteConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		src, err := NewSQLiteSourceWithCursor(
			SQLiteAuth{Database: cfg.Database},
			cfg.Table,
			cfg.Query,
			cfg.TimestampColumn,
			cfg.CursorColumn,
			cfg.Name,
			resolveFetchSize(cfg.FetchSize, cfg.BatchSize),
			cfg.MaxLookback,
			cfg.SourceTimezone,
			cfg.Columns,
		)
		if err != nil {
			return nil, err
		}
		if err := applySQLOptions(src.sqlSource, "sqlite", cfg.MaxOpenConns, cfg.WaitForChange); err != nil {
			src.Close()
			return nil, err
		}
		return src, nil
	})

}

type sqliteDriver struct {
	database string
	columns  []string
}

func (d *sqliteDriver) Open(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", d.database)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return db, nil
}

// BuildQuery compares the timestamp column with the cursor as time, not
// text: julianday parses both sides, so "2024-01-01 10:00:00",
// "2024-01-01T10:00:00Z" and different fractional digit counts all order
// correctly. Values julianday cannot parse (NULL), such as integer epochs,
// fall back to the plain comparison used before.
//
// julianday resolves only milliseconds. With a cursor column the order and
// the WHERE clause both use (julianday, tie), one consistent key, so rows
// sharing a millisecond are not skipped. Without one, the query re-reads
// the cursor's millisecond (>=), takes its row limit as ?2, and Collect
// drops the rows already delivered (see alreadyDelivered).
func (d *sqliteDriver) BuildQuery(table, query, timestampColumn, cursorColumn string, fetchSize int) string {
	if query != "" {
		return query
	}
	projection := "*"
	if len(d.columns) > 0 {
		projection = strings.Join(d.columns, ", ")
	}
	ts := timestampColumn
	if cursorColumn != "" {
		return fmt.Sprintf(`
			SELECT %[1]s FROM %[2]s
			WHERE COALESCE(julianday(%[3]s) > julianday(?1), %[3]s > ?1)
				OR (COALESCE(julianday(%[3]s) = julianday(?1), %[3]s = ?1) AND %[4]s > ?2)
			ORDER BY COALESCE(julianday(%[3]s), %[3]s) ASC, %[4]s ASC
			LIMIT %[5]d`,
			projection, table, ts, cursorColumn, fetchSize)
	}
	return fmt.Sprintf(`
		SELECT %[1]s FROM %[2]s
		WHERE COALESCE(julianday(%[3]s) >= julianday(?1), %[3]s >= ?1)
		ORDER BY COALESCE(julianday(%[3]s), %[3]s) ASC, %[3]s ASC
		LIMIT ?2`,
		projection, table, ts)
}

func (d *sqliteDriver) BindCursor(cursor sqlCursor, useTieBreaker bool) []interface{} {
	if useTieBreaker {
		return []interface{}{cursor.Timestamp, cursor.Tie}
	}
	return []interface{}{cursor.Timestamp}
}

// alreadyDelivered reports whether a row re-read by the timestamp-only
// query (which matches the cursor's whole millisecond) is at or before
// the cursor, at full precision. The row is compared at the cursor's
// precision (timestampLayout keeps 100ns), so a row is not re-sent just
// because its cursor was truncated. When either side does not parse as a
// time, only the row equal to the cursor text is dropped, which is the
// old `>` behavior.
func (d *sqliteDriver) alreadyDelivered(cursorTimestamp string, value interface{}) bool {
	if cursorTimestamp == "" {
		return false
	}
	cursorTime, cursorOK := parseSQLiteTime(cursorTimestamp)
	rowTime, rowOK := sqliteTimeValue(value)
	if cursorOK && rowOK {
		return !rowTime.Truncate(100 * time.Nanosecond).After(cursorTime)
	}
	return formatValue(value) == cursorTimestamp
}

// sqliteTimeLayouts are the SQLite date-time text formats (with a 'T' or
// a space; seconds optional). Go accepts fractional seconds after the
// seconds field without the layout naming them.
var sqliteTimeLayouts = []string{
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04Z07:00",
	"2006-01-02 15:04Z07:00",
	"2006-01-02T15:04",
	"2006-01-02 15:04",
	"2006-01-02",
}

// parseSQLiteTime parses a SQLite date-time string. Times without a zone
// are UTC, as in SQLite.
func parseSQLiteTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	for _, layout := range sqliteTimeLayouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func sqliteTimeValue(value interface{}) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		return v, true
	case string:
		return parseSQLiteTime(v)
	case []byte:
		return parseSQLiteTime(string(v))
	default:
		return time.Time{}, false
	}
}

// SQLiteAuth holds authentication configuration for SQLite.
type SQLiteAuth struct {
	Method   string
	Database string
	User     string
	Password string
}

// SQLiteSource collects logs from a SQLite database.
// It is implemented as a thin wrapper around the generic sqlSource.
type SQLiteSource struct {
	*sqlSource
}

func NewSQLiteSource(auth SQLiteAuth, table, query, timestampColumn, sourceName string, batchSize int) (*SQLiteSource, error) {
	return NewSQLiteSourceWithCursor(auth, table, query, timestampColumn, "", sourceName, batchSize, "", "", nil)
}

func NewSQLiteSourceWithCursor(auth SQLiteAuth, table, query, timestampColumn, cursorColumn, sourceName string, batchSize int, maxLookback, sourceTimezone string, columns []string) (*SQLiteSource, error) {
	if auth.Database == "" {
		return nil, fmt.Errorf("sqlite source: database is required")
	}
	if query == "" {
		if table == "" {
			return nil, fmt.Errorf("sqlite source: table is required when query is empty")
		}
		if timestampColumn == "" {
			return nil, fmt.Errorf("sqlite source: timestamp_column is required when query is empty")
		}
	}
	for _, col := range columns {
		if !validIdentifierPattern.MatchString(col) {
			return nil, fmt.Errorf("sqlite source: invalid column name: %s", col)
		}
	}
	driver := &sqliteDriver{database: auth.Database, columns: columns}
	src, err := newSQLSource(driver, table, query, timestampColumn, cursorColumn, sourceName, batchSize)
	if err != nil {
		return nil, err
	}
	if maxLookback != "" {
		d, err := time.ParseDuration(maxLookback)
		if err != nil {
			return nil, fmt.Errorf("sqlite source: invalid max_lookback: %w", err)
		}
		src.maxLookback = d
	}
	if sourceTimezone != "" {
		loc, err := time.LoadLocation(sourceTimezone)
		if err != nil {
			return nil, fmt.Errorf("sqlite source: invalid source_timezone: %w", err)
		}
		src.sourceLocation = loc
	}
	return &SQLiteSource{sqlSource: src}, nil
}
