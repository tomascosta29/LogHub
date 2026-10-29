package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"loghub/internal/core"
)

var validIdentifierPattern = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// sqlDriver abstracts database-specific behaviors for sqlSource.
type sqlDriver interface {
	Open(ctx context.Context) (*sql.DB, error)
	BuildQuery(table, query, timestampColumn, cursorColumn string, fetchSize int) string
	BindCursor(cursor sqlCursor, useTieBreaker bool) []interface{}
}

// sqlRowDeduper is implemented by drivers whose generated timestamp-only
// query also matches rows at the cursor itself (>=, see the SQLite driver).
// Such a query takes the row limit as the argument after BindCursor's.
// Collect skips the rows alreadyDelivered reports.
type sqlRowDeduper interface {
	alreadyDelivered(cursorTimestamp string, value interface{}) bool
}

// sqlSource is a generic SQL-based log source.
type sqlSource struct {
	driver          sqlDriver
	table           string
	query           string
	timestampColumn string
	cursorColumn    string
	generatedQuery  bool
	cursor          string
	sourceName      string
	batchSize       int
	maxLookback     time.Duration
	sourceLocation  *time.Location
	maxOpenConns    int
	maxIdleConns    int
	db              *sql.DB
	closed          bool
	mu              sync.Mutex
	now             func() time.Time
}

type sqlCursor struct {
	Timestamp string `json:"timestamp"`
	Tie       string `json:"tie,omitempty"`
}

func newSQLSource(driver sqlDriver, table, query, timestampColumn, cursorColumn, sourceName string, batchSize int) (*sqlSource, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("fetch_size must be positive, got %d", batchSize)
	}

	if query == "" {
		if !validIdentifierPattern.MatchString(table) {
			return nil, fmt.Errorf("invalid table name: %s", table)
		}
		if !validIdentifierPattern.MatchString(timestampColumn) {
			return nil, fmt.Errorf("invalid timestamp column name: %s", timestampColumn)
		}
		if cursorColumn != "" && !validIdentifierPattern.MatchString(cursorColumn) {
			return nil, fmt.Errorf("invalid cursor column name: %s", cursorColumn)
		}
	}

	return &sqlSource{
		driver:          driver,
		table:           table,
		query:           driver.BuildQuery(table, query, timestampColumn, cursorColumn, batchSize),
		timestampColumn: timestampColumn,
		cursorColumn:    cursorColumn,
		generatedQuery:  query == "",
		sourceName:      sourceName,
		batchSize:       batchSize,
		maxOpenConns:    1,
		maxIdleConns:    1,
		now:             time.Now,
	}, nil
}

// applySQLOptions applies the pool and deprecated settings shared by the
// SQL source factories. max_open_conns sizes the pool (idle connections
// too); 0 keeps the default of 1. wait_for_change never had an effect:
// it still loads, with a warning, so old configs keep working.
func applySQLOptions(src *sqlSource, kind string, maxOpenConns int, waitForChange string) error {
	if maxOpenConns < 0 {
		return fmt.Errorf("%s source: max_open_conns cannot be negative, got %d", kind, maxOpenConns)
	}
	if maxOpenConns > 0 {
		src.maxOpenConns = maxOpenConns
		src.maxIdleConns = maxOpenConns
	}
	if waitForChange != "" {
		slog.Warn("wait_for_change has no effect and is deprecated; the source is polled every poll_interval. Remove it from the config.", "source", src.sourceName, "type", kind)
	}
	return nil
}

func (s *sqlSource) Connect(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("source %s is closed", s.sourceName)
	}
	if s.db != nil {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	db, err := s.driver.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Default to a single connection to preserve cursor correctness
	// (a single goroutine is the only one reading from the source).
	// Operators can override via max_open_conns in the source config.
	if s.maxOpenConns <= 0 {
		s.maxOpenConns = 1
	}
	if s.maxIdleConns <= 0 {
		s.maxIdleConns = 1
	}
	db.SetMaxOpenConns(s.maxOpenConns)
	db.SetMaxIdleConns(s.maxIdleConns)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		db.Close()
		return fmt.Errorf("source %s is closed", s.sourceName)
	}
	if s.db != nil {
		// Another goroutine won the race.
		s.mu.Unlock()
		db.Close()
		return nil
	}
	s.db = db
	s.mu.Unlock()
	return nil
}

// ensureConnected performs lazy Connect on the first call, serializing
// concurrent first-callers via the mutex. The double-check inside Connect
// is the actual race guard.
func (s *sqlSource) ensureConnected(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("source %s is closed", s.sourceName)
	}
	if s.db != nil {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	return s.Connect(ctx)
}

func (s *sqlSource) Collect(ctx context.Context, cursor string) ([]*core.LogEntry, string, error) {
	if err := s.ensureConnected(ctx); err != nil {
		return nil, "", err
	}

	s.mu.Lock()
	currentCursor, err := s.effectiveCursor(cursor)
	if err != nil {
		s.mu.Unlock()
		return nil, "", err
	}
	s.cursor = cursor
	// Snapshot sourceLocation under the mutex. It is set once at construction
	// and never mutated, but snapshotting here makes that contract explicit and
	// safe in case that assumption ever changes.
	loc := s.sourceLocation
	s.mu.Unlock()

	queryCursor := s.cursorForQuery(currentCursor, loc)
	args := s.driver.BindCursor(queryCursor, s.cursorColumn != "")
	deduper, _ := s.driver.(sqlRowDeduper)
	if !s.generatedQuery || s.cursorColumn != "" {
		deduper = nil
	}
	queryStart := time.Now()
	var entries []*core.LogEntry
	if deduper == nil {
		entries, _, err = s.queryEntries(ctx, args, nil, "", loc)
		if err != nil {
			return nil, "", err
		}
	} else {
		// One extra row leaves room for the cursor's own row. Only when a
		// whole page was already delivered (many rows at the cursor's
		// timestamp) is the query repeated with a larger limit.
		for limit := s.batchSize + 1; ; limit *= 2 {
			var fetched int
			entries, fetched, err = s.queryEntries(ctx, append(args[:len(args):len(args)], limit), deduper, queryCursor.Timestamp, loc)
			if err != nil {
				return nil, "", err
			}
			if len(entries) > 0 || fetched < limit {
				break
			}
			if limit > maxRowsAtCursor {
				return nil, "", fmt.Errorf("source %s: more than %d rows share the cursor timestamp %q; configure cursor_column", s.sourceName, maxRowsAtCursor, queryCursor.Timestamp)
			}
		}
	}

	nextCursor := cursor
	if len(entries) > 0 {
		lastEntry := entries[len(entries)-1]
		if timestamp, ok := lastEntry.Fields[s.timestampColumn]; ok {
			next := sqlCursor{Timestamp: timestamp}
			if s.cursorColumn != "" {
				next.Tie = lastEntry.Fields[s.cursorColumn]
			}
			nextCursor = formatSQLCursor(next)
		}
	}

	s.mu.Lock()
	s.cursor = nextCursor
	s.mu.Unlock()
	slog.Debug("Source query completed", "source", s.sourceName, "rows", len(entries), "duration", time.Since(queryStart), "next_cursor", nextCursor)

	return entries, nextCursor, nil
}

// maxRowsAtCursor bounds how many rows at the cursor's own timestamp a
// deduplicating query re-reads before Collect gives up. A variable so
// tests can lower it.
var maxRowsAtCursor = 100000

// queryEntries runs the source query with args and converts the rows to
// entries. With a deduper, it skips the rows already delivered and stops
// at batchSize entries. It also returns the number of rows read.
func (s *sqlSource) queryEntries(ctx context.Context, args []interface{}, deduper sqlRowDeduper, cursorTimestamp string, loc *time.Location) ([]*core.LogEntry, int, error) {
	slog.Debug("Executing source query", "source", s.sourceName, "query", compactQuery(s.query), "args", formatQueryArgs(args))
	rows, err := s.db.QueryContext(ctx, s.query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get columns: %w", err)
	}

	colIndex := columnIndex(columns, s.timestampColumn)

	var entries []*core.LogEntry
	fetched := 0
	for rows.Next() {
		values, err := scanRow(rows, columns)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan row: %w", err)
		}
		fetched++
		if deduper != nil {
			if colIndex >= 0 && deduper.alreadyDelivered(cursorTimestamp, values[colIndex]) {
				continue
			}
			if len(entries) == s.batchSize {
				break
			}
		}

		entry := s.rowToLogEntry(values, columns, colIndex, loc)
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("rows error: %w", err)
	}
	return entries, fetched, nil
}

func (s *sqlSource) effectiveCursor(cursor string) (sqlCursor, error) {
	currentCursor, err := parseSQLCursor(cursor)
	if err != nil {
		return sqlCursor{}, fmt.Errorf("source %s: checkpoint cursor %q: %w; fix the checkpoint, or delete it to start from the beginning (or max_lookback)", s.sourceName, cursor, err)
	}
	if cursor != "" || s.maxLookback <= 0 {
		return currentCursor, nil
	}
	currentCursor.Timestamp = s.now().UTC().Add(-s.maxLookback).Format(time.RFC3339)
	slog.Info("Applying max lookback to empty cursor", "source", s.sourceName, "max_lookback", s.maxLookback, "cursor", currentCursor.Timestamp)
	return currentCursor, nil
}

func (s *sqlSource) cursorForQuery(cursor sqlCursor, loc *time.Location) sqlCursor {
	if loc == nil || cursor.Timestamp == "" {
		return cursor
	}

	if ts, ok := parseRFC3339Timestamp(cursor.Timestamp); ok {
		cursor.Timestamp = ts.In(loc).Format(sourceLocalLayout)
	}

	return cursor
}

// Cursor format version. Bump this when changing the cursor schema;
// parseSQLCursor rejects versions it does not know. Unprefixed (v0)
// cursors from older builds are still accepted on read.
const cursorFormatVersion = "v1"

// errInvalidCursor is wrapped by parseSQLCursor for a cursor it cannot
// trust. Collect fails with it instead of guessing, because a guessed
// cursor silently re-reads (and re-sends) the whole table.
var errInvalidCursor = errors.New("invalid cursor")

// cursorVersionPrefix matches a version prefix such as "v1:".
var cursorVersionPrefix = regexp.MustCompile(`^v([0-9]+):`)

// parseSQLCursor parses a persisted cursor. The current format, v1, is
// "v1:" followed by either a JSON object {"timestamp", "tie"} (when a
// cursor column is configured) or the bare timestamp. Unprefixed cursors
// (v0, written by older builds) are a JSON object or a bare timestamp.
//
// It returns an error wrapping errInvalidCursor for an unknown version,
// a JSON object that does not decode, or a cursor without a timestamp.
// An empty cursor means "no checkpoint yet" and is not an error.
func parseSQLCursor(cursor string) (sqlCursor, error) {
	if cursor == "" {
		return sqlCursor{}, nil
	}
	body := cursor
	if m := cursorVersionPrefix.FindStringSubmatch(cursor); m != nil {
		if m[0] != cursorFormatVersion+":" {
			return sqlCursor{}, fmt.Errorf("%w: unsupported version %q (written by a newer LogHub?)", errInvalidCursor, "v"+m[1])
		}
		body = cursor[len(m[0]):]
	}
	if strings.HasPrefix(body, "{") {
		var parsed sqlCursor
		if err := json.Unmarshal([]byte(body), &parsed); err != nil {
			return sqlCursor{}, fmt.Errorf("%w: %v", errInvalidCursor, err)
		}
		if parsed.Timestamp == "" {
			return sqlCursor{}, fmt.Errorf("%w: no timestamp", errInvalidCursor)
		}
		return parsed, nil
	}
	if body == "" {
		return sqlCursor{}, fmt.Errorf("%w: no timestamp", errInvalidCursor)
	}
	return sqlCursor{Timestamp: body}, nil
}

func formatSQLCursor(cursor sqlCursor) string {
	if cursor.Tie == "" {
		return cursorFormatVersion + ":" + cursor.Timestamp
	}
	data, err := json.Marshal(cursor)
	if err != nil {
		return cursorFormatVersion + ":" + cursor.Timestamp
	}
	return cursorFormatVersion + ":" + string(data)
}

func compactQuery(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

func formatQueryArgs(args []interface{}) []string {
	formatted := make([]string, 0, len(args))
	for _, arg := range args {
		if named, ok := arg.(sql.NamedArg); ok {
			formatted = append(formatted, fmt.Sprintf("%s=%v", named.Name, named.Value))
			continue
		}
		formatted = append(formatted, fmt.Sprintf("%v", arg))
	}
	return formatted
}

func (s *sqlSource) rowToLogEntry(values []interface{}, columns []string, timestampColIndex int, loc *time.Location) *core.LogEntry {
	fields := make(map[string]string)
	var timestamp time.Time

	for i, col := range columns {
		if col == s.timestampColumn {
			normalized, ok := normalizeTimestampValue(values[i], loc)
			if ok {
				timestamp = normalized
				fields[col] = normalized.UTC().Format(timestampLayout)
				continue
			}
		}
		fields[col] = formatValue(values[i])
	}

	if timestamp.IsZero() && timestampColIndex >= 0 && timestampColIndex < len(values) {
		timestamp = parseTimestamp(values[timestampColIndex])
	}

	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	entry := &core.LogEntry{
		Timestamp: timestamp,
		Source:    s.sourceName,
		Fields:    fields,
	}
	cursor := sqlCursor{}
	if !entry.Timestamp.IsZero() {
		cursor.Timestamp = entry.Timestamp.UTC().Format(timestampLayout)
	} else if cursorTimestamp, ok := entry.Fields[s.timestampColumn]; ok {
		cursor.Timestamp = cursorTimestamp
	}
	if cursor.Timestamp != "" {
		if s.cursorColumn != "" {
			cursor.Tie = entry.Fields[s.cursorColumn]
		}
		entry.Cursor = formatSQLCursor(cursor)
	}
	return entry
}

func (s *sqlSource) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	db := s.db
	s.mu.Unlock()
	if db != nil {
		return db.Close()
	}
	return nil
}

func (s *sqlSource) Name() string {
	return s.sourceName
}
