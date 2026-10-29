package source

import (
	"database/sql"
	"fmt"
	"time"
)

func columnIndex(columns []string, name string) int {
	for i, col := range columns {
		if col == name {
			return i
		}
	}
	return -1
}

func scanRow(rows *sql.Rows, columns []string) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}
	return values, nil
}

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(timestampLayout)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}

// timestampLayout formats timestamps read from a source, and the cursor
// built from them. It keeps up to 7 fractional digits: the precision of
// SQL Server's DATETIME2, and the most it accepts when converting the
// cursor back. Whole seconds would make `ts > cursor` match the rows of
// the cursor's last second again on every poll.
const timestampLayout = "2006-01-02T15:04:05.9999999Z07:00"

// sourceLocalLayout is timestampLayout without a zone, for sources whose
// timestamps are stored in local time (source_timezone).
const sourceLocalLayout = "2006-01-02 15:04:05.9999999"

func parseTimestamp(v interface{}) time.Time {
	switch val := v.(type) {
	case time.Time:
		return val
	case []byte:
		if t, err := time.Parse(time.RFC3339, string(val)); err == nil {
			return t
		}
		if t, err := time.Parse("2006-01-02 15:04:05", string(val)); err == nil {
			return t
		}
	case string:
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t
		}
		if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
			return t
		}
	}
	return time.Time{}
}

func parseRFC3339Timestamp(value string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, true
	}
	return time.Time{}, false
}

func normalizeTimestampValue(v interface{}, loc *time.Location) (time.Time, bool) {
	if loc == nil {
		return time.Time{}, false
	}
	switch val := v.(type) {
	case time.Time:
		return reinterpretTimeInLocation(val, loc), true
	case []byte:
		return parseTimestampInLocation(string(val), loc)
	case string:
		return parseTimestampInLocation(val, loc)
	default:
		return time.Time{}, false
	}
}

func parseTimestampInLocation(value string, loc *time.Location) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, true
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", value, loc); err == nil {
		return t, true
	}
	return time.Time{}, false
}

func reinterpretTimeInLocation(t time.Time, loc *time.Location) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
}
