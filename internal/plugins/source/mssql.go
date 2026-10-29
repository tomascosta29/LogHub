package source

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"loghub/internal/azauth"
	"loghub/internal/core"
	"loghub/internal/util"
)

type mssqlConfig struct {
	Auth                   authConfig `mapstructure:"auth"`
	Server                 string     `mapstructure:"server"`
	Database               string     `mapstructure:"database"`
	Table                  string     `mapstructure:"table"`
	Query                  string     `mapstructure:"query"`
	TimestampColumn        string     `mapstructure:"timestamp_column"`
	CursorColumn           string     `mapstructure:"cursor_column"`
	Name                   string     `mapstructure:"name"`
	FetchSize              int        `mapstructure:"fetch_size"`
	BatchSize              int        `mapstructure:"batch_size"`
	MaxLookback            string     `mapstructure:"max_lookback"`
	MaxOpenConns           int        `mapstructure:"max_open_conns"`
	Columns                []string   `mapstructure:"columns"`
	WaitForChange          string     `mapstructure:"wait_for_change"`
	SourceTimezone         string     `mapstructure:"source_timezone"`
	TrustServerCertificate bool       `mapstructure:"trust_server_certificate"`
	Encrypt                bool       `mapstructure:"encrypt"`
}

type authConfig struct {
	Method       string `mapstructure:"method"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	TenantID     string `mapstructure:"tenant_id"`
	ClientID     string `mapstructure:"client_id"`
	ClientSecret string `mapstructure:"client_secret"`
}

func init() {
	Register("mssql", func(ctx context.Context, props map[string]interface{}, sh SecretResolver) (core.Source, error) {
		var cfg mssqlConfig
		if err := util.DecodeProperties(props, &cfg); err != nil {
			return nil, err
		}
		auth, err := resolveAuth(ctx, cfg.Auth, sh)
		if err != nil {
			return nil, err
		}
		src, err := NewMSSQLSourceWithCursor(
			MSSQLAuth{
				Method:                 auth.Method,
				Server:                 cfg.Server,
				Database:               cfg.Database,
				User:                   auth.User,
				Password:               auth.Password,
				TenantID:               auth.TenantID,
				ClientID:               auth.ClientID,
				ClientSec:              auth.ClientSecret,
				TrustServerCertificate: cfg.TrustServerCertificate,
				Encrypt:                cfg.Encrypt,
			},
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
		if err := applySQLOptions(src.sqlSource, "mssql", cfg.MaxOpenConns, cfg.WaitForChange); err != nil {
			src.Close()
			return nil, err
		}
		return src, nil
	})

}

func resolveAuth(ctx context.Context, auth authConfig, sh SecretResolver) (authConfig, error) {
	var err error
	auth.User, err = resolveField(ctx, sh, "user", auth.User)
	if err != nil {
		return auth, fmt.Errorf("failed to resolve user: %w", err)
	}
	auth.Password, err = resolveField(ctx, sh, "password", auth.Password)
	if err != nil {
		return auth, fmt.Errorf("failed to resolve password: %w", err)
	}
	auth.TenantID, err = resolveField(ctx, sh, "tenant_id", auth.TenantID)
	if err != nil {
		return auth, fmt.Errorf("failed to resolve tenant_id: %w", err)
	}
	auth.ClientID, err = resolveField(ctx, sh, "client_id", auth.ClientID)
	if err != nil {
		return auth, fmt.Errorf("failed to resolve client_id: %w", err)
	}
	auth.ClientSecret, err = resolveField(ctx, sh, "client_secret", auth.ClientSecret)
	if err != nil {
		return auth, fmt.Errorf("failed to resolve client_secret: %w", err)
	}
	return auth, nil
}

// resolveField resolves a secret reference in value. Without a resolver
// (sh is nil) the value is used as written.
func resolveField(ctx context.Context, sh SecretResolver, fieldName, value string) (string, error) {
	if value == "" || sh == nil {
		return value, nil
	}
	return sh.Resolve(ctx, value)
}

type mssqlDriver struct {
	auth    MSSQLAuth
	columns []string
}

// azureSQLScope is the token scope for Azure SQL Database and Managed
// Instance.
const azureSQLScope = "https://database.windows.net/.default"

// Open returns a connection pool for the configured auth method. SQL
// auth passes the user and password in the DSN. The Microsoft Entra
// methods get an access token from azauth for every new connection.
func (d *mssqlDriver) Open(ctx context.Context) (*sql.DB, error) {
	dsn := d.dsn()
	if d.auth.Method == "sql" {
		db, err := sql.Open("sqlserver", dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open connection: %w", err)
		}
		return db, nil
	}

	cred, err := azauth.NewCredential(azauth.Config{
		Method:       d.auth.Method,
		TenantID:     d.auth.TenantID,
		ClientID:     d.auth.ClientID,
		ClientSecret: d.auth.ClientSec,
		User:         d.auth.User,
		UserPassword: d.auth.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create credential: %w", err)
	}
	cfg, err := msdsn.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	connector, err := mssql.NewSecurityTokenConnector(cfg, azauth.TokenFunc(cred, azureSQLScope))
	if err != nil {
		return nil, fmt.Errorf("failed to create connector: %w", err)
	}
	return sql.OpenDB(connector), nil
}

// BuildQuery returns the SELECT query. If a custom query is set, it
// is used as-is. Otherwise the table-based query is built, with the
// column projection either `*` (default) or a parenthesized list
// from `columns`.
func (d *mssqlDriver) BuildQuery(table, query, timestampColumn, cursorColumn string, fetchSize int) string {
	if query != "" {
		return query
	}
	projection := "*"
	if len(d.columns) > 0 {
		projection = strings.Join(d.columns, ", ")
	}
	if cursorColumn != "" {
		return fmt.Sprintf(`
			SELECT TOP (%d) %s FROM %s
			WHERE %s > @ts_cursor OR (%s = @ts_cursor AND %s > @tie_cursor)
			ORDER BY %s ASC, %s ASC`,
			fetchSize, projection, table, timestampColumn, timestampColumn, cursorColumn, timestampColumn, cursorColumn)
	}
	return fmt.Sprintf(`
		SELECT TOP (%d) %s FROM %s
		WHERE %s > @cursor
		ORDER BY %s ASC`,
		fetchSize, projection, table, timestampColumn, timestampColumn)
}

func (d *mssqlDriver) BindCursor(cursor sqlCursor, useTieBreaker bool) []interface{} {
	if useTieBreaker {
		return []interface{}{sql.Named("ts_cursor", cursor.Timestamp), sql.Named("tie_cursor", cursor.Tie)}
	}
	return []interface{}{sql.Named("cursor", cursor.Timestamp)}
}

// dsn returns the connection string in URL form, so that net/url
// escapes the user and password: they may contain ';', '=', '@' or
// spaces. Only SQL auth puts credentials in it; the Entra methods
// authenticate with a token instead.
func (d *mssqlDriver) dsn() string {
	host, instance := splitServer(d.auth.Server)
	u := &url.URL{Scheme: "sqlserver", Host: host}
	if instance != "" {
		u.Path = "/" + instance
	}
	if d.auth.Method == "sql" {
		u.User = url.UserPassword(d.auth.User, d.auth.Password)
	}
	q := url.Values{}
	q.Set("database", d.auth.Database)
	q.Set("encrypt", strconv.FormatBool(d.auth.Encrypt))
	q.Set("TrustServerCertificate", strconv.FormatBool(d.auth.TrustServerCertificate))
	u.RawQuery = q.Encode()
	return u.String()
}

// splitServer turns the configured server into a URL host and an
// optional named instance. It accepts "host", "host:port", the ADO form
// "host,port", and "host\instance".
func splitServer(server string) (host, instance string) {
	if i := strings.IndexByte(server, '\\'); i >= 0 {
		server, instance = server[:i], server[i+1:]
	}
	return strings.Replace(server, ",", ":", 1), instance
}

// MSSQLAuth holds authentication configuration for MSSQL.
type MSSQLAuth struct {
	Method                 string
	Server                 string
	Database               string
	User                   string
	Password               string
	TenantID               string
	ClientID               string
	ClientSec              string
	TrustServerCertificate bool
	Encrypt                bool
}

// MSSQLSource collects logs from SQL Server.
// It is implemented as a thin wrapper around the generic sqlSource.
type MSSQLSource struct {
	*sqlSource
}

func NewMSSQLSource(auth MSSQLAuth, table, query, timestampColumn, sourceName string, batchSize int) (*MSSQLSource, error) {
	return NewMSSQLSourceWithCursor(auth, table, query, timestampColumn, "", sourceName, batchSize, "", "", nil)
}

func NewMSSQLSourceWithCursor(auth MSSQLAuth, table, query, timestampColumn, cursorColumn, sourceName string, batchSize int, maxLookback, sourceTimezone string, columns []string) (*MSSQLSource, error) {
	if auth.Server == "" {
		return nil, fmt.Errorf("mssql source: server is required")
	}
	if auth.Database == "" {
		return nil, fmt.Errorf("mssql source: database is required")
	}
	if query == "" {
		if table == "" {
			return nil, fmt.Errorf("mssql source: table is required when query is empty")
		}
		if timestampColumn == "" {
			return nil, fmt.Errorf("mssql source: timestamp_column is required when query is empty")
		}
	}
	if err := validateAuthMethod(auth); err != nil {
		return nil, fmt.Errorf("mssql source: %w", err)
	}
	for _, col := range columns {
		if !validIdentifierPattern.MatchString(col) {
			return nil, fmt.Errorf("mssql source: invalid column name: %s", col)
		}
	}
	// TLS enforcement: Encrypt defaults to true; warn if certificate validation is disabled.
	if !auth.Encrypt {
		auth.Encrypt = true
	}
	if auth.TrustServerCertificate {
		slog.Warn("MSSQL trust_server_certificate is enabled; TLS certificate validation is disabled. This is insecure for production.", "server", auth.Server, "database", auth.Database)
	}
	driver := &mssqlDriver{auth: auth, columns: columns}
	src, err := newSQLSource(driver, table, query, timestampColumn, cursorColumn, sourceName, batchSize)
	if err != nil {
		return nil, err
	}
	if maxLookback != "" {
		d, err := time.ParseDuration(maxLookback)
		if err != nil {
			return nil, fmt.Errorf("mssql source: invalid max_lookback: %w", err)
		}
		src.maxLookback = d
	}
	if sourceTimezone != "" {
		loc, err := time.LoadLocation(sourceTimezone)
		if err != nil {
			return nil, fmt.Errorf("mssql source: invalid source_timezone: %w", err)
		}
		src.sourceLocation = loc
	}
	return &MSSQLSource{sqlSource: src}, nil
}

func validateAuthMethod(auth MSSQLAuth) error {
	switch auth.Method {
	case "sql":
		if auth.User == "" {
			return fmt.Errorf("sql auth: user is required")
		}
		if auth.Password == "" {
			return fmt.Errorf("sql auth: password is required")
		}
	case "ad-password":
		if auth.User == "" {
			return fmt.Errorf("ad-password auth: user is required")
		}
		if auth.Password == "" {
			return fmt.Errorf("ad-password auth: password is required")
		}
		if auth.ClientID == "" {
			return fmt.Errorf("ad-password auth: client_id (the application the user signs in through) is required")
		}
	case "ad-managed-identity":
	case "ad-service-principal":
		if auth.TenantID == "" {
			return fmt.Errorf("ad-service-principal auth: tenant_id is required")
		}
		if auth.ClientID == "" {
			return fmt.Errorf("ad-service-principal auth: client_id is required")
		}
		if auth.ClientSec == "" {
			return fmt.Errorf("ad-service-principal auth: client_secret is required")
		}
	case "":
		return fmt.Errorf("auth method is required")
	default:
		return fmt.Errorf("unsupported auth method: %s", auth.Method)
	}
	return nil
}
