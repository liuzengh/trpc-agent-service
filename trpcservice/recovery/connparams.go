package recovery

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// connParams holds the non-secret parts of one operator DSN plus the
// password kept only in memory until it is written to a 0600 PGPASSFILE.
// The password and the DSN itself never appear in argv, logs, manifests or
// errors.
type connParams struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
	SSLMode  string
}

// parseDSN parses postgres:// or postgresql:// URLs. Any other scheme or a
// keyword/value DSN is rejected: the recovery tool accepts the same URL form
// the deployment already uses for DATABASE_URL.
func parseDSN(raw string) (connParams, error) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil {
		return connParams{}, fmt.Errorf("%w: dsn parse failed", ErrInvalidConfig)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return connParams{}, fmt.Errorf("%w: dsn scheme unsupported", ErrInvalidConfig)
	}
	params := connParams{Host: parsed.Hostname(), Port: parsed.Port()}
	if params.Port == "" {
		params.Port = "5432"
	}
	if parsed.User != nil {
		params.User = parsed.User.Username()
		params.Password, _ = parsed.User.Password()
	}
	params.Database = strings.TrimPrefix(parsed.Path, "/")
	if params.Host == "" || params.User == "" || params.Database == "" {
		return connParams{}, fmt.Errorf("%w: dsn missing host, user or database", ErrInvalidConfig)
	}
	for key, values := range parsed.Query() {
		if len(values) == 0 {
			continue
		}
		switch strings.ToLower(key) {
		case "sslmode":
			params.SSLMode = values[0]
		default:
			// Unknown query parameters are rejected instead of forwarded so a
			// caller cannot smuggle libpq options through the DSN.
			return connParams{}, fmt.Errorf("%w: dsn query parameter not accepted", ErrInvalidConfig)
		}
	}
	if params.SSLMode == "" {
		params.SSLMode = "prefer"
	}
	return params, nil
}

// writePGPassFile materialises the connection password as a 0600 PGPASSFILE
// inside a fresh 0700 temporary directory. The returned cleanup removes both.
// When the DSN carries no password (trust/peer auth) no file is created.
func writePGPassFile(params connParams) (env []string, cleanup func(), err error) {
	cleanup = func() {}
	if params.Password == "" {
		return nil, cleanup, nil
	}
	dir, err := os.MkdirTemp("", "p202-pgpass-")
	if err != nil {
		return nil, cleanup, fmt.Errorf("%w: pgpass temp dir", ErrInvalidConfig)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return nil, cleanup, fmt.Errorf("%w: pgpass temp dir mode", ErrInvalidConfig)
	}
	file := filepath.Join(dir, "pgpass")
	line := fmt.Sprintf("%s:%s:%s:%s:%s\n",
		escapePGPass(params.Host), escapePGPass(params.Port),
		escapePGPass(params.Database), escapePGPass(params.User),
		escapePGPass(params.Password))
	if err := os.WriteFile(file, []byte(line), 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return nil, cleanup, fmt.Errorf("%w: pgpass write", ErrInvalidConfig)
	}
	env = append(env, "PGPASSFILE="+file)
	cleanup = func() { _ = os.RemoveAll(dir) }
	return env, cleanup, nil
}

func escapePGPass(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `:`, `\:`)
}

// libpqEnv builds the child-process connection environment from parsed
// parameters: coordinates only, password exclusively via PGPASSFILE.
func libpqEnv(params connParams, extra []string) []string {
	env := []string{
		"PGHOST=" + params.Host,
		"PGPORT=" + params.Port,
		"PGUSER=" + params.User,
		"PGDATABASE=" + params.Database,
		"PGSSLMODE=" + params.SSLMode,
	}
	return append(env, extra...)
}
