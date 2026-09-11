package storage

import (
	"database/sql"
	"fmt"
	"time"

	gosql "github.com/go-sql-driver/mysql"
)

// Connection-pool bounds. database/sql defaults to unlimited open connections,
// so without a cap a burst of concurrent turns can exhaust MySQL's
// max_connections and take every tenant down together. The values are sized for
// a single node: 64 concurrent statements is far above the worker's useful
// parallelism (16 consume slots) while staying well inside a default MySQL
// server's 151-connection budget, leaving room for other clients.
const (
	defaultMaxOpenConns    = 64
	defaultMaxIdleConns    = 16
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
)

// applyPoolSettings bounds the connection pool of a platform MySQL handle.
func applyPoolSettings(db *sql.DB) {
	db.SetMaxOpenConns(defaultMaxOpenConns)
	db.SetMaxIdleConns(defaultMaxIdleConns)
	// Recycle connections so a server-side restart, a failover, or an idle
	// timeout in a middlebox cannot leave the pool full of dead sockets.
	db.SetConnMaxLifetime(defaultConnMaxLifetime)
	db.SetConnMaxIdleTime(defaultConnMaxIdleTime)
}

// OpenMySQL opens the platform MySQL connection with the parameters the
// stores rely on:
//   - parseTime=true: DATETIME columns scan into time.Time;
//   - clientFoundRows=true: RowsAffected reports matched rows, so an UPDATE
//     that writes identical values is not misread as "row missing";
//   - time_zone='+00:00': server-generated timestamps are UTC, matching the
//     driver, which parses and formats DATETIME in cfg.Loc (UTC by default).
//
// The time zone matters because every table mixes both sources: MySQL writes
// CURRENT_TIMESTAMP defaults while Go writes time.Time values. Left alone, a
// server whose session zone is the host's local time produces rows where the
// same instant reads eight hours apart depending on which side wrote it — the
// outbox dispatcher compared such a timestamp against its own clock and
// deferred every reply for the whole offset. Extra params already present in
// the DSN win over these defaults, and the zone is only pinned when the DSN
// keeps the driver's default UTC location (a caller who asked for loc=Local
// gets a session zone to match). The connection pool is bounded (see
// applyPoolSettings).
func OpenMySQL(dsn string) (*sql.DB, error) {
	cfg, err := gosql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: parse mysql dsn: %w", err)
	}
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	if _, ok := cfg.Params["parseTime"]; !ok {
		cfg.Params["parseTime"] = "true"
	}
	if _, ok := cfg.Params["clientFoundRows"]; !ok {
		cfg.Params["clientFoundRows"] = "true"
	}
	if cfg.Loc == time.UTC {
		if _, ok := cfg.Params["time_zone"]; !ok {
			// The quoted offset form needs no time-zone tables on the server.
			cfg.Params["time_zone"] = "'+00:00'"
		}
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("storage: open mysql: %w", err)
	}
	applyPoolSettings(db)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage: ping mysql: %w", err)
	}
	return db, nil
}
