package storage

import (
	"database/sql"
	"fmt"

	gosql "github.com/go-sql-driver/mysql"
)

// OpenMySQL opens the platform MySQL connection with the parameters the
// stores rely on:
//   - parseTime=true: DATETIME columns scan into time.Time;
//   - clientFoundRows=true: RowsAffected reports matched rows, so an UPDATE
//     that writes identical values is not misread as "row missing".
//
// Extra params already present in the DSN win over these defaults.
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
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("storage: open mysql: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage: ping mysql: %w", err)
	}
	return db, nil
}
