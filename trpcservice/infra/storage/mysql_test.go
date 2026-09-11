package storage

import (
	"database/sql"
	"testing"
)

// TestApplyPoolSettingsCapsConnections guards the production connection budget:
// database/sql defaults to unlimited open connections, so a traffic spike (or a
// leak in a caller that forgets to Close a Rows) can exhaust MySQL's
// max_connections and take down every tenant at once.
func TestApplyPoolSettingsCapsConnections(t *testing.T) {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:1)/db")
	if err != nil {
		t.Fatalf("open stub db: %v", err)
	}
	defer func() { _ = db.Close() }()

	applyPoolSettings(db)

	stats := db.Stats()
	if stats.MaxOpenConnections != defaultMaxOpenConns {
		t.Errorf("MaxOpenConnections = %d, want %d", stats.MaxOpenConnections, defaultMaxOpenConns)
	}
	if got := db.Stats().MaxOpenConnections; got <= 0 {
		t.Errorf("MaxOpenConnections = %d, want a positive cap", got)
	}
	if defaultMaxIdleConns > defaultMaxOpenConns {
		t.Errorf("idle cap %d exceeds open cap %d, which can never be reached",
			defaultMaxIdleConns, defaultMaxOpenConns)
	}
}
