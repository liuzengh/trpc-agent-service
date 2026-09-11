package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresDatabaseIdentity(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://fixture-role:fixture-credential@db.internal:5432/agents?search_path=runtime")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	store := &Postgres{pool: pool}
	identity := store.DatabaseIdentity()
	if identity == "" {
		t.Fatal("database identity is empty")
	}
	for _, forbidden := range []string{"fixture-role", "fixture-credential", "db.internal", "agents", "runtime"} {
		if strings.Contains(identity, forbidden) {
			t.Fatalf("database identity exposed configuration value %q", forbidden)
		}
	}
	if (*Postgres)(nil).DatabaseIdentity() != "" {
		t.Fatal("nil Postgres returned a database identity")
	}
}

func TestDurationIntervalRoundsAwayFromZero(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		duration     time.Duration
		microseconds int64
	}{
		{name: "zero", duration: 0, microseconds: 0},
		{name: "positive sub-microsecond", duration: time.Nanosecond, microseconds: 1},
		{name: "positive exact", duration: time.Microsecond, microseconds: 1},
		{name: "positive remainder", duration: time.Microsecond + time.Nanosecond, microseconds: 2},
		{name: "negative sub-microsecond", duration: -time.Nanosecond, microseconds: -1},
		{name: "negative exact", duration: -time.Microsecond, microseconds: -1},
		{name: "negative remainder", duration: -time.Microsecond - time.Nanosecond, microseconds: -2},
		{name: "maximum", duration: time.Duration(1<<63 - 1), microseconds: 9_223_372_036_854_776},
		{name: "minimum", duration: time.Duration(-1 << 63), microseconds: -9_223_372_036_854_776},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := durationInterval(tt.duration)
			if !got.Valid {
				t.Fatal("durationInterval returned an invalid interval")
			}
			if got.Microseconds != tt.microseconds || got.Days != 0 || got.Months != 0 {
				t.Fatalf("durationInterval(%s) = %+v, want %d microseconds", tt.duration, got, tt.microseconds)
			}
		})
	}
}
