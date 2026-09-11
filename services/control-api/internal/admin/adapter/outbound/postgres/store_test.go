package postgresadapter_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	postgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/domain"
)

func TestStoreGrantsOperator(t *testing.T) {
	db := &dbStub{}
	store := postgres.NewStore(db)
	now := time.Date(2026, time.August, 31, 16, 0, 0, 0, time.UTC)
	err := store.GrantOperator(context.Background(), domain.OperatorGrant{
		UserID: "user-2", GrantedByActorType: domain.GrantedByUser,
		GrantedByUserID: "operator-1", GrantedAt: now,
	})
	if err != nil {
		t.Fatalf("GrantOperator() error = %v", err)
	}
	if db.execCalls != 1 || db.execArgs[0] != "user-2" || db.execArgs[2] != "operator-1" {
		t.Fatalf("calls/args = %d/%#v", db.execCalls, db.execArgs)
	}
}

func TestStoreRejectsRevokingLastOperator(t *testing.T) {
	db := &dbStub{rows: []pgx.Row{rowStub{values: []any{1, true, 0}}}}
	store := postgres.NewStore(db)
	err := store.RevokeOperator(context.Background(), "user-1", "user-1", time.Now())
	if err != application.ErrLastOperator {
		t.Fatalf("RevokeOperator() error = %v", err)
	}
}

type dbStub struct {
	rows      []pgx.Row
	rowIndex  int
	execCalls int
	execArgs  []any
}

func (s *dbStub) QueryRow(context.Context, string, ...any) pgx.Row {
	row := s.rows[s.rowIndex]
	s.rowIndex++
	return row
}

func (s *dbStub) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	s.execCalls++
	s.execArgs = append([]any(nil), args...)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

type rowStub struct{ values []any }

func (row rowStub) Scan(dest ...any) error {
	for index, value := range row.values {
		switch target := dest[index].(type) {
		case *int:
			*target = value.(int)
		case *bool:
			*target = value.(bool)
		default:
			panic("unsupported scan destination")
		}
	}
	return nil
}
