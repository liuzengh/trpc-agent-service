package postgresadapter_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

func TestStoreProvisionsTenantAndOwnerWithOneStatement(t *testing.T) {
	db := &dbStub{}
	store := postgresadapter.NewStore(db)
	now := time.Date(2026, time.August, 31, 14, 0, 0, 0, time.UTC)

	err := store.ProvisionTenant(context.Background(), domain.Tenant{
		ID: "tenant-1", Slug: "team-a", Name: "Team A", Status: domain.TenantStatusActive,
		CreatedAt: now, UpdatedAt: now,
	}, domain.Membership{
		ID: "membership-1", TenantID: "tenant-1", UserID: "user-1",
		Role: domain.MembershipRoleOwner, CreatedBy: "operator-1", CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("ProvisionTenant() error = %v", err)
	}
	if db.execCalls != 1 || db.execArgs[0] != "tenant-1" || db.execArgs[6] != "membership-1" {
		t.Fatalf("exec calls/args = %d/%#v", db.execCalls, db.execArgs)
	}
}

func TestStoreReadsTenantMembership(t *testing.T) {
	now := time.Date(2026, time.August, 31, 14, 0, 0, 0, time.UTC)
	db := &dbStub{row: rowStub{values: []any{
		"tenant-1", "team-a", "Team A", "ACTIVE", now, now,
		"membership-1", "user-1", "OWNER", "operator-1", now,
	}}}
	store := postgresadapter.NewStore(db)

	state, err := store.GetTenantMembership(context.Background(), "tenant-1", "user-1")
	if err != nil {
		t.Fatalf("GetTenantMembership() error = %v", err)
	}
	if state.Tenant.ID != "tenant-1" || state.Membership.Role != domain.MembershipRoleOwner {
		t.Fatalf("state = %#v", state)
	}
}

type dbStub struct {
	row       pgx.Row
	query     string
	queryArgs []any
	execCalls int
	execArgs  []any
}

func (s *dbStub) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	s.query = query
	s.queryArgs = append([]any(nil), args...)
	return s.row
}

func (s *dbStub) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	s.execCalls++
	s.execArgs = append([]any(nil), args...)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

type rowStub struct{ values []any }

func (r rowStub) Scan(dest ...any) error {
	for index, value := range r.values {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *time.Time:
			*target = value.(time.Time)
		case *[]byte:
			*target = value.([]byte)
		case *int:
			*target = value.(int)
		default:
			panic("unsupported scan destination")
		}
	}
	return nil
}
