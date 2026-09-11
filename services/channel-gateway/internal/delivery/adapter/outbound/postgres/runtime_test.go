package postgresadapter_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

func runtimePorts(t *testing.T, store *pg.Store) (app.DueAccountReader, app.MaintenanceStore) {
	t.Helper()
	due, ok := any(store).(app.DueAccountReader)
	if !ok {
		t.Fatal("PostgreSQL Store does not expose DueAccountReader")
	}
	maintenance, ok := any(store).(app.MaintenanceStore)
	if !ok {
		t.Fatal("PostgreSQL Store does not expose MaintenanceStore")
	}
	return due, maintenance
}

func TestRuntimePublicPortsValidateBeforeDatabase(t *testing.T) {
	due, maintenance := runtimePorts(t, &pg.Store{})
	ctx := context.Background()
	for _, query := range []app.DueAccountQuery{
		{Provider: "telegram", Limit: 0}, {Provider: "telegram", Limit: 1001},
		{Provider: "unknown", Limit: 1}, {Provider: "telegram", AfterAccountID: " bad", Limit: 1},
		{Provider: "wecom", AfterAccountID: strings.Repeat("a", 129), Limit: 1},
		{Provider: "telegram", AfterAccountID: "账户", Limit: 1},
	} {
		if _, err := due.ListDueAccounts(ctx, query); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("due query=%+v err=%v", query, err)
		}
	}
	for _, query := range []app.ObservedAttemptQuery{{Limit: 0}, {Limit: 1001}, {AfterAttemptID: "bad/id", Limit: 1}, {AfterAttemptID: strings.Repeat("a", 129), Limit: 1}} {
		if _, err := maintenance.ListResolvableObserved(ctx, query); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("observed query=%+v err=%v", query, err)
		}
	}
	for _, limit := range []int{0, -1, 1001} {
		if _, err := maintenance.ExpirePending(ctx, limit); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("expire limit=%d err=%v", limit, err)
		}
	}
	if _, err := due.ListDueAccounts(nil, app.DueAccountQuery{Provider: "telegram", Limit: 1}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("nil due context: %v", err)
	}
	if _, err := maintenance.ListResolvableObserved(nil, app.ObservedAttemptQuery{Limit: 1}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("nil observed context: %v", err)
	}
	if _, err := maintenance.ExpirePending(nil, 1); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("nil expire context: %v", err)
	}
}
