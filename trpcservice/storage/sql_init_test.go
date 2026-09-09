package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type stubSQLInitBackend struct {
	readyErrors []error
	readyCalls  int
	closeCalls  int
}

func (b *stubSQLInitBackend) Ready(context.Context) error {
	index := b.readyCalls
	b.readyCalls++
	if index < len(b.readyErrors) {
		return b.readyErrors[index]
	}
	return nil
}

func (b *stubSQLInitBackend) Close() error {
	b.closeCalls++
	return nil
}

func TestInitializeSQLProfilesReadyOnlyAndRetry(t *testing.T) {
	profiles := []tenant.StorageProfile{
		{TenantID: "tenant-pg", ID: "pg", Kind: tenant.StorageKindPostgres, CredentialRef: "env:PG"},
		{TenantID: "tenant-mysql", ID: "mysql", Kind: tenant.StorageKindMySQL, CredentialRef: "env:MYSQL"},
		{TenantID: "tenant-redis", ID: "redis", Kind: tenant.StorageKindRedis, CredentialRef: "env:REDIS"},
	}
	resolver := config.NewStaticCredentialResolver(map[string]string{"env:PG": "pg-dsn", "env:MYSQL": "mysql-dsn"})
	backends := []*stubSQLInitBackend{{readyErrors: []error{persistence.ErrBackendUnavailable}}, {}}
	created := 0
	results, err := initializeSQLProfiles(context.Background(), profiles, resolver, "all", true, func(profile tenant.StorageProfile, credential string, limits sessionfence.Limits) (sqlInitBackend, error) {
		if !profile.SkipDBInit {
			t.Fatal("sql-ready did not force SkipDBInit")
		}
		if credential == "" || limits.MaxTurnEvents != config.DefaultMaxTurnEvents || limits.MaxTurnBytes != config.DefaultMaxTurnBytes {
			t.Fatalf("factory arguments = credential:%q limits:%#v", credential, limits)
		}
		backend := backends[created]
		created++
		return backend, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || created != 2 || backends[0].readyCalls != 2 || backends[1].readyCalls != 1 || backends[0].closeCalls != 1 || backends[1].closeCalls != 1 {
		t.Fatalf("results=%#v created=%d backends=%#v", results, created, backends)
	}
}

func TestInitializeSQLProfilesRejectsInvalidOrMissingKind(t *testing.T) {
	resolver := config.NewStaticCredentialResolver(nil)
	factory := func(tenant.StorageProfile, string, sessionfence.Limits) (sqlInitBackend, error) {
		return nil, errors.New("unexpected factory call")
	}
	if _, err := initializeSQLProfiles(context.Background(), nil, resolver, "sqlite", false, factory); err == nil {
		t.Fatal("invalid kind unexpectedly passed")
	}
	if _, err := initializeSQLProfiles(context.Background(), nil, resolver, "postgres", false, factory); err == nil {
		t.Fatal("missing profile unexpectedly passed")
	}
}
