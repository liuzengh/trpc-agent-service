package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestRunCommandRoutesMigrateWithoutStartingService(t *testing.T) {
	var gotEnvironment environment
	err := runCommand([]string{"migrate"}, mapEnvironment(map[string]string{
		"DATABASE_URL": "postgres://example",
	}), func() error {
		t.Fatal("serve must not run for the migrate command")
		return nil
	}, func(_ context.Context, getenv environment) error {
		gotEnvironment = getenv
		return nil
	})
	if err != nil {
		t.Fatalf("runCommand() error = %v", err)
	}
	if gotEnvironment == nil || gotEnvironment("DATABASE_URL") != "postgres://example" {
		t.Fatal("migrate command did not receive the process environment")
	}
}

func TestRunCommandRoutesServe(t *testing.T) {
	called := false
	err := runCommand([]string{"serve"}, mapEnvironment(nil), func() error {
		called = true
		return nil
	}, func(context.Context, environment) error {
		t.Fatal("migrate must not run for the serve command")
		return nil
	})
	if err != nil || !called {
		t.Fatalf("runCommand() = %v, serve called = %t", err, called)
	}
}

func TestRunCommandRejectsMissingCommand(t *testing.T) {
	err := runCommand(nil, mapEnvironment(nil), func() error { return nil }, func(context.Context, environment) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "command is required") {
		t.Fatalf("runCommand() error = %v, want missing command rejected", err)
	}
}

func TestRunCommandRejectsUnknownCommand(t *testing.T) {
	err := runCommand([]string{"unknown"}, mapEnvironment(nil), func() error {
		return nil
	}, func(context.Context, environment) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("runCommand() error = %v, want unknown command", err)
	}
}

func TestRunMigrationsAppliesCurrentBaseline(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	if err := runMigrations(context.Background(), mapEnvironment(map[string]string{"DATABASE_URL": dsn})); err != nil {
		t.Fatalf("runMigrations() error = %v", err)
	}
}
