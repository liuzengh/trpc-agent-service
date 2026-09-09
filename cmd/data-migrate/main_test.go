package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
)

func TestMigrationCommandRejectsIncompleteOrUnsupportedCreate(t *testing.T) {
	valid := []string{"create", "--id", "move-1", "--tenant", "tenant-1", "--domain", "session", "--source-profile", "old", "--target-profile", "new", "--source-backend", "redis", "--target-backend", "postgres", "--config-version", "7", "--actor", "operator", "--reason", "storage replacement"}
	if _, err := parseCommand(valid, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ flag, value string }{
		{"--actor", ""}, {"--reason", "\n"}, {"--config-version", "0"},
		{"--domain", "memory"}, {"--target-backend", "qdrant"}, {"--target-profile", "old"},
		{"--batch-size", "0"}, {"--batch-size", "1001"}, {"--timeout", "0s"}, {"--interval", "0s"},
	} {
		t.Run(test.flag+test.value, func(t *testing.T) {
			args := append(append([]string(nil), valid...), test.flag, test.value)
			if _, err := parseCommand(args, io.Discard); err == nil {
				t.Fatal("invalid operator command accepted")
			}
		})
	}
}

func TestAbortRequiresAuditIdentity(t *testing.T) {
	if _, err := parseCommand([]string{"abort", "--id", "move-1"}, io.Discard); err == nil {
		t.Fatal("abort accepted without operator identity and reason")
	}
	if _, err := parseCommand([]string{"abort", "--id", "move-1", "--actor", "operator", "--reason", "destination unavailable"}, io.Discard); err != nil {
		t.Fatal(err)
	}
}

type commandController struct {
	migrationController
	job   datamigration.Job
	steps int
}

func (c *commandController) Get(context.Context, string) (datamigration.Job, error) {
	return c.job, nil
}

func (c *commandController) RunOnce(context.Context, string) (datamigration.Job, error) {
	c.steps++
	c.job.Phase = datamigration.PhaseRollbackWindow
	return c.job, nil
}

func TestRunStopsAtRollbackWindowWithoutFinalizing(t *testing.T) {
	c := &commandController{job: datamigration.Job{ID: "m", Phase: datamigration.PhaseCutover}}
	var output bytes.Buffer
	if err := runSteps(context.Background(), c, commandOptions{id: "m", interval: time.Nanosecond}, &output); err != nil {
		t.Fatal(err)
	}
	if c.steps != 1 || c.job.Phase != datamigration.PhaseRollbackWindow {
		t.Fatal("run must preserve the rollback window")
	}
}

func TestRunLeavesPausedMigrationUntouched(t *testing.T) {
	c := &commandController{job: datamigration.Job{ID: "m", Phase: datamigration.PhaseCatchUp, Paused: true}}
	if err := runSteps(context.Background(), c, commandOptions{id: "m"}, io.Discard); err != nil || c.steps != 0 {
		t.Fatalf("paused job advanced: steps=%d error=%v", c.steps, err)
	}
}

func TestRunCancellationInterruptsPollWait(t *testing.T) {
	c := &commandController{job: datamigration.Job{ID: "m", Phase: datamigration.PhaseCatchUp}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runSteps(ctx, c, commandOptions{id: "m", interval: time.Hour}, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
