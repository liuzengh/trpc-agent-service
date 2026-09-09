// Command replay performs an audited, explicit DLQ replay.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
)

func main() {
	queue, id, actor, reason, restart, err := parseArgs(os.Args[1:])
	if err != nil {
		log.Fatalf("replay arguments invalid: error=%s", telemetry.StableErrorCode(err))
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	if err := storage.ValidateServicePostgresURL(dsn, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		log.Fatal("invalid DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := reliable.OpenPostgresStore(ctx, dsn)
	if err != nil {
		log.Fatalf("open replay store failed: error=%s", telemetry.StableErrorCode(err))
	}
	defer store.Close()
	switch queue {
	case "inbox":
		err = store.ReplayInbox(ctx, id, actor, reason)
	case "outbox":
		if restart {
			err = store.RestartOutbox(ctx, id, actor, reason)
		} else {
			err = store.ReplayOutbox(ctx, id, actor, reason)
		}
	default:
		log.Fatal("queue must be inbox or outbox")
	}
	if err != nil {
		log.Fatalf("replay failed: error=%s", telemetry.StableErrorCode(err))
	}
	mode := replayMode(queue, restart)
	fmt.Printf("replayed %s message %d (%s)\n", queue, id, mode)
}

func replayMode(queue string, restart bool) reliable.OutboxReplayMode {
	if queue == "inbox" || restart {
		return reliable.OutboxReplayRestart
	}
	return reliable.OutboxReplayResume
}

func parseArgs(args []string) (queue string, id int64, actor, reason string, restart bool, err error) {
	const usage = "usage: replay <inbox|outbox> <message-id> <actor> <reason> [--restart]"
	if len(args) != 4 && len(args) != 5 {
		return "", 0, "", "", false, errors.New(usage)
	}
	queue = args[0]
	if queue != "inbox" && queue != "outbox" {
		return "", 0, "", "", false, fmt.Errorf("%s: queue must be inbox or outbox", usage)
	}
	if id, err = strconv.ParseInt(args[1], 10, 64); err != nil || id <= 0 {
		return "", 0, "", "", false, fmt.Errorf("message-id must be a positive integer")
	}
	actor, reason = args[2], args[3]
	if actor == "" || reason == "" {
		return "", 0, "", "", false, fmt.Errorf("actor and reason are required")
	}
	if len(args) == 5 {
		if queue != "outbox" || args[4] != "--restart" {
			return "", 0, "", "", false, fmt.Errorf("%s: --restart is only valid for outbox", usage)
		}
		restart = true
	}
	return queue, id, actor, reason, restart, nil
}
