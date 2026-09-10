package postgres

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

func TestAgentPostgresBoundaryHelpers(t *testing.T) {
	if err := validateAgentMetadata(appmodel.ChangeMetadata{ActorType: "a", ActorID: "b", Reason: "c", CorrelationID: "d"}); err != nil {
		t.Fatal(err)
	}
	if err := validateAgentMetadata(appmodel.ChangeMetadata{ActorType: "a", ActorID: "b", Reason: strings.Repeat("x", 1001), CorrelationID: "d"}); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("long metadata error = %v", err)
	}
	if agentStatusEventType(appmodel.StatusSuspended) != appmodel.ChangeSuspended || agentStatusEventType(appmodel.StatusActive) != appmodel.ChangeResumed || agentStatusEventType(appmodel.StatusDisabled) != appmodel.ChangeDisabled {
		t.Fatal("status event mapping is incorrect")
	}
	value := int64(4)
	if clone := cloneAgentInt64(&value); clone == nil || clone == &value || *clone != value || cloneAgentInt64(nil) != nil {
		t.Fatal("integer clone is not defensive")
	}
	if got := *agentInt64(value); got != value || nullableInt64(sql.NullInt64{Int64: value, Valid: true}) == nil || nullableInt64(sql.NullInt64{}) != nil {
		t.Fatal("integer helpers are incorrect")
	}
	now := time.Now().UTC()
	if pointer := timePointer(now); pointer == nil || !pointer.Equal(now) || !maxTime(now, now.Add(time.Second)).Equal(now.Add(time.Second)) {
		t.Fatal("time helpers are incorrect")
	}
	if err := mutableAgentApp(&appmodel.App{Status: appmodel.StatusDisabled, Version: 1}, 1); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("disabled app error = %v", err)
	}
	if err := mutableAgentApp(&appmodel.App{Status: appmodel.StatusActive, Version: 2}, 1); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("stale app error = %v", err)
	}
}
