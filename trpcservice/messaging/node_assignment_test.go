package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
)

func TestAssignedSubmitAndNodeWaitLifecycle(t *testing.T) {
	store, redisServer := newTestStore(t)
	task := testTask("task-node", "message-node")
	now := time.Now().UTC()
	assignment := control.NodeAssignment{
		InboxID: task.InboxID(), TenantID: task.TenantID, AgentAppID: task.AgentAppID,
		PayloadDigest: task.PayloadDigest, NodeID: "worker-a", Mode: control.PlacementShared,
		State: control.AssignmentPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	snapshot, created, err := store.SubmitAssigned(context.Background(), task, assignment)
	if err != nil || !created {
		t.Fatalf("assigned submit = (%#v, %t, %v)", snapshot, created, err)
	}
	if snapshot.NodeID != "worker-a" || snapshot.AssignmentRevision != 1 || snapshot.AssignmentState != string(control.AssignmentPlanned) || snapshot.AssignmentPayloadDigest != task.PayloadDigest {
		t.Fatalf("assignment projection = %#v", snapshot)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeferNode(context.Background(), delivery, "worker-a", now.Add(time.Second), "node_mismatch"); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != "node_wait" {
		t.Fatalf("node wait snapshot = (%#v, %v)", snapshot, err)
	}
	redisServer.FastForward(2 * time.Second)
	redisServer.SetTime(now.Add(2 * time.Second))
	promoted, err := store.PromoteNodeWait(context.Background(), "worker-b", 10)
	if err != nil || promoted != 0 {
		t.Fatalf("wrong node promotion = (%d, %v)", promoted, err)
	}
	promoted, err = store.PromoteNodeWait(context.Background(), "worker-a", 10)
	if err != nil || promoted != 1 {
		t.Fatalf("target node promotion = (%d, %v)", promoted, err)
	}
	delivery, err = store.ReadTask(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdmitNodeAssignment(context.Background(), delivery, "worker-a"); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.AssignmentState != string(control.AssignmentAdmitted) {
		t.Fatalf("admitted snapshot = (%#v, %v)", snapshot, err)
	}
}

func TestNodeAssignmentUpdateAndBackfill(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("task-node-update", "message-node-update")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	assignment := control.NodeAssignment{InboxID: task.InboxID(), TenantID: task.TenantID, AgentAppID: task.AgentAppID, PayloadDigest: task.PayloadDigest, NodeID: "worker-a", Mode: control.PlacementShared, State: control.AssignmentPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now}
	if err := store.BackfillSharedAssignment(context.Background(), delivery, assignment); err != nil {
		t.Fatal(err)
	}
	if err := store.BackfillSharedAssignment(context.Background(), delivery, assignment); err != nil {
		t.Fatal(err)
	}
	assignment.State = control.AssignmentBlocked
	assignment.BlockedReason = "dedicated_node_offline"
	assignment.Revision = 2
	if err := store.UpdateNodeAssignment(context.Background(), task.InboxID(), task.TaskID, assignment, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateNodeAssignment(context.Background(), task.InboxID(), task.TaskID, assignment, 1); !errors.Is(err, control.ErrRevisionConflict) {
		t.Fatalf("stale node assignment update = %v", err)
	}
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.AssignmentState != string(control.AssignmentBlocked) || snapshot.NodeID != "worker-a" {
		t.Fatalf("updated projection = (%#v, %v)", snapshot, err)
	}
}

func TestNodeAssignmentRunningCanBeReassignedAfterLeaseExpiry(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("task-node-reassign", "message-node-reassign")
	now := time.Now().UTC()
	assignment := control.NodeAssignment{InboxID: task.InboxID(), TenantID: task.TenantID, AgentAppID: task.AgentAppID, PayloadDigest: task.PayloadDigest, NodeID: "worker-a", Mode: control.PlacementShared, State: control.AssignmentPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now}
	if _, _, err := store.SubmitAssigned(context.Background(), task, assignment); err != nil {
		t.Fatal(err)
	}
	if err := store.client.HSet(context.Background(), store.inboxKey(task.InboxID()), "assignment_state", string(control.AssignmentRunning), "state", StateQueued).Err(); err != nil {
		t.Fatal(err)
	}
	next := assignment
	next.NodeID = "worker-b"
	next.State = control.AssignmentPlanned
	next.Revision = 2
	if err := store.UpdateNodeAssignment(context.Background(), task.InboxID(), task.TaskID, next, 1); err != nil {
		t.Fatalf("expired running assignment should be reassignable: %v", err)
	}
	if err := store.client.HSet(context.Background(), store.inboxKey(task.InboxID()), "assignment_state", string(control.AssignmentRunning), "state", StateProcessing, "lease_until", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	next.Revision = 3
	if err := store.UpdateNodeAssignment(context.Background(), task.InboxID(), task.TaskID, next, 2); !errors.Is(err, control.ErrAssignmentNotOverridable) {
		t.Fatalf("live running assignment update = %v", err)
	}
}
