// Package scheduler implements control-plane node assignment and worker
// admission without exposing control Redis operations to business code.
package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

var (
	ErrNoEligibleNode       = errors.New("no eligible node")
	ErrDedicatedNodeOffline = errors.New("dedicated node is offline")
	ErrPlacementUnavailable = errors.New("tenant placement is unavailable")
)

type Service struct {
	repository control.Repository
	store      *messaging.Store
	enabled    bool
	reconciler *Reconciler
}

func New(repository control.Repository, store *messaging.Store, enabled bool) (*Service, error) {
	if repository == nil || store == nil {
		return nil, errors.New("scheduler repository and messaging store are required")
	}
	return &Service{repository: repository, store: store, enabled: enabled, reconciler: NewReconciler(repository, store)}, nil
}

func (s *Service) Enabled() bool { return s != nil && s.enabled }

func (s *Service) Submit(ctx context.Context, task message.ExecutionTask) (messaging.Snapshot, bool, error) {
	ctx, span := telemetry.Start(ctx, "scheduler.assign")
	defer span.End()
	if !s.enabled {
		return s.store.Submit(ctx, task)
	}
	if err := task.Validate(); err != nil {
		return messaging.Snapshot{}, false, err
	}
	existing, err := s.store.Snapshot(ctx, task.InboxID())
	if err == nil {
		if !message.ConstantTimeDigestEqual(existing.Digest, task.PayloadDigest) {
			storedTask, storedErr := existing.StoredTask()
			if storedErr != nil || !message.ConstantTimeDigestEqual(storedTask.BusinessDigest(), task.BusinessDigest()) {
				return messaging.Snapshot{}, false, messaging.ErrConflict
			}
		}
		return existing, false, nil
	}
	if !errors.Is(err, messaging.ErrInboxMissing) {
		return messaging.Snapshot{}, false, err
	}
	if err := s.repository.Ready(ctx); err != nil {
		return messaging.Snapshot{}, false, err
	}
	assignment, err := s.repository.GetAssignment(ctx, task.InboxID())
	if err == nil {
		if !message.ConstantTimeDigestEqual(assignment.PayloadDigest, task.PayloadDigest) {
			stored, storedErr := s.store.Snapshot(ctx, task.InboxID())
			if storedErr != nil {
				return messaging.Snapshot{}, false, storedErr
			}
			storedTask, storedErr := stored.StoredTask()
			if storedErr != nil || !message.ConstantTimeDigestEqual(storedTask.BusinessDigest(), task.BusinessDigest()) {
				return messaging.Snapshot{}, false, messaging.ErrConflict
			}
		}
		return s.store.SubmitAssigned(ctx, task, assignment)
	}
	if !errors.Is(err, control.ErrAssignmentNotFound) {
		return messaging.Snapshot{}, false, err
	}
	assignment, err = s.Assign(ctx, task)
	if err != nil {
		return messaging.Snapshot{}, false, err
	}
	created, wasCreated, err := s.repository.CreateAssignment(ctx, assignment)
	if err != nil {
		if errors.Is(err, control.ErrAssignmentConflict) {
			return messaging.Snapshot{}, false, messaging.ErrConflict
		}
		return messaging.Snapshot{}, false, err
	}
	if !wasCreated {
		assignment = created
	}
	return s.store.SubmitAssigned(ctx, task, assignment)
}

func (s *Service) Assign(ctx context.Context, task message.ExecutionTask) (control.NodeAssignment, error) {
	if err := task.Validate(); err != nil {
		return control.NodeAssignment{}, err
	}
	placement, err := s.repository.GetPlacement(ctx, task.TenantID)
	if err != nil {
		if errors.Is(err, control.ErrPlacementMissing) {
			return control.NodeAssignment{}, ErrPlacementUnavailable
		}
		return control.NodeAssignment{}, err
	}
	nodes, err := s.repository.ListNodes(ctx)
	if err != nil {
		return control.NodeAssignment{}, err
	}
	node, err := chooseNode(task.InboxID(), placement, nodes, time.Now().UTC())
	if err != nil {
		return control.NodeAssignment{}, err
	}
	now := time.Now().UTC()
	return control.NodeAssignment{
		InboxID: task.InboxID(), TenantID: task.TenantID, AgentAppID: task.AgentAppID,
		PayloadDigest: task.PayloadDigest, NodeID: node.NodeID, Mode: placement.Mode,
		State: control.AssignmentPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}, nil
}

func (s *Service) Override(ctx context.Context, inboxID, nodeID string) (control.NodeAssignment, error) {
	if !s.enabled {
		return control.NodeAssignment{}, errors.New("node assignment is disabled")
	}
	assignment, err := s.repository.GetAssignment(ctx, inboxID)
	if err != nil {
		return control.NodeAssignment{}, err
	}
	if assignment.State != control.AssignmentPlanned && assignment.State != control.AssignmentBlocked {
		return control.NodeAssignment{}, control.ErrAssignmentNotOverridable
	}
	node, err := s.repository.GetNode(ctx, nodeID)
	if err != nil && !errors.Is(err, control.ErrNodeConflict) {
		return control.NodeAssignment{}, err
	}
	placement, err := s.repository.GetPlacement(ctx, assignment.TenantID)
	if err != nil {
		return control.NodeAssignment{}, err
	}
	if placement.Mode == control.PlacementDedicated && placement.NodeID != nodeID {
		return control.NodeAssignment{}, control.ErrAssignmentNotOverridable
	}
	snapshot, err := s.store.Snapshot(ctx, inboxID)
	if err != nil {
		return control.NodeAssignment{}, err
	}
	next := assignment
	next.NodeID = nodeID
	next.Mode = placement.Mode
	next.Revision++
	next.BlockedReason = ""
	next.State = control.AssignmentPlanned
	if node.State == control.NodeOffline || !node.LeaseUntil.After(time.Now()) {
		next.State = control.AssignmentBlocked
		next.BlockedReason = "node_offline"
	}
	if err := s.store.UpdateNodeAssignment(ctx, inboxID, snapshot.TaskID, next, assignment.Revision); err != nil {
		return control.NodeAssignment{}, err
	}
	updated, err := s.repository.PutAssignment(ctx, next, assignment.Revision)
	if err != nil {
		return control.NodeAssignment{}, err
	}
	return updated, nil
}

func chooseNode(inboxID string, placement control.TenantPlacement, nodes []control.NodeRecord, now time.Time) (control.NodeRecord, error) {
	if placement.Mode == control.PlacementDedicated {
		for _, node := range nodes {
			if node.NodeID != placement.NodeID {
				continue
			}
			if eligible(node, now) {
				return node, nil
			}
			return control.NodeRecord{}, ErrDedicatedNodeOffline
		}
		return control.NodeRecord{}, ErrDedicatedNodeOffline
	}
	eligibleNodes := make([]control.NodeRecord, 0, len(nodes))
	for _, node := range nodes {
		if eligible(node, now) {
			eligibleNodes = append(eligibleNodes, node)
		}
	}
	if len(eligibleNodes) == 0 {
		return control.NodeRecord{}, ErrNoEligibleNode
	}
	sort.SliceStable(eligibleNodes, func(i, j int) bool {
		left, right := eligibleNodes[i], eligibleNodes[j]
		leftScore := int64(left.Inflight) * int64(right.Capacity)
		rightScore := int64(right.Inflight) * int64(left.Capacity)
		if leftScore != rightScore {
			return leftScore < rightScore
		}
		return rendezvousScore(inboxID, left.NodeID) > rendezvousScore(inboxID, right.NodeID)
	})
	return eligibleNodes[0], nil
}

func eligible(node control.NodeRecord, now time.Time) bool {
	return node.State == control.NodeReady && node.Inflight < node.Capacity && node.LeaseUntil.After(now) && node.LastHeartbeat.Add(30*time.Second).After(now)
}

func rendezvousScore(inboxID, nodeID string) uint64 {
	sum := sha256.Sum256([]byte(inboxID + "\x00" + nodeID))
	return uint64(sum[0])<<56 | uint64(sum[1])<<48 | uint64(sum[2])<<40 | uint64(sum[3])<<32 | uint64(sum[4])<<24 | uint64(sum[5])<<16 | uint64(sum[6])<<8 | uint64(sum[7])
}

func (s *Service) Ready(ctx context.Context) error {
	if s == nil || s.repository == nil {
		return nil
	}
	return s.repository.Ready(ctx)
}

func (s *Service) Start(ctx context.Context) error {
	if s == nil || s.reconciler == nil {
		return nil
	}
	return s.reconciler.Start(ctx)
}

func (s *Service) Close() error {
	if s == nil || s.reconciler == nil {
		return nil
	}
	return s.reconciler.Close()
}

func (s *Service) IsLeader() bool {
	return s != nil && s.reconciler != nil && s.reconciler.IsLeader()
}

// Reconciler owns the single Gateway-side control-plane lease. Assignment
// reconciliation is deliberately bounded to this loop so multiple Gateways
// never perform the same repair concurrently.
type Reconciler struct {
	repository control.Repository
	store      *messaging.Store
	leader     atomic.Bool
	scanCursor atomic.Uint64

	mu     sync.Mutex
	cancel context.CancelFunc
	loops  sync.WaitGroup
	owner  string
}

func NewReconciler(repository control.Repository, store *messaging.Store) *Reconciler {
	owner, err := identity.RequestID()
	if err != nil {
		owner = HexHash(fmt.Sprintf("%d", time.Now().UnixNano()))[:16]
	}
	return &Reconciler{repository: repository, store: store, owner: "reconciler-" + owner}
}

func (r *Reconciler) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return errors.New("reconciler is already running")
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.loops.Add(1)
	r.mu.Unlock()
	go func() {
		defer r.loops.Done()
		r.run(runCtx)
	}()
	return nil
}

func (r *Reconciler) run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	const leaseTTL = 5 * time.Second
	for {
		if err := r.repository.Ready(ctx); err != nil {
			r.leader.Store(false)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
		if !r.leader.Load() {
			acquired, err := r.repository.AcquireLease(ctx, "reconciler:leader", r.owner, leaseTTL)
			if err == nil && acquired {
				r.leader.Store(true)
			}
		} else {
			ok, err := r.repository.RenewLease(ctx, "reconciler:leader", r.owner, leaseTTL)
			if err != nil || !ok {
				r.leader.Store(false)
			} else {
				_ = r.ReconcileOnce(ctx, 64)
			}
		}
		select {
		case <-ctx.Done():
			if r.leader.Load() {
				_ = r.repository.ReleaseLease(context.Background(), "reconciler:leader", r.owner)
			}
			r.leader.Store(false)
			return
		case <-ticker.C:
		}
	}
}

func (r *Reconciler) ReconcileOnce(ctx context.Context, limit int) error {
	ctx, span := telemetry.Start(ctx, "scheduler.reconcile")
	defer span.End()
	if r == nil || r.repository == nil || r.store == nil {
		return nil
	}
	assignments, err := r.repository.ListAssignments(ctx)
	if err != nil {
		return err
	}
	assignments = r.nextAssignmentBatch(assignments, limit)
	nodes, err := r.repository.ListNodes(ctx)
	if err != nil {
		return err
	}
	for _, assignment := range assignments {
		if err := r.reconcileAssignment(ctx, assignment, nodes); err != nil && !errors.Is(err, control.ErrRevisionConflict) && !errors.Is(err, control.ErrAssignmentNotOverridable) {
			return err
		}
	}
	return nil
}

func (r *Reconciler) nextAssignmentBatch(assignments []control.NodeAssignment, limit int) []control.NodeAssignment {
	total := len(assignments)
	if total == 0 {
		return nil
	}
	if limit <= 0 || limit > total {
		limit = total
	}
	start := int((r.scanCursor.Add(uint64(limit)) - uint64(limit)) % uint64(total))
	batch := make([]control.NodeAssignment, limit)
	for i := range batch {
		batch[i] = assignments[(start+i)%total]
	}
	return batch
}

func (r *Reconciler) reconcileAssignment(ctx context.Context, assignment control.NodeAssignment, nodes []control.NodeRecord) error {
	if assignment.State == control.AssignmentAdmitted || assignment.State == control.AssignmentRunning {
		// Keep a live assignment pinned, but permit takeover after its node lease
		// and heartbeat expire so another eligible worker can admit the task.
		for _, node := range nodes {
			if node.NodeID == assignment.NodeID && eligible(node, time.Now().UTC()) {
				return nil
			}
		}
	}
	snapshot, err := r.store.Snapshot(ctx, assignment.InboxID)
	if err != nil {
		return err
	}
	if snapshot.Digest != assignment.PayloadDigest {
		return control.ErrAssignmentConflict
	}
	if snapshot.Terminal() {
		if assignment.State == control.AssignmentCompleted {
			return nil
		}
		assignment.State = control.AssignmentCompleted
		assignment.BlockedReason = ""
		_, err = r.repository.PutAssignment(ctx, assignment, assignment.Revision)
		return err
	}
	if snapshot.State == messaging.StateProcessing || snapshot.State == messaging.StatePersisting {
		// An in-flight task is left alone while its lease is valid. A killed
		// worker leaves the inbox in this state until the lease expires; then
		// normal placement must be allowed to hand it to another node.
		if snapshot.LeaseUntil > time.Now().UnixMilli() {
			return nil
		}
	}
	placement, err := r.repository.GetPlacement(ctx, assignment.TenantID)
	if err != nil {
		return err
	}
	node, chooseErr := chooseNode(assignment.InboxID, placement, nodes, time.Now().UTC())
	next := assignment
	if chooseErr != nil {
		if placement.Mode == control.PlacementDedicated && errors.Is(chooseErr, ErrDedicatedNodeOffline) {
			next.Mode = placement.Mode
			next.NodeID = placement.NodeID
			next.State = control.AssignmentBlocked
			next.BlockedReason = "dedicated_node_offline"
		} else {
			return chooseErr
		}
	} else {
		next.Mode = placement.Mode
		next.NodeID = node.NodeID
		next.State = control.AssignmentPlanned
		next.BlockedReason = ""
	}
	if next.NodeID == assignment.NodeID && next.State == assignment.State && next.Mode == assignment.Mode {
		return nil
	}
	next.Revision = assignment.Revision + 1
	// The control repository revision advances on admitted/running transitions,
	// while the Redis projection revision advances only when placement changes.
	// Use the projection's CAS revision here; the repository write below still
	// uses the control-plane revision.
	if err := r.store.UpdateNodeAssignment(ctx, assignment.InboxID, snapshot.TaskID, next, snapshot.AssignmentRevision); err != nil {
		return err
	}
	_, err = r.repository.PutAssignment(ctx, next, assignment.Revision)
	return err
}

func (r *Reconciler) IsLeader() bool { return r != nil && r.leader.Load() }

func (r *Reconciler) Close() error {
	r.mu.Lock()
	cancel := r.cancel
	r.cancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.loops.Wait()
	r.leader.Store(false)
	return nil
}

type Controller struct {
	repository control.Repository
	store      *messaging.Store
	lifecycle  *control.NodeLifecycle
	enabled    bool
}

func NewController(repository control.Repository, store *messaging.Store, cfg *config.ControlPlaneConfig, nodeID, build string) (*Controller, error) {
	if repository == nil || store == nil {
		return nil, errors.New("node controller repository and messaging store are required")
	}
	if cfg == nil {
		return &Controller{repository: repository, store: store}, nil
	}
	lifecycle, err := control.NewNodeLifecycle(repository, *cfg, nodeID, "worker", build, []string{"governance.v1", "trace_digest_v2", "node_assignment"})
	if err != nil {
		return nil, err
	}
	return &Controller{repository: repository, store: store, lifecycle: lifecycle, enabled: cfg.NodeAssignmentEnabled}, nil
}

func (c *Controller) Enabled() bool { return c != nil && c.enabled }

func (c *Controller) Start(ctx context.Context) error {
	if c.lifecycle == nil {
		return nil
	}
	return c.lifecycle.Start(ctx)
}

func (c *Controller) Ready(ctx context.Context) error {
	if c.lifecycle == nil {
		return nil
	}
	return c.lifecycle.Ready(ctx)
}

func (c *Controller) Admit(ctx context.Context, delivery messaging.Delivery) (bool, error) {
	ctx, span := telemetry.Start(ctx, "worker.admit")
	defer span.End()
	if !c.enabled {
		return true, nil
	}
	snapshot, err := c.store.Snapshot(ctx, delivery.InboxID)
	if err != nil {
		return c.rejectAdmission(ctx, delivery.Task.TenantID, "node_assignment_unavailable", err)
	}
	if snapshot.NodeID == "" {
		placement, placementErr := c.repository.GetPlacement(ctx, delivery.Task.TenantID)
		if placementErr != nil {
			return c.rejectAdmission(ctx, delivery.Task.TenantID, "tenant_placement_unavailable", fmt.Errorf("%w: %v", ErrPlacementUnavailable, placementErr))
		}
		nodes, nodesErr := c.repository.ListNodes(ctx)
		if nodesErr != nil {
			return c.rejectAdmission(ctx, delivery.Task.TenantID, "node_registry_unavailable", nodesErr)
		}
		node, chooseErr := chooseNode(delivery.InboxID, placement, nodes, time.Now().UTC())
		if chooseErr != nil {
			if placement.Mode != control.PlacementDedicated || !errors.Is(chooseErr, ErrDedicatedNodeOffline) {
				return c.rejectAdmission(ctx, delivery.Task.TenantID, "no_eligible_node", chooseErr)
			}
			now := time.Now().UTC()
			blocked := control.NodeAssignment{InboxID: delivery.InboxID, TenantID: delivery.Task.TenantID, AgentAppID: delivery.Task.AgentAppID, PayloadDigest: delivery.Task.PayloadDigest, NodeID: placement.NodeID, Mode: placement.Mode, State: control.AssignmentBlocked, BlockedReason: "dedicated_node_offline", Revision: 1, CreatedAt: now, UpdatedAt: now}
			if err := c.store.BackfillSharedAssignment(ctx, delivery, blocked); err != nil {
				return c.rejectAdmission(ctx, delivery.Task.TenantID, "node_assignment_unavailable", err)
			}
			_, _, _ = c.repository.CreateAssignment(ctx, blocked)
			_ = c.repository.SetTenantDegraded(ctx, delivery.Task.TenantID, "dedicated_node_offline")
			if err := c.store.DeferNode(ctx, delivery, placement.NodeID, time.Now().Add(250*time.Millisecond), "dedicated_node_offline"); err != nil {
				return c.rejectAdmission(ctx, delivery.Task.TenantID, "node_assignment_unavailable", err)
			}
			return false, nil
		}
		now := time.Now().UTC()
		assignment := control.NodeAssignment{InboxID: delivery.InboxID, TenantID: delivery.Task.TenantID, AgentAppID: delivery.Task.AgentAppID, PayloadDigest: delivery.Task.PayloadDigest, NodeID: node.NodeID, Mode: placement.Mode, State: control.AssignmentPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now}
		if err := c.store.BackfillSharedAssignment(ctx, delivery, assignment); err != nil {
			return c.rejectAdmission(ctx, delivery.Task.TenantID, "node_assignment_unavailable", err)
		}
		snapshot, err = c.store.Snapshot(ctx, delivery.InboxID)
		if err != nil {
			return c.rejectAdmission(ctx, delivery.Task.TenantID, "node_assignment_unavailable", err)
		}
	}
	if snapshot.AssignmentPayloadDigest != "" && snapshot.AssignmentPayloadDigest != delivery.Task.PayloadDigest {
		return c.rejectAdmission(ctx, delivery.Task.TenantID, "assignment_conflict", control.ErrAssignmentConflict)
	}
	if snapshot.NodeID != c.lifecycle.NodeID() {
		if err := c.store.DeferNode(ctx, delivery, snapshot.NodeID, time.Now().Add(250*time.Millisecond), "node_mismatch"); err != nil {
			return c.rejectAdmission(ctx, delivery.Task.TenantID, "node_assignment_unavailable", err)
		}
		return false, nil
	}
	if err := c.store.AdmitNodeAssignment(ctx, delivery, c.lifecycle.NodeID()); err != nil {
		return c.rejectAdmission(ctx, delivery.Task.TenantID, "node_assignment_unavailable", err)
	}
	if assignment, getErr := c.repository.GetAssignment(ctx, delivery.InboxID); getErr == nil && assignment.State == control.AssignmentPlanned {
		assignment.State = control.AssignmentAdmitted
		_, _ = c.repository.PutAssignment(ctx, assignment, assignment.Revision)
	}
	_ = c.repository.ClearTenantDegraded(ctx, delivery.Task.TenantID)
	telemetry.RecordAssignment(ctx, "admitted", "")
	telemetry.RecordDegraded(ctx, "false", "")
	return true, nil
}

func (c *Controller) rejectAdmission(ctx context.Context, tenantID, reason string, err error) (bool, error) {
	_ = c.repository.SetTenantDegraded(ctx, tenantID, reason)
	telemetry.RecordAssignment(ctx, "rejected", reason)
	telemetry.RecordDegraded(ctx, "true", reason)
	return false, err
}

func (c *Controller) MarkRunning(ctx context.Context, delivery messaging.Delivery) {
	if !c.enabled {
		return
	}
	_ = c.store.AdvanceNodeAssignment(ctx, delivery, c.NodeID(), control.AssignmentRunning)
	if assignment, err := c.repository.GetAssignment(ctx, delivery.InboxID); err == nil && assignment.State == control.AssignmentAdmitted {
		assignment.State = control.AssignmentRunning
		_, _ = c.repository.PutAssignment(ctx, assignment, assignment.Revision)
	}
	telemetry.RecordAssignment(ctx, "running", "")
}

func (c *Controller) MarkCompleted(ctx context.Context, delivery messaging.Delivery) {
	if !c.enabled {
		return
	}
	_ = c.store.AdvanceNodeAssignment(ctx, delivery, c.NodeID(), control.AssignmentCompleted)
	if assignment, err := c.repository.GetAssignment(ctx, delivery.InboxID); err == nil && assignment.State != control.AssignmentCompleted {
		assignment.State = control.AssignmentCompleted
		assignment.BlockedReason = ""
		_, _ = c.repository.PutAssignment(ctx, assignment, assignment.Revision)
	}
	telemetry.RecordAssignment(ctx, "completed", "")
}

func (c *Controller) Promote(ctx context.Context, limit int) (int, error) {
	if !c.enabled {
		return 0, nil
	}
	return c.store.PromoteNodeWait(ctx, c.lifecycle.NodeID(), limit)
}

func (c *Controller) SetInflight(value int) {
	telemetry.SetInflight(value)
	if c.lifecycle != nil {
		c.lifecycle.SetInflight(value)
	}
}

func (c *Controller) NodeID() string {
	if c.lifecycle == nil {
		return ""
	}
	return c.lifecycle.NodeID()
}

func (c *Controller) ConsumerName() string {
	if c.lifecycle == nil {
		return ""
	}
	return "worker-" + c.lifecycle.NodeID() + "-" + c.lifecycle.BootID()
}

func (c *Controller) MarkDraining(ctx context.Context) error {
	if c.lifecycle == nil {
		return nil
	}
	return c.lifecycle.MarkDraining(ctx)
}

func (c *Controller) Close() error {
	if c.lifecycle == nil {
		return nil
	}
	return c.lifecycle.Close()
}

func ParseNodeID(raw string) string { return strings.TrimSpace(raw) }

func HexHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
