package platform

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// RunKey is the tenant-scoped idempotency identity for one chat execution.
type RunKey struct {
	TenantID  string
	SessionID string
	RequestID string
}

// RunTerminal is the only state a permit may commit after it owns a run.
type RunTerminal struct {
	Type  string
	Error string
}

type ClaimDisposition string

const (
	Claimed         ClaimDisposition = "claimed"
	ClaimQueued     ClaimDisposition = "queued"
	ClaimAlreadyRun ClaimDisposition = "already_running"
	ClaimExisting   ClaimDisposition = "existing_terminal"
)

type CancelDisposition string

const (
	CancelRequested CancelDisposition = "cancellation_requested"
	CancelExisting  CancelDisposition = "existing_terminal"
	CancelNotFound  CancelDisposition = "not_found"
)

var (
	ErrIdempotencyKeyReused = errors.New("idempotency_key_reused")
	ErrRunAlreadyRunning    = errors.New("already_running")
	ErrRunNotFound          = errors.New("run_not_found")
	ErrStaleRunOwner        = errors.New("stale_run_owner")
)

type RunPermit interface {
	FencingToken() uint64
	Lost() <-chan struct{}
	CancelRequested() <-chan struct{}
	Finish(context.Context, RunTerminal) error
	Release()
}

type RunCoordinator interface {
	Claim(context.Context, RunKey, string) (RunPermit, ClaimDisposition, error)
	RequestCancel(context.Context, RunKey) (CancelDisposition, error)
	Close() error
}

type runTerminalReconciler interface {
	ReconcileTerminal(context.Context, RunKey, RunTerminal) error
}

func validateRunKey(key RunKey) error {
	if strings.TrimSpace(key.TenantID) == "" || strings.TrimSpace(key.SessionID) == "" || strings.TrimSpace(key.RequestID) == "" {
		return errors.New("invalid_run_key")
	}
	return nil
}

func runInputHash(input string) string {
	input = strings.TrimSpace(input)
	sum := sha256.Sum256([]byte(input))
	return fmt.Sprintf("sha256:%x", sum[:])
}

// memoryRun is deliberately mutable only while the coordinator mutex is held.
type memoryRun struct {
	key          RunKey
	inputHash    string
	enqueueOrder uint64
	owner        string
	fence        uint64
	state        string
	leaseExpiry  time.Time
	cancel       chan struct{}
	ready        chan struct{}
	terminal     RunTerminal
	closed       bool
	permit       *memoryPermit
}

type InMemoryRunCoordinator struct {
	mu            sync.Mutex
	owner         string
	ttl           time.Duration
	renewInterval time.Duration
	sequence      uint64
	runs          map[RunKey]*memoryRun
	fences        map[string]uint64
	closed        bool
}

func NewInMemoryRunCoordinator(owner ...string) *InMemoryRunCoordinator {
	name := "gateway-local"
	if len(owner) > 0 && strings.TrimSpace(owner[0]) != "" {
		name = strings.TrimSpace(owner[0])
	}
	return &InMemoryRunCoordinator{
		owner: name, ttl: 30 * time.Second, renewInterval: 10 * time.Second,
		runs: make(map[RunKey]*memoryRun), fences: make(map[string]uint64),
	}
}

// NewTimedInMemoryRunCoordinator is useful for deterministic lease-loss tests.
func NewTimedInMemoryRunCoordinator(owner string, ttl, renewInterval time.Duration) (*InMemoryRunCoordinator, error) {
	if ttl <= 0 || renewInterval <= 0 || renewInterval >= ttl {
		return nil, errors.New("run coordinator timing is invalid")
	}
	coordinator := NewInMemoryRunCoordinator(owner)
	coordinator.ttl, coordinator.renewInterval = ttl, renewInterval
	return coordinator, nil
}

func (c *InMemoryRunCoordinator) Claim(ctx context.Context, key RunKey, input string) (RunPermit, ClaimDisposition, error) {
	if err := validateRunKey(key); err != nil {
		return nil, "", err
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	hash := runInputHash(input)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, "", errors.New("run_coordinator_closed")
	}
	if run := c.runs[key]; run != nil {
		if run.inputHash != hash {
			return nil, "", ErrIdempotencyKeyReused
		}
		if isRunTerminal(run.state) {
			return &memoryPermit{coordinator: c, run: run, terminal: true}, ClaimExisting, nil
		}
		if run.state == "running" && c.expiredLocked(run) {
			run.state = "outcome_unknown"
			run.terminal = RunTerminal{Type: "outcome_unknown", Error: "owner_lost"}
			closeOnce(run.permit.lost)
			return &memoryPermit{coordinator: c, run: run, terminal: true}, ClaimExisting, nil
		}
		if run.state == "running" {
			return nil, ClaimAlreadyRun, nil
		}
		if run.state == "queued" {
			c.promoteLocked(key.TenantID, key.SessionID)
			if run.state == "running" {
				return run.permit, Claimed, nil
			}
			return nil, ClaimAlreadyRun, nil
		}
	}
	c.sequence++
	run := &memoryRun{key: key, inputHash: hash, enqueueOrder: c.sequence, state: "queued", ready: make(chan struct{}), cancel: make(chan struct{})}
	run.permit = &memoryPermit{coordinator: c, run: run, lost: make(chan struct{})}
	c.runs[key] = run
	c.promoteLocked(key.TenantID, key.SessionID)
	if run.state == "running" {
		return run.permit, Claimed, nil
	}
	return run.permit, ClaimQueued, nil
}

func (c *InMemoryRunCoordinator) expiredLocked(run *memoryRun) bool {
	return run.state == "running" && !run.leaseExpiry.IsZero() && !time.Now().UTC().Before(run.leaseExpiry)
}

func (c *InMemoryRunCoordinator) takeoverLocked(run *memoryRun) {
	previous := run.permit
	if run.owner != "" {
		closeOnce(previous.lost)
	}
	run.owner, run.state = c.owner, "running"
	sessionKey := run.key.TenantID + "\x00" + run.key.SessionID
	c.fences[sessionKey]++
	run.fence = c.fences[sessionKey]
	previous.mu.Lock()
	previousFence := previous.fence
	previous.mu.Unlock()
	if previousFence != 0 {
		run.permit = &memoryPermit{coordinator: c, run: run, fence: run.fence, lost: make(chan struct{})}
	} else {
		previous.mu.Lock()
		previous.fence = run.fence
		previous.mu.Unlock()
	}
	run.leaseExpiry = time.Now().UTC().Add(c.ttl)
	select {
	case <-run.ready:
	default:
		close(run.ready)
	}
	go run.permit.renew()
}

func (c *InMemoryRunCoordinator) promoteLocked(tenantID, sessionID string) {
	var running *memoryRun
	var next *memoryRun
	for _, run := range c.runs {
		if run.key.TenantID != tenantID || run.key.SessionID != sessionID {
			continue
		}
		if run.state == "running" {
			if c.expiredLocked(run) {
				run.state = "outcome_unknown"
				run.terminal = RunTerminal{Type: "outcome_unknown", Error: "owner_lost"}
				if run.permit != nil {
					closeOnce(run.permit.lost)
				}
			} else {
				running = run
			}
		}
		if run.state == "queued" && (next == nil || run.enqueueOrder < next.enqueueOrder) {
			next = run
		}
	}
	if running != nil || next == nil {
		return
	}
	c.takeoverLocked(next)
}

func (c *InMemoryRunCoordinator) RequestCancel(ctx context.Context, key RunKey) (CancelDisposition, error) {
	if err := validateRunKey(key); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	run := c.runs[key]
	if run == nil {
		return CancelNotFound, nil
	}
	if isRunTerminal(run.state) {
		return CancelExisting, nil
	}
	select {
	case <-run.cancel:
	default:
		close(run.cancel)
	}
	if run.state == "queued" {
		run.state = "cancelled"
		run.terminal = RunTerminal{Type: "cancelled", Error: "cancelled_before_start"}
		c.promoteLocked(key.TenantID, key.SessionID)
	}
	return CancelRequested, nil
}

func (c *InMemoryRunCoordinator) ReconcileTerminal(ctx context.Context, key RunKey, terminal RunTerminal) error {
	if err := validateRunKey(key); err != nil {
		return err
	}
	if err := validateRunTerminal(terminal); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	run := c.runs[key]
	if run == nil {
		return ErrRunNotFound
	}
	if isRunTerminal(run.state) {
		return nil
	}
	run.state, run.terminal, run.owner = terminal.Type, terminal, ""
	run.leaseExpiry = time.Unix(0, 0).UTC()
	c.promoteLocked(key.TenantID, key.SessionID)
	return nil
}

func (c *InMemoryRunCoordinator) closeRun(run *memoryRun) {
	if run.owner != "" {
		run.owner = ""
	}
	if run.state == "running" {
		run.leaseExpiry = time.Unix(0, 0).UTC()
	}
	c.promoteLocked(run.key.TenantID, run.key.SessionID)
}

func (c *InMemoryRunCoordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	for _, run := range c.runs {
		if run.state == "running" {
			select {
			case <-run.permit.lost:
			default:
				close(run.permit.lost)
			}
			select {
			case <-run.cancel:
			default:
				close(run.cancel)
			}
		}
	}
	return nil
}

// Expire is a test/operations hook that simulates a Gateway losing its lease.
func (c *InMemoryRunCoordinator) Expire(key RunKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	run := c.runs[key]
	if run == nil || run.state != "running" {
		return ErrRunNotFound
	}
	run.leaseExpiry = time.Unix(0, 0).UTC()
	closeOnce(run.permit.lost)
	return nil
}

type memoryPermit struct {
	coordinator *InMemoryRunCoordinator
	run         *memoryRun
	mu          sync.Mutex
	fence       uint64
	lost        chan struct{}
	terminal    bool
	once        sync.Once
}

func (p *memoryPermit) FencingToken() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fence
}
func (p *memoryPermit) Lost() <-chan struct{}            { return p.lost }
func (p *memoryPermit) CancelRequested() <-chan struct{} { return p.run.cancel }
func (p *memoryPermit) Wait(ctx context.Context) error {
	ticker := time.NewTicker(p.coordinator.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.run.ready:
			return nil
		case <-p.run.cancel:
			return context.Canceled
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.coordinator.mu.Lock()
			p.coordinator.promoteLocked(p.run.key.TenantID, p.run.key.SessionID)
			running := p.run.state == "running" && p.run.permit == p
			terminal := isRunTerminal(p.run.state)
			p.coordinator.mu.Unlock()
			if running {
				return nil
			}
			if terminal {
				return context.Canceled
			}
		}
	}
}
func (p *memoryPermit) renew() {
	ticker := time.NewTicker(p.coordinator.renewInterval)
	defer ticker.Stop()
	for range ticker.C {
		p.coordinator.mu.Lock()
		p.mu.Lock()
		fence := p.fence
		p.mu.Unlock()
		if p.coordinator.closed || p.run.state != "running" || p.run.owner != p.coordinator.owner || p.run.fence != fence {
			p.coordinator.mu.Unlock()
			return
		}
		select {
		case <-p.lost:
			p.coordinator.mu.Unlock()
			return
		default:
		}
		select {
		case <-p.run.cancel:
			p.coordinator.mu.Unlock()
			return
		default:
		}
		p.run.leaseExpiry = time.Now().UTC().Add(p.coordinator.ttl)
		p.coordinator.mu.Unlock()
	}
}
func (p *memoryPermit) Finish(ctx context.Context, terminal RunTerminal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRunTerminal(terminal); err != nil {
		return err
	}
	p.coordinator.mu.Lock()
	defer p.coordinator.mu.Unlock()
	if p.terminal {
		return nil
	}
	p.mu.Lock()
	fence := p.fence
	p.mu.Unlock()
	if p.run.state != "running" || p.run.owner != p.coordinator.owner || p.run.fence != fence {
		return ErrStaleRunOwner
	}
	p.run.state, p.run.terminal = terminal.Type, terminal
	p.coordinator.closeRun(p.run)
	p.terminal = true
	return nil
}
func (p *memoryPermit) Release() {
	p.once.Do(func() {
		p.coordinator.mu.Lock()
		defer p.coordinator.mu.Unlock()
		if p.terminal {
			return
		}
		// Releasing without a terminal leaves the run recoverable by a later
		// claimant. It is intentionally not marked successful or cancelled.
		p.mu.Lock()
		fence := p.fence
		p.mu.Unlock()
		if p.run.state == "running" && p.run.owner == p.coordinator.owner && p.run.fence == fence {
			p.run.state = "queued"
			p.run.owner = ""
			p.run.leaseExpiry = time.Unix(0, 0).UTC()
		}
	})
}

func isRunTerminal(state string) bool {
	switch state {
	case "completed", "failed", "cancelled", "outcome_unknown":
		return true
	default:
		return false
	}
}

func validateRunTerminal(terminal RunTerminal) error {
	if !isRunTerminal(terminal.Type) {
		return errors.New("invalid_run_terminal")
	}
	return nil
}

func runTerminalState(eventType string) string {
	switch eventType {
	case "run.completed", "completed":
		return "completed"
	case "run.failed", "failed":
		return "failed"
	case "run.cancelled", "cancelled":
		return "cancelled"
	case "outcome_unknown":
		return "outcome_unknown"
	default:
		return eventType
	}
}

// PostgresRunCoordinator persists the same state machine in the shared
// Control Plane database. Claims use short serializable transactions; queued
// permits poll for promotion so HTTP handlers never need sticky placement.
type PostgresRunCoordinator struct {
	db            *sql.DB
	owner         string
	ttl           time.Duration
	renewInterval time.Duration
}

func NewPostgresRunCoordinator(dsn, owner string, timing ...time.Duration) (*PostgresRunCoordinator, error) {
	if strings.TrimSpace(dsn) == "" || strings.TrimSpace(owner) == "" {
		return nil, errors.New("PostgreSQL DSN and Gateway owner are required")
	}
	ttl, renew := 30*time.Second, 10*time.Second
	if len(timing) > 0 {
		if len(timing) != 2 || timing[0] <= 0 || timing[1] <= 0 || timing[1] >= timing[0] {
			return nil, errors.New("run coordinator timing is invalid")
		}
		ttl, renew = timing[0], timing[1]
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &PostgresRunCoordinator{db: db, owner: owner, ttl: ttl, renewInterval: renew}, nil
}

func (c *PostgresRunCoordinator) Claim(ctx context.Context, key RunKey, input string) (RunPermit, ClaimDisposition, error) {
	if err := validateRunKey(key); err != nil {
		return nil, "", err
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	hash := runInputHash(input)
	for {
		permit, disposition, retry, err := c.claimOnce(ctx, key, hash)
		if err != nil || !retry {
			if err == nil && disposition == Claimed {
				if postgresPermit, ok := permit.(*postgresPermit); ok {
					go postgresPermit.renew()
				}
			}
			return permit, disposition, err
		}
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (c *PostgresRunCoordinator) claimOnce(ctx context.Context, key RunKey, hash string) (permit RunPermit, disposition ClaimDisposition, retry bool, err error) {
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, "", false, err
	}
	defer func() {
		_ = tx.Rollback()
		if isPostgresRetryable(err) {
			permit, disposition, retry, err = nil, "", true, nil
		}
	}()
	_, err = tx.ExecContext(ctx, `INSERT INTO run_executions(tenant_id,session_id,request_id,input_hash,owner_id,fencing_token,state,lease_expires_at,cancel_requested_at,terminal_type,updated_at)
		VALUES($1,$2,$3,$4,'',0,'queued',TIMESTAMPTZ 'epoch',NULL,'',NOW()) ON CONFLICT(tenant_id,session_id,request_id) DO NOTHING`, key.TenantID, key.SessionID, key.RequestID, hash)
	if err != nil {
		return nil, "", false, err
	}
	var storedHash, owner, state, terminalType string
	var fence int64
	var expires time.Time
	if err := tx.QueryRowContext(ctx, `SELECT input_hash,owner_id,fencing_token,state,lease_expires_at,terminal_type FROM run_executions WHERE tenant_id=$1 AND session_id=$2 AND request_id=$3 FOR UPDATE`, key.TenantID, key.SessionID, key.RequestID).Scan(&storedHash, &owner, &fence, &state, &expires, &terminalType); err != nil {
		return nil, "", false, err
	}
	if storedHash != hash {
		return nil, "", false, ErrIdempotencyKeyReused
	}
	if isRunTerminal(state) {
		if err := tx.Commit(); err != nil {
			return nil, "", false, err
		}
		if terminalType == "" {
			terminalType = state
		}
		return newPostgresPermit(c, key, hash, uint64(maxInt64(fence, 0)), true, true, terminalType), ClaimExisting, false, nil
	}
	now := time.Now().UTC()
	if state == "running" && owner != "" && expires.After(now) {
		return nil, ClaimAlreadyRun, false, tx.Commit()
	}
	// A dead Gateway leaves its row in running until another claimant
	// observes the expired lease. Retire every stale owner in this session
	// before promoting the queue so the partial unique index cannot block
	// takeover of a different request.
	if _, err := tx.ExecContext(ctx, `UPDATE run_executions SET state='outcome_unknown',terminal_type='outcome_unknown',owner_id='',lease_expires_at=TIMESTAMPTZ 'epoch',updated_at=NOW() WHERE tenant_id=$1 AND session_id=$2 AND state='running' AND (owner_id='' OR lease_expires_at <= $3)`, key.TenantID, key.SessionID, now); err != nil {
		return nil, "", false, err
	}
	if state == "running" {
		state, owner, terminalType = "outcome_unknown", "", "outcome_unknown"
	}
	var running int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_executions WHERE tenant_id=$1 AND session_id=$2 AND state='running' AND lease_expires_at > $3`, key.TenantID, key.SessionID, now).Scan(&running); err != nil {
		return nil, "", false, err
	}
	if running > 0 {
		if err := tx.Commit(); err != nil {
			return nil, "", false, err
		}
		return newPostgresPermit(c, key, hash, uint64(maxInt64(fence, 0)), false, false, ""), ClaimQueued, false, nil
	}
	var firstRequest string
	err = tx.QueryRowContext(ctx, `SELECT request_id FROM run_executions WHERE tenant_id=$1 AND session_id=$2 AND state='queued' ORDER BY enqueue_order LIMIT 1`, key.TenantID, key.SessionID).Scan(&firstRequest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, "", false, err
	}
	if firstRequest != key.RequestID {
		if err := tx.Commit(); err != nil {
			return nil, "", false, err
		}
		return newPostgresPermit(c, key, hash, uint64(maxInt64(fence, 0)), false, false, ""), ClaimQueued, false, nil
	}
	var nextFence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(fencing_token),0)+1 FROM run_executions WHERE tenant_id=$1 AND session_id=$2`, key.TenantID, key.SessionID).Scan(&nextFence); err != nil {
		return nil, "", false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_executions SET owner_id=$4,fencing_token=$5,state='running',lease_expires_at=$6,updated_at=NOW() WHERE tenant_id=$1 AND session_id=$2 AND request_id=$3`, key.TenantID, key.SessionID, key.RequestID, c.owner, nextFence, now.Add(c.ttl)); err != nil {
		return nil, "", false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, "", false, err
	}
	return newPostgresPermit(c, key, hash, uint64(nextFence), true, false, ""), Claimed, false, nil
}

func (c *PostgresRunCoordinator) RequestCancel(ctx context.Context, key RunKey) (CancelDisposition, error) {
	if err := validateRunKey(key); err != nil {
		return "", err
	}
	result, err := c.db.ExecContext(ctx, `UPDATE run_executions SET cancel_requested_at=COALESCE(cancel_requested_at,NOW()),state=CASE WHEN state='queued' THEN 'cancelled' ELSE state END,terminal_type=CASE WHEN state='queued' THEN 'cancelled' ELSE terminal_type END,updated_at=NOW() WHERE tenant_id=$1 AND session_id=$2 AND request_id=$3 AND state IN ('queued','running')`, key.TenantID, key.SessionID, key.RequestID)
	if err != nil {
		return "", err
	}
	rows, _ := result.RowsAffected()
	if rows == 1 {
		return CancelRequested, nil
	}
	var state string
	err = c.db.QueryRowContext(ctx, `SELECT state FROM run_executions WHERE tenant_id=$1 AND session_id=$2 AND request_id=$3`, key.TenantID, key.SessionID, key.RequestID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return CancelNotFound, nil
	}
	if err != nil {
		return "", err
	}
	if isRunTerminal(state) {
		return CancelExisting, nil
	}
	return CancelNotFound, nil
}

func (c *PostgresRunCoordinator) ReconcileTerminal(ctx context.Context, key RunKey, terminal RunTerminal) error {
	if err := validateRunKey(key); err != nil {
		return err
	}
	if err := validateRunTerminal(terminal); err != nil {
		return err
	}
	_, err := c.db.ExecContext(ctx, `UPDATE run_executions SET state=$1,terminal_type=$1,owner_id='',lease_expires_at=TIMESTAMPTZ 'epoch',updated_at=NOW() WHERE tenant_id=$2 AND session_id=$3 AND request_id=$4 AND state IN ('queued','running')`, terminal.Type, key.TenantID, key.SessionID, key.RequestID)
	return err
}

func (c *PostgresRunCoordinator) Close() error { return c.db.Close() }

type postgresPermit struct {
	coordinator  *PostgresRunCoordinator
	key          RunKey
	inputHash    string
	mu           sync.Mutex
	fence        uint64
	ready        chan struct{}
	cancel       chan struct{}
	lost         chan struct{}
	terminal     bool
	terminalType string
	once         sync.Once
}

func newPostgresPermit(c *PostgresRunCoordinator, key RunKey, inputHash string, fence uint64, ready bool, terminal bool, terminalType string) *postgresPermit {
	readyChan := make(chan struct{})
	if ready {
		close(readyChan)
	}
	return &postgresPermit{
		coordinator: c, key: key, inputHash: inputHash, fence: fence,
		ready: readyChan, cancel: make(chan struct{}), lost: make(chan struct{}),
		terminal: terminal, terminalType: terminalType,
	}
}
func (p *postgresPermit) FencingToken() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fence
}
func (p *postgresPermit) Lost() <-chan struct{}            { return p.lost }
func (p *postgresPermit) CancelRequested() <-chan struct{} { return p.cancel }
func (p *postgresPermit) Wait(ctx context.Context) error {
	p.mu.Lock()
	terminal := p.terminal
	ready := p.ready
	p.mu.Unlock()
	if terminal {
		return nil
	}
	ticker := time.NewTicker(p.coordinator.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.cancel:
			return context.Canceled
		case <-ready:
			return nil
		case <-ticker.C:
			claimed, err := p.tryPromote(ctx)
			if err != nil {
				return err
			}
			if claimed {
				return nil
			}
		}
	}
}
func (p *postgresPermit) tryPromote(ctx context.Context) (bool, error) {
	permit, disposition, _, err := p.coordinator.claimOnce(ctx, p.key, p.inputHash)
	if err != nil {
		return false, err
	}
	if disposition == Claimed {
		if next, ok := permit.(*postgresPermit); ok {
			p.mu.Lock()
			p.fence = next.fence
			ready := p.ready
			p.mu.Unlock()
			closeOnce(ready)
			go p.renew()
		}
		return true, nil
	}
	if disposition == ClaimExisting {
		if next, ok := permit.(*postgresPermit); ok && next.terminalType == "cancelled" {
			closeOnce(p.cancel)
			return false, context.Canceled
		}
		return false, ErrRunNotFound
	}
	return false, nil
}
func (p *postgresPermit) renew() {
	ticker := time.NewTicker(p.coordinator.renewInterval)
	defer ticker.Stop()
	for range ticker.C {
		p.mu.Lock()
		if p.terminal {
			p.mu.Unlock()
			return
		}
		fence := p.fence
		p.mu.Unlock()
		result, err := p.coordinator.db.Exec(`UPDATE run_executions SET lease_expires_at=$1,updated_at=NOW() WHERE tenant_id=$2 AND session_id=$3 AND request_id=$4 AND owner_id=$5 AND fencing_token=$6 AND state='running'`, time.Now().UTC().Add(p.coordinator.ttl), p.key.TenantID, p.key.SessionID, p.key.RequestID, p.coordinator.owner, fence)
		if err != nil {
			closeOnce(p.lost)
			return
		}
		rows, _ := result.RowsAffected()
		if rows != 1 {
			closeOnce(p.lost)
			return
		}
		var cancelAt sql.NullTime
		if err := p.coordinator.db.QueryRow(`SELECT cancel_requested_at FROM run_executions WHERE tenant_id=$1 AND session_id=$2 AND request_id=$3`, p.key.TenantID, p.key.SessionID, p.key.RequestID).Scan(&cancelAt); err == nil && cancelAt.Valid {
			closeOnce(p.cancel)
		}
	}
}
func closeOnce(ch chan struct{}) { defer func() { _ = recover() }(); close(ch) }

func isPostgresRetryable(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "serialization failure") || strings.Contains(message, "deadlock detected")
}

func (p *postgresPermit) Finish(ctx context.Context, terminal RunTerminal) error {
	p.mu.Lock()
	if p.terminal {
		p.mu.Unlock()
		return nil
	}
	fence := p.fence
	p.mu.Unlock()
	if err := validateRunTerminal(terminal); err != nil {
		return err
	}
	result, err := p.coordinator.db.ExecContext(ctx, `UPDATE run_executions SET state=$1,terminal_type=$2,owner_id='',lease_expires_at=TIMESTAMPTZ 'epoch',updated_at=NOW() WHERE tenant_id=$3 AND session_id=$4 AND request_id=$5 AND owner_id=$6 AND fencing_token=$7 AND state='running'`, terminal.Type, terminal.Type, p.key.TenantID, p.key.SessionID, p.key.RequestID, p.coordinator.owner, fence)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrStaleRunOwner
	}
	p.mu.Lock()
	p.terminal = true
	p.terminalType = terminal.Type
	p.mu.Unlock()
	return nil
}
func (p *postgresPermit) Release() {
	p.once.Do(func() {
		p.mu.Lock()
		if p.terminal {
			p.mu.Unlock()
			return
		}
		fence := p.fence
		p.mu.Unlock()
		_, _ = p.coordinator.db.Exec(`UPDATE run_executions SET state='queued',owner_id='',lease_expires_at=TIMESTAMPTZ 'epoch',updated_at=NOW() WHERE tenant_id=$1 AND session_id=$2 AND request_id=$3 AND owner_id=$4 AND fencing_token=$5 AND state='running'`, p.key.TenantID, p.key.SessionID, p.key.RequestID, p.coordinator.owner, fence)
	})
}

func maxInt64(value, fallback int64) int64 {
	if value > fallback {
		return value
	}
	return fallback
}
