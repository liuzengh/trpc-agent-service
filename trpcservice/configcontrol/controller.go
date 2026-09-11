package configcontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

type ApplyStateFunc func(TenantState, config.TenantConfig, *config.TenantConfig) error

type ControllerOptions struct {
	Store             Store
	Apply             ApplyStateFunc
	NodeID            string
	BootID            string
	RefreshInterval   time.Duration
	HeartbeatInterval time.Duration
	HeartbeatTTL      time.Duration
	AckTimeout        time.Duration
}

// Controller coordinates local Registry refreshes with the persistent
// release state machine. PostgreSQL LISTEN is represented by the poll loop as
// well: polling is the correctness fallback for notification reconnects and
// keeps the same code path usable by MemoryStore tests.
type Controller struct {
	store             Store
	apply             ApplyStateFunc
	node              NodeIdentity
	refreshInterval   time.Duration
	heartbeatInterval time.Duration
	heartbeatTTL      time.Duration
	ackTimeout        time.Duration

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	start     sync.Once
	close     sync.Once
	refreshMu sync.Mutex
	processMu sync.Mutex
	ready     atomic.Bool
	lastError atomic.Value // string
}

func NewController(options ControllerOptions) (*Controller, error) {
	if options.Store == nil {
		return nil, errors.New("config control: store is required")
	}
	if options.Apply == nil {
		return nil, errors.New("config control: apply callback is required")
	}
	if options.NodeID == "" {
		return nil, errors.New("config control: stable node_id is required")
	}
	if options.BootID == "" {
		options.BootID = uuid.NewString()
	}
	if options.RefreshInterval <= 0 {
		options.RefreshInterval = 5 * time.Second
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = 5 * time.Second
	}
	if options.HeartbeatTTL <= 0 {
		options.HeartbeatTTL = 30 * time.Second
	}
	if options.AckTimeout <= 0 {
		options.AckTimeout = 30 * time.Second
	}
	if memory, ok := options.Store.(*MemoryStore); ok {
		memory.SetHeartbeatTTL(options.HeartbeatTTL)
	}
	return &Controller{
		store: options.Store, apply: options.Apply,
		node:            NodeIdentity{NodeID: options.NodeID, BootID: options.BootID},
		refreshInterval: options.RefreshInterval, heartbeatInterval: options.HeartbeatInterval,
		heartbeatTTL: options.HeartbeatTTL, ackTimeout: options.AckTimeout,
	}, nil
}

func (c *Controller) Store() Store       { return c.store }
func (c *Controller) Node() NodeIdentity { return c.node }
func (c *Controller) Ready() bool        { return c.ready.Load() }
func (c *Controller) RefreshNow(ctx context.Context) error {
	if err := c.processPending(ctx); err != nil {
		c.setError(err)
		c.ready.Store(false)
		return err
	}
	c.ready.Store(true)
	c.lastError.Store("")
	return nil
}
func (c *Controller) LastError() string {
	value := c.lastError.Load()
	if value == nil {
		return ""
	}
	return value.(string)
}

func (c *Controller) Bootstrap(ctx context.Context, tenants []config.TenantConfig, actor, reason string) error {
	for _, item := range tenants {
		if err := config.ValidateTenantConfig(item); err != nil {
			return err
		}
	}
	return c.store.Bootstrap(ctx, tenants, actor, reason)
}

func (c *Controller) Start(parent context.Context) error {
	var startErr error
	c.start.Do(func() {
		c.ctx, c.cancel = context.WithCancel(parent)
		if err := c.refresh(c.ctx); err != nil {
			startErr = err
			c.setError(err)
			return
		}
		if err := c.processPending(c.ctx); err != nil {
			startErr = err
			c.setError(err)
			return
		}
		if err := c.refresh(c.ctx); err != nil {
			startErr = err
			c.setError(err)
			return
		}
		if err := c.heartbeat(c.ctx); err != nil {
			startErr = err
			c.setError(err)
			return
		}
		c.ready.Store(true)
		c.lastError.Store("")
		c.wg.Add(1)
		go c.loop()
		if listener, ok := c.store.(NotificationSource); ok {
			c.wg.Add(1)
			go c.listen(listener)
		}
	})
	return startErr
}

func (c *Controller) Close() {
	c.close.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		c.wg.Wait()
		c.ready.Store(false)
	})
}

func (c *Controller) loop() {
	defer c.wg.Done()
	refreshTicker := time.NewTicker(c.refreshInterval)
	heartbeatTicker := time.NewTicker(c.heartbeatInterval)
	defer refreshTicker.Stop()
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-refreshTicker.C:
			if err := c.processPending(c.ctx); err != nil {
				c.setError(err)
				c.ready.Store(false)
			} else {
				c.lastError.Store("")
				c.ready.Store(true)
			}
		case <-heartbeatTicker.C:
			if err := c.heartbeat(c.ctx); err != nil {
				c.setError(err)
			}
		}
	}
}

func (c *Controller) listen(listener NotificationSource) {
	defer c.wg.Done()
	for {
		if c.ctx.Err() != nil {
			return
		}
		err := listener.Listen(c.ctx, func() {
			if processErr := c.processPending(c.ctx); processErr != nil {
				c.setError(processErr)
				c.ready.Store(false)
			}
		})
		if c.ctx.Err() != nil {
			return
		}
		if err != nil {
			c.setError(err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (c *Controller) refresh(ctx context.Context) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	states, err := c.store.ListStates(ctx)
	if err != nil {
		return err
	}
	for _, state := range states {
		active, err := c.store.GetRevision(ctx, state.TenantID, state.ActiveRevision)
		if err != nil {
			return fmt.Errorf("load active revision %s/%s: %w", state.TenantID, state.ActiveRevision, err)
		}
		var canary *config.TenantConfig
		if state.CanaryRevision != "" {
			revision, err := c.store.GetRevision(ctx, state.TenantID, state.CanaryRevision)
			if err != nil {
				return fmt.Errorf("load canary revision %s/%s: %w", state.TenantID, state.CanaryRevision, err)
			}
			copy := revision.Tenant
			canary = &copy
		}
		if err := c.apply(state, active.Tenant, canary); err != nil {
			return fmt.Errorf("apply tenant %s generation %d: %w", state.TenantID, state.Generation, err)
		}
	}
	return nil
}

func (c *Controller) processPending(ctx context.Context) error {
	c.processMu.Lock()
	defer c.processMu.Unlock()
	if err := c.refresh(ctx); err != nil {
		return err
	}
	releases, err := c.store.ListPendingReleases(ctx)
	if err != nil {
		return err
	}
	for _, release := range releases {
		if c.releaseExpired(release) {
			if err := c.store.FailRelease(ctx, release.ReleaseID, "node acknowledgement timeout"); err != nil && !errors.Is(err, ErrInvalidTransition) {
				return err
			}
			continue
		}
		if release.Status == ReleasePreparing {
			revision, loadErr := c.store.GetRevision(ctx, release.TenantID, release.TargetRevision)
			if loadErr == nil {
				loadErr = config.ValidateTenantConfig(revision.Tenant)
			}
			if ackErr := c.store.AckPrepared(ctx, release.ReleaseID, c.node, release.TargetRevision, release.ExpectedGeneration, loadErr); ackErr != nil && !errors.Is(ackErr, ErrNodeNotRegistered) {
				return ackErr
			}
		}
		activated, activateErr := c.store.ActivateIfReady(ctx, release.ReleaseID)
		if activateErr != nil && !errors.Is(activateErr, ErrGenerationConflict) {
			return activateErr
		}
		if activateErr == nil && activated.Status == ReleaseActivePending {
			// State changes are applied on the next refresh below. This explicit
			// refresh also makes ACKs reflect the revision that this node loaded.
			if err := c.refresh(ctx); err != nil {
				return err
			}
			state, stateErr := c.store.GetState(ctx, release.TenantID)
			if stateErr != nil {
				return stateErr
			}
			loaded := state.ActiveRevision
			if state.CanaryRevision != "" {
				loaded = state.CanaryRevision
			}
			if err := c.store.AckApplied(ctx, release.ReleaseID, c.node, loaded, state.Generation, nil); err != nil && !errors.Is(err, ErrNodeNotRegistered) {
				return err
			}
		}
	}
	return nil
}

func (c *Controller) releaseExpired(release Release) bool {
	if c.ackTimeout <= 0 {
		return false
	}
	deadlineBase := release.CreatedAt
	if release.Status == ReleaseActivePending && release.ActivatedAt != nil {
		deadlineBase = *release.ActivatedAt
	}
	return !deadlineBase.IsZero() && time.Since(deadlineBase) >= c.ackTimeout
}

func (c *Controller) heartbeat(ctx context.Context) error {
	states, err := c.store.ListStates(ctx)
	if err != nil {
		return err
	}
	var loadedGeneration int64
	var active, canary string
	if len(states) > 0 {
		loadedGeneration = states[0].Generation
		active, canary = states[0].ActiveRevision, states[0].CanaryRevision
	}
	return c.store.Heartbeat(ctx, NodeHeartbeat{
		NodeID: c.node.NodeID, BootID: c.node.BootID, Ready: c.ready.Load(),
		LoadedGeneration: loadedGeneration, ActiveRevision: active, CanaryRevision: canary,
		ErrorMessage: c.LastError(),
	})
}

func (c *Controller) setError(err error) {
	if err == nil {
		c.lastError.Store("")
		return
	}
	c.lastError.Store(safeError(err))
}

func (c *Controller) CreateRevision(ctx context.Context, input RevisionInput) (Revision, error) {
	if err := config.ValidateTenantConfig(input.Tenant); err != nil {
		return Revision{}, err
	}
	return c.store.PutRevision(ctx, input)
}

func (c *Controller) CreateRelease(ctx context.Context, request CreateReleaseRequest) (Release, error) {
	return c.store.CreateRelease(ctx, request)
}

func (c *Controller) ImportAndFullRelease(ctx context.Context, tenants []config.TenantConfig, actor, reason string, expected map[string]int64) ([]Release, error) {
	result := make([]Release, 0, len(tenants))
	for _, tenant := range tenants {
		if err := config.ValidateTenantConfig(tenant); err != nil {
			return nil, err
		}
		if _, err := c.store.EnsureTenant(ctx, tenant, actor, reason); err != nil {
			return nil, err
		}
		if _, err := c.store.PutRevision(ctx, RevisionInput{TenantID: tenant.TenantID, Revision: tenant.Version, Tenant: tenant, CreatedBy: actor, ChangeReason: reason}); err != nil {
			return nil, err
		}
		state, err := c.store.GetState(ctx, tenant.TenantID)
		if err != nil {
			return nil, err
		}
		generation := state.Generation
		if expected != nil && expected[tenant.TenantID] > 0 {
			generation = expected[tenant.TenantID]
		}
		if state.ActiveRevision == tenant.Version && state.CanaryRevision == "" {
			continue
		}
		release, err := c.store.CreateRelease(ctx, CreateReleaseRequest{
			TenantID: tenant.TenantID, Kind: ReleaseFull, TargetRevision: tenant.Version,
			ExpectedGeneration: generation, ExpectedActive: state.ActiveRevision,
			ExpectedCanary: state.CanaryRevision, RequestedBy: actor, ChangeReason: reason,
		})
		if err != nil {
			return nil, err
		}
		result = append(result, release)
	}
	return result, nil
}

func (c *Controller) Rollback(ctx context.Context, tenantID, targetRevision, actor, reason string, expectedGeneration int64) (Release, error) {
	state, err := c.store.GetState(ctx, tenantID)
	if err != nil {
		return Release{}, err
	}
	if targetRevision == "" {
		releases, err := c.store.ListReleases(ctx, tenantID)
		if err == nil {
			for i := len(releases) - 1; i >= 0; i-- {
				candidate := releases[i]
				if candidate.Status == ReleaseVerified && candidate.TargetRevision == state.ActiveRevision && candidate.SourceActiveRevision != state.ActiveRevision {
					targetRevision = candidate.SourceActiveRevision
					break
				}
			}
		} else if !errors.Is(err, ErrTenantNotFound) {
			return Release{}, err
		}
		if targetRevision == "" {
			revisions, err := c.store.ListRevisions(ctx, tenantID)
			if err != nil {
				return Release{}, err
			}
			for i := len(revisions) - 1; i >= 0; i-- {
				if revisions[i].Revision != state.ActiveRevision && revisions[i].Revision != state.CanaryRevision {
					targetRevision = revisions[i].Revision
					break
				}
			}
		}
	}
	if targetRevision == "" {
		return Release{}, ErrRevisionNotFound
	}
	if expectedGeneration <= 0 {
		expectedGeneration = state.Generation
	}
	return c.store.CreateRelease(ctx, CreateReleaseRequest{
		TenantID: tenantID, Kind: ReleaseRollback, TargetRevision: targetRevision,
		ExpectedGeneration: expectedGeneration, ExpectedActive: state.ActiveRevision,
		ExpectedCanary: state.CanaryRevision, RequestedBy: actor, ChangeReason: reason,
	})
}
