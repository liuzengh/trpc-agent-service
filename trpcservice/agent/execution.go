package agent

import (
	"context"
	"errors"
	"fmt"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrInvalid reports an invalid external-agent execution snapshot.
	ErrInvalid = errors.New("invalid agent execution")
)

type executionSnapshotContextKey struct{}

// AgentExecutionSnapshot is the sealed, immutable control-plane input for one
// Worker execution. It contains no credentials or live runtime clients.
type AgentExecutionSnapshot struct {
	tenant   *tenant.Tenant
	app      *appmodel.App
	revision *appmodel.Revision
}

// FactoryCacheKey is the comparable identity for one materialized Agent.
// Versions prevent mutable tenant or App metadata from reusing stale entries.
type FactoryCacheKey struct {
	TenantID      string
	TenantVersion int64
	AppID         string
	AppVersion    int64
	Revision      int64
	ContentDigest string
}

// LLMAgentFactoryInput is the provider-neutral definition mapped into an
// executable Agent by a later dependency resolver. The historical name is
// kept for source compatibility while the input now also carries composite
// Agent definitions. References remain IDs; secrets and live clients are
// intentionally absent.
type LLMAgentFactoryInput struct {
	TenantID          string
	TenantVersion     int64
	AppID             string
	AppKey            string
	AppVersion        int64
	DisplayName       string
	Name              string
	Description       string
	Revision          int64
	ContentDigest     string
	Kind              appmodel.Kind
	SchemaVersion     int
	Instruction       string
	GlobalInstruction string
	ModelProfileID    string
	Generation        appmodel.GenerationConfig
	Runtime           appmodel.RuntimePolicy
	Tools             []appmodel.ToolAuthorization
	Chain             *appmodel.ChainConfiguration
}

// Clone returns a defensive copy of Factory input pointer and slice fields.
func (input LLMAgentFactoryInput) Clone() LLMAgentFactoryInput {
	clone := input
	clone.Generation = cloneGenerationConfig(input.Generation)
	clone.Tools = cloneTools(input.Tools)
	clone.Chain = input.Chain.Clone()
	return clone
}

// NewAgentExecutionSnapshot validates and freezes an active Tenant snapshot,
// active App root, and the App's selected immutable published Revision.
func NewAgentExecutionSnapshot(tenantSnapshot tenant.ConfigurationSnapshot, appRoot *appmodel.App, revision *appmodel.Revision) (AgentExecutionSnapshot, error) {
	tenantValue := tenantSnapshot.Tenant()
	if err := validateExecutionState(tenantValue, appRoot, revision); err != nil {
		return AgentExecutionSnapshot{}, err
	}
	tenantCopy := tenantValue.Clone()
	appCopy := appRoot.Clone()
	revisionCopy := revision.Clone()
	return AgentExecutionSnapshot{tenant: &tenantCopy, app: &appCopy, revision: &revisionCopy}, nil
}

func validateExecutionState(tenantValue tenant.Tenant, appRoot *appmodel.App, revision *appmodel.Revision) error {
	if err := tenantValue.Validate(); err != nil {
		return fmt.Errorf("%w: invalid tenant snapshot: %v", ErrInvalid, err)
	}
	if !tenantValue.CanAcceptExecution() {
		return fmt.Errorf("%w: tenant status %q cannot accept execution", ErrInvalid, tenantValue.Status)
	}
	if appRoot == nil {
		return fmt.Errorf("%w: App snapshot is required", ErrInvalid)
	}
	if err := appRoot.Validate(); err != nil {
		return fmt.Errorf("%w: invalid App snapshot: %v", ErrInvalid, err)
	}
	if !appRoot.CanAcceptExecution() {
		return fmt.Errorf("%w: App status %q cannot accept execution", ErrInvalid, appRoot.Status)
	}
	if revision == nil {
		return fmt.Errorf("%w: Revision snapshot is required", ErrInvalid)
	}
	if err := revision.Validate(); err != nil {
		return fmt.Errorf("%w: invalid Revision snapshot: %v", ErrInvalid, err)
	}
	if revision.State != appmodel.RevisionStatePublished {
		return fmt.Errorf("%w: execution requires a published Revision", ErrInvalid)
	}
	if tenantValue.TenantID != appRoot.TenantID || tenantValue.TenantID != revision.TenantID || appRoot.AppID != revision.AppID {
		return fmt.Errorf("%w: Tenant, App, and Revision scopes must match", ErrInvalid)
	}
	if appRoot.CurrentRevision == nil || (*appRoot.CurrentRevision != revision.Revision && (appRoot.CanaryRevision == nil || *appRoot.CanaryRevision != revision.Revision)) {
		return fmt.Errorf("%w: Revision is not the App current revision", ErrInvalid)
	}
	return nil
}

// Tenant returns the fixed tenant version captured for this execution.
func (snapshot AgentExecutionSnapshot) Tenant() tenant.Tenant {
	if snapshot.tenant == nil {
		return tenant.Tenant{}
	}
	return snapshot.tenant.Clone()
}

// App returns a defensive copy of the fixed App root.
func (snapshot AgentExecutionSnapshot) App() appmodel.App {
	if snapshot.app == nil {
		return appmodel.App{}
	}
	return snapshot.app.Clone()
}

// Revision returns a defensive copy of immutable executable content.
func (snapshot AgentExecutionSnapshot) Revision() appmodel.Revision {
	if snapshot.revision == nil {
		return appmodel.Revision{}
	}
	return snapshot.revision.Clone()
}

// CacheKey returns the stable Factory cache identity for the snapshot.
func (snapshot AgentExecutionSnapshot) CacheKey() (FactoryCacheKey, error) {
	if err := snapshot.validate(); err != nil {
		return FactoryCacheKey{}, err
	}
	return FactoryCacheKey{
		TenantID: snapshot.tenant.TenantID, TenantVersion: snapshot.tenant.Version,
		AppID: snapshot.app.AppID, AppVersion: snapshot.app.Version,
		Revision: snapshot.revision.Revision, ContentDigest: snapshot.revision.ContentDigest,
	}, nil
}

// FactoryInput maps the sealed domain state into a secret-free LLMAgent
// construction contract. Name uses the stable App key; executable description
// and behavior come from the immutable Revision.
func (snapshot AgentExecutionSnapshot) FactoryInput() (LLMAgentFactoryInput, error) {
	if err := snapshot.validate(); err != nil {
		return LLMAgentFactoryInput{}, err
	}
	return LLMAgentFactoryInput{
		TenantID: snapshot.tenant.TenantID, TenantVersion: snapshot.tenant.Version,
		AppID: snapshot.app.AppID, AppKey: snapshot.app.AppKey, AppVersion: snapshot.app.Version,
		DisplayName: snapshot.app.DisplayName, Name: snapshot.app.AppKey,
		Description: snapshot.revision.Description, Revision: snapshot.revision.Revision,
		ContentDigest: snapshot.revision.ContentDigest, Kind: snapshot.revision.Kind,
		SchemaVersion: snapshot.revision.SchemaVersion, Instruction: snapshot.revision.Instruction,
		GlobalInstruction: snapshot.revision.GlobalInstruction, ModelProfileID: snapshot.revision.ModelProfileID,
		Generation: cloneGenerationConfig(snapshot.revision.Generation), Runtime: snapshot.revision.Runtime,
		Tools: cloneTools(snapshot.revision.Tools), Chain: snapshot.revision.Chain.Clone(),
	}, nil
}

// WithAgentExecutionSnapshot carries a defensive snapshot copy for one
// execution. Invalid or zero snapshots overwrite the key with an empty value.
func WithAgentExecutionSnapshot(ctx context.Context, snapshot AgentExecutionSnapshot) context.Context {
	if err := snapshot.validate(); err != nil {
		return context.WithValue(ctx, executionSnapshotContextKey{}, AgentExecutionSnapshot{})
	}
	return context.WithValue(ctx, executionSnapshotContextKey{}, snapshot.clone())
}

// AgentExecutionSnapshotFromContext returns a validated defensive copy.
func AgentExecutionSnapshotFromContext(ctx context.Context) (AgentExecutionSnapshot, bool) {
	snapshot, ok := ctx.Value(executionSnapshotContextKey{}).(AgentExecutionSnapshot)
	if !ok || snapshot.validate() != nil {
		return AgentExecutionSnapshot{}, false
	}
	return snapshot.clone(), true
}

func (snapshot AgentExecutionSnapshot) validate() error {
	if snapshot.tenant == nil || snapshot.app == nil || snapshot.revision == nil {
		return fmt.Errorf("%w: execution snapshot is not initialized", ErrInvalid)
	}
	return validateExecutionState(*snapshot.tenant, snapshot.app, snapshot.revision)
}

func (snapshot AgentExecutionSnapshot) clone() AgentExecutionSnapshot {
	tenantCopy := snapshot.tenant.Clone()
	appCopy := snapshot.app.Clone()
	revisionCopy := snapshot.revision.Clone()
	return AgentExecutionSnapshot{tenant: &tenantCopy, app: &appCopy, revision: &revisionCopy}
}

func cloneGenerationConfig(configuration appmodel.GenerationConfig) appmodel.GenerationConfig {
	clone := configuration
	if configuration.Temperature != nil {
		value := *configuration.Temperature
		clone.Temperature = &value
	}
	if configuration.TopP != nil {
		value := *configuration.TopP
		clone.TopP = &value
	}
	if configuration.MaxOutputTokens != nil {
		value := *configuration.MaxOutputTokens
		clone.MaxOutputTokens = &value
	}
	return clone
}

func cloneTools(tools []appmodel.ToolAuthorization) []appmodel.ToolAuthorization {
	if tools == nil {
		return nil
	}
	clone := make([]appmodel.ToolAuthorization, len(tools))
	copy(clone, tools)
	return clone
}
