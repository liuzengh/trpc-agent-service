package domain

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const (
	CompilerVersionV1        = "deployment-compiler-v1"
	RuntimeContractVersionV1 = "worker-manifest-v1"
)

// AgentVersionSource is the immutable, integrity-checked Agent owner snapshot
// consumed by the compiler. PublishedBy and PublishedAt deliberately do not
// participate in compilation.
type AgentVersionSource struct {
	TenantID      string
	AgentID       string
	VersionID     string
	VersionNumber int64
	SchemaVersion string
	SpecDigest    string
	Spec          agentdomain.Spec
}

// ProfileRevisionSource is the immutable, integrity-checked Runtime Profile
// owner snapshot consumed by the compiler. It contains internal credential
// associations, but never credential values, ciphertext, or live state.
type ProfileRevisionSource struct {
	TenantID       string
	ProfileID      string
	RevisionID     string
	RevisionNumber int64
	SchemaVersion  string
	SpecDigest     string
	Spec           profiledomain.Spec
}

// CompileInput contains only fixed values. The compiler performs no I/O and
// does not consult mutable Draft, credential, provider, or worker state.
type CompileInput struct {
	ManagedBackends map[string]datav1.Snapshot
	TenantID        string
	Agent           AgentVersionSource
	Profile         ProfileRevisionSource
	Platform        PlatformExecutionContract
}
