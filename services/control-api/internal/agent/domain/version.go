package domain

import (
	"encoding/json"
	"time"
)

// AgentVersion is an immutable publication containing one canonical AgentSpec.
type AgentVersion struct {
	ID                  string
	TenantID            string
	AgentID             string
	VersionNumber       int64
	SourceDraftRevision int64
	SchemaVersion       string
	Spec                json.RawMessage
	SpecDigest          string
	PublishedBy         string
	PublishedAt         time.Time
}

func (v AgentVersion) Clone() AgentVersion {
	v.Spec = append(json.RawMessage(nil), v.Spec...)
	return v
}
