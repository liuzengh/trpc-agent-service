// Package agentapp defines the tenant-scoped Agent App control-plane domain.
package agentapp

import "time"

type Status string

const (
	StatusDraft     Status = "draft"
	StatusActive    Status = "active"
	StatusSuspended Status = "suspended"
	StatusDisabled  Status = "disabled"
)

type AgentApp struct {
	TenantID        string
	AgentAppID      string
	AgentAppKey     string
	DisplayName     string
	Description     string
	Status          Status
	CurrentRevision int64
	NextRevision    int64
	Version         int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
