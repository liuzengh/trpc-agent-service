package domain

import (
	"encoding/json"
	"time"
)

// AgentDraft is the single mutable working copy owned by an Agent.
type AgentDraft struct {
	TenantID  string
	AgentID   string
	Revision  int64
	Spec      json.RawMessage
	UpdatedBy string
	UpdatedAt time.Time
}

func (d AgentDraft) Clone() AgentDraft {
	d.Spec = append(json.RawMessage(nil), d.Spec...)
	return d
}
