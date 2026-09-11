package domain

import (
	"encoding/json"
	"time"
)

// ProfileDraft is the single mutable working copy owned by a RuntimeProfile.
type ProfileDraft struct {
	TenantID  string
	ProfileID string
	Revision  int64
	Spec      json.RawMessage
	UpdatedBy string
	UpdatedAt time.Time
}

func (d ProfileDraft) Clone() ProfileDraft {
	d.Spec = append(json.RawMessage(nil), d.Spec...)
	return d
}
