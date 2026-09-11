package accountcatalog

import "time"

type Observation struct {
	ReceiveMode        string    `json:"receive_mode,omitempty"`
	ScopeID            string    `json:"scope_id"`
	SourceEpoch        string    `json:"source_epoch"`
	TenantID           string    `json:"tenant_id"`
	AccountID          string    `json:"account_id"`
	Provider           string    `json:"provider"`
	ConnectionRevision int64     `json:"connection_revision"`
	InstanceID         string    `json:"instance_id"`
	InstanceEpoch      string    `json:"instance_epoch"`
	Sequence           int64     `json:"report_sequence"`
	State              string    `json:"state"`
	Reason             string    `json:"reason_code"`
	At                 time.Time `json:"observed_at"`
	OwnerEpoch         *int64    `json:"owner_epoch,omitempty"`
}
