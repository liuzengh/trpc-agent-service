package accountcatalog

import "time"

// Poll is trusted local request metadata, not part of either Control wire DTO.
// Start watermarks and clocks are captured by the replica before HTTP begins.
type Poll struct {
	Revision                                        int64
	Digest                                          string
	StartedAt, LocalStarted                         time.Time
	ScopeID, SourceEpoch, InstanceID, InstanceEpoch string
}
type Qualification struct {
	Generation, Revision int64
	ValidUntil           time.Time
}
