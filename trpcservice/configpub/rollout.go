package configpub

import (
	"crypto/sha256"
	"encoding/binary"
)

// assignmentDomain is the server-owned domain separator. Changing it would
// re-shuffle every tenant bucket, so it is versioned and immutable here.
const assignmentDomain = "p1-08-config-rollout-v1"

// BucketCount is the fixed upper bound of the assignment space. Buckets are
// derived deterministically from the tenant identity; there is no randomness
// and no dependency on request content, user content or provider responses.
const BucketCount = 100

// AssignmentBucket maps a tenant identity to a stable bucket in [0, 100).
// The tenant ID must already be a validated server-owned identifier; empty or
// oversized input fails closed instead of degrading to a default bucket.
func AssignmentBucket(tenantID string) (int, error) {
	if !validID(tenantID) {
		return 0, ErrInvalidArgument
	}
	digest := sha256.Sum256([]byte(assignmentDomain + "|" + tenantID))
	return int(binary.BigEndian.Uint64(digest[:8]) % BucketCount), nil
}

// Assign resolves the config version a request must use for the given bucket.
// The second return value is false when the durable rollout state cannot
// safely answer (missing baseline with a partial percentage), which callers
// must treat as fail-closed rather than falling back to any other version.
func (s RolloutState) Assign(bucket int) (int64, bool) {
	if bucket < 0 || bucket >= BucketCount {
		return 0, false
	}
	if s.ActiveVersion < 1 {
		return 0, false
	}
	if s.Percentage >= 100 {
		return s.ActiveVersion, true
	}
	if s.BaselineVersion < 1 {
		// A partial rollout without a baseline cannot serve the out-of-bucket
		// population; refusing is the only safe answer.
		return 0, false
	}
	if s.Percentage <= 0 {
		return s.BaselineVersion, true
	}
	if bucket < s.Percentage {
		return s.ActiveVersion, true
	}
	return s.BaselineVersion, true
}
