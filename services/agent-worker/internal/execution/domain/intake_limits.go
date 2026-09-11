package domain

// IntakeLimits are explicit deployment capacities for new Run identities.
// MaxQueuedRuns counts nonterminal work; MaxRetainedRuns counts every retained
// Run, including completed history. Neither changes a Run's frozen model or
// retry policy. Existing receipts and Run recovery bypass new-identity limits.
type IntakeLimits struct {
	MaxQueuedRuns   int
	MaxRetainedRuns int
}

func (l IntakeLimits) Validate() error {
	if l.MaxQueuedRuns < 1 || l.MaxRetainedRuns < 1 {
		return ErrInvalid
	}
	return nil
}
