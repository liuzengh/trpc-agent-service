package pipeline

import (
	"fmt"
	"strconv"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

// validateDerivedOwner checks the actual lease-owner values Run will submit,
// including the worker-index suffix. Validating only the configured prefix can
// let a process start and fail every claim once the suffix is appended.
func validateDerivedOwner(owner string, concurrency int) error {
	if err := reliable.ValidateLeaseOwner(owner); err != nil {
		return err
	}
	if concurrency <= 0 {
		concurrency = 4
	}
	derived := owner + "-" + strconv.Itoa(concurrency-1)
	if err := reliable.ValidateLeaseOwner(derived); err != nil {
		return fmt.Errorf("derived lease owner: %w", err)
	}
	return nil
}
