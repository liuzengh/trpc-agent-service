//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"fmt"
	"time"
)

const (
	// ExecutionContractVersion is the Consumer-to-Worker execution budget
	// protocol carried in the signed request body.
	ExecutionContractVersion = 1
	// ExecutionContractVersionHeader marks responses produced by a Worker that
	// understood the versioned execution contract. Clients use it to distinguish
	// an old Worker's route-level 404 from a real application 404.
	ExecutionContractVersionHeader = "X-TRPC-Agent-Execution-Contract-Version"
	// ExecutionContractProcessPath isolates the budget-aware protocol from the
	// legacy endpoint. An older Worker cannot route this path and therefore
	// cannot execute a request whose contract it does not understand.
	ExecutionContractProcessPath = "/v1/process"
)

// ExecutionContract is a signed per-request assertion of the Worker execution
// timeout expected by the Consumer. Timeout uses time.Duration's canonical
// text form so no precision is lost when operators use sub-second units.
type ExecutionContract struct {
	Version int    `json:"version"`
	Timeout string `json:"timeout"`
}

// NewExecutionContract creates the canonical contract for one configured
// Worker timeout.
func NewExecutionContract(timeout time.Duration) (ExecutionContract, error) {
	if err := ValidateExecutionTimeout(timeout); err != nil {
		return ExecutionContract{}, fmt.Errorf("%w: %v", ErrWorkerExecutionBudgetInvalid, err)
	}
	return ExecutionContract{
		Version: ExecutionContractVersion,
		Timeout: timeout.String(),
	}, nil
}

// ValidateExecutionContract rejects missing, unknown, non-canonical or
// configuration-drifted contracts before any tenant/control-plane work starts.
func ValidateExecutionContract(contract *ExecutionContract, configured time.Duration) error {
	if err := ValidateExecutionTimeout(configured); err != nil {
		return fmt.Errorf("%w: configured timeout: %v", ErrWorkerExecutionBudgetInvalid, err)
	}
	if contract == nil {
		return fmt.Errorf("%w: execution contract is required", ErrWorkerExecutionBudgetInvalid)
	}
	if contract.Version != ExecutionContractVersion {
		return fmt.Errorf(
			"%w: execution contract version %d is unsupported",
			ErrWorkerExecutionBudgetInvalid,
			contract.Version,
		)
	}
	parsed, err := time.ParseDuration(contract.Timeout)
	if err != nil || parsed.String() != contract.Timeout {
		return fmt.Errorf("%w: execution contract timeout is not canonical", ErrWorkerExecutionBudgetInvalid)
	}
	if err := ValidateExecutionTimeout(parsed); err != nil {
		return fmt.Errorf("%w: execution contract timeout is unsafe", ErrWorkerExecutionBudgetInvalid)
	}
	if parsed != configured {
		return fmt.Errorf(
			"%w: execution contract timeout %s does not match Worker timeout %s",
			ErrWorkerExecutionBudgetInvalid,
			parsed,
			configured,
		)
	}
	return nil
}
