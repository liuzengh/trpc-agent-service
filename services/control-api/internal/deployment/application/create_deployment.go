package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

type createReceiptResult struct {
	Deployment domain.Deployment `json:"deployment"`
}

func (s *Service) CreateDeployment(ctx context.Context, command CreateDeploymentCommand) (CreateDeploymentResult, error) {
	if err := s.authorizeMember(ctx, command.TenantID, command.ActorUserID); err != nil {
		return CreateDeploymentResult{}, err
	}
	if !validIdempotencyKey(command.IdempotencyKey) {
		return CreateDeploymentResult{}, ErrInvalidDeploymentInput
	}
	// Normalize and validate without allowing generated envelope fields to affect
	// the request digest.
	prototype, err := domain.NewDeployment("prototype", command.TenantID, command.Name, command.Description, command.ActorUserID, s.deps.Now())
	if err != nil {
		return CreateDeploymentResult{}, ErrInvalidDeployment
	}
	digest, err := requestDigest("create", command.TenantID, command.TenantID, struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}{prototype.Name, prototype.Description})
	if err != nil {
		return CreateDeploymentResult{}, err
	}
	key := CommandReceiptKey{TenantID: command.TenantID, Operation: "create", ScopeID: command.TenantID, KeyHash: keyHash(command.IdempotencyKey)}
	if receipt, found, err := s.deps.Publications.FindCommandReceipt(ctx, key); err != nil {
		return CreateDeploymentResult{}, fmt.Errorf("find create receipt: %w", err)
	} else if found {
		return s.replayCreate(ctx, receipt, key, digest)
	}
	id, err := s.deps.NewDeploymentID()
	if err != nil {
		return CreateDeploymentResult{}, fmt.Errorf("generate deployment id: %w", err)
	}
	created, err := domain.NewDeployment(id, command.TenantID, command.Name, command.Description, command.ActorUserID, s.deps.Now())
	if err != nil {
		return CreateDeploymentResult{}, ErrInvalidDeployment
	}
	resultJSON, err := json.Marshal(createReceiptResult{Deployment: created})
	if err != nil {
		return CreateDeploymentResult{}, fmt.Errorf("encode create receipt: %w", err)
	}
	receipt := domain.CommandReceipt{
		TenantID: command.TenantID, Operation: "create", ScopeID: command.TenantID,
		KeyHash: key.KeyHash, RequestDigest: digest, DeploymentID: created.ID,
		Result: resultJSON, CreatedBy: command.ActorUserID, CreatedAt: created.CreatedAt,
	}
	committed, err := s.deps.Publications.CreateDeployment(ctx, CreateCommit{Deployment: created, Receipt: receipt})
	if err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			return CreateDeploymentResult{}, err
		}
		return CreateDeploymentResult{}, fmt.Errorf("create deployment: %w", err)
	}
	replayed, err := s.replayCreate(ctx, committed.Receipt, key, digest)
	if err != nil {
		return CreateDeploymentResult{}, err
	}
	replayed.Created = committed.Created
	return replayed, nil
}

func (s *Service) replayCreate(ctx context.Context, receipt domain.CommandReceipt, key CommandReceiptKey, digest string) (CreateDeploymentResult, error) {
	if err := verifyReceipt(receipt, key, digest); err != nil {
		return CreateDeploymentResult{}, err
	}
	var stored createReceiptResult
	if err := json.Unmarshal(receipt.Result, &stored); err != nil || !closedJSONMatches(receipt.Result, stored) ||
		stored.Deployment.Validate() != nil ||
		stored.Deployment.ID != receipt.DeploymentID || stored.Deployment.TenantID != receipt.TenantID {
		return CreateDeploymentResult{}, ErrPublicationIntegrity
	}
	current, err := s.deps.Queries.GetDeployment(ctx, receipt.TenantID, receipt.DeploymentID)
	if err != nil || current.Validate() != nil || current.ID != stored.Deployment.ID ||
		current.TenantID != stored.Deployment.TenantID ||
		current.CreatedBy != stored.Deployment.CreatedBy ||
		!current.CreatedAt.Equal(stored.Deployment.CreatedAt) {
		return CreateDeploymentResult{}, ErrPublicationIntegrity
	}
	return CreateDeploymentResult{Deployment: stored.Deployment}, nil
}
