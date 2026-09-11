package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

type compilationResult struct {
	Agent    agentdomain.AgentVersion
	Profile  profiledomain.ProfileRevision
	Compiled domain.CompiledManifest
	Report   domain.ValidationReport
}

func (s *Service) compileAndCheck(ctx context.Context, tenantID, deploymentID, actorUserID string, input domain.DeploymentInput) (compilationResult, error) {
	if err := input.Validate(); err != nil {
		return compilationResult{}, ErrInvalidDeploymentInput
	}
	deployment, err := s.deps.Queries.GetDeployment(ctx, tenantID, deploymentID)
	if errors.Is(err, ErrDeploymentNotFound) {
		return compilationResult{}, ErrDeploymentNotFound
	}
	if err != nil {
		return compilationResult{}, fmt.Errorf("get deployment before compilation: %w", err)
	}
	if deployment.Validate() != nil || deployment.TenantID != tenantID || deployment.ID != deploymentID {
		return compilationResult{}, ErrPublicationIntegrity
	}
	agentVersion, err := s.deps.AgentVersions.GetAgentVersion(ctx, tenantID, input.Agent.AgentID, actorUserID, input.Agent.VersionNumber)
	if err != nil {
		return compilationResult{}, translateAgentError(err)
	}
	if agentVersion.TenantID != tenantID || agentVersion.AgentID != input.Agent.AgentID ||
		agentVersion.VersionNumber != input.Agent.VersionNumber || agentVersion.ID == "" || agentVersion.SpecDigest == "" {
		return compilationResult{}, ErrPublicationIntegrity
	}
	profileRevision, err := s.deps.ProfileRevisions.GetProfileRevision(ctx, tenantID, input.Profile.ProfileID, actorUserID, input.Profile.RevisionNumber)
	if err != nil {
		return compilationResult{}, translateProfileError(err)
	}
	if profileRevision.TenantID != tenantID || profileRevision.ProfileID != input.Profile.ProfileID ||
		profileRevision.RevisionNumber != input.Profile.RevisionNumber || profileRevision.ID == "" || profileRevision.SpecDigest == "" {
		return compilationResult{}, ErrPublicationIntegrity
	}
	var agentSpec agentdomain.Spec
	if err := json.Unmarshal(agentVersion.Spec, &agentSpec); err != nil {
		return compilationResult{}, ErrPublicationIntegrity
	}
	var profileSpec profiledomain.Spec
	if err := json.Unmarshal(profileRevision.Spec, &profileSpec); err != nil {
		return compilationResult{}, ErrPublicationIntegrity
	}
	requests := domain.ManagedBackendRequests(agentSpec, profileSpec)
	backends, backendDiagnostics := s.resolveManagedBackends(ctx, tenantID, actorUserID, requests)
	if len(backendDiagnostics) > 0 {
		return compilationResult{Agent: agentVersion, Profile: profileRevision, Report: domain.NewValidationReport(s.deps.Platform.CompilerVersion, s.deps.Platform.Digest, backendDiagnostics)}, nil
	}
	compiled, report := domain.Compile(domain.CompileInput{
		ManagedBackends: backends,
		TenantID:        tenantID,
		Agent: domain.AgentVersionSource{
			TenantID: agentVersion.TenantID, AgentID: agentVersion.AgentID,
			VersionID: agentVersion.ID, VersionNumber: agentVersion.VersionNumber,
			SchemaVersion: agentVersion.SchemaVersion, SpecDigest: agentVersion.SpecDigest, Spec: agentSpec,
		},
		Profile: domain.ProfileRevisionSource{
			TenantID: profileRevision.TenantID, ProfileID: profileRevision.ProfileID,
			RevisionID: profileRevision.ID, RevisionNumber: profileRevision.RevisionNumber,
			SchemaVersion: profileRevision.SchemaVersion, SpecDigest: profileRevision.SpecDigest, Spec: profileSpec,
		},
		Platform: s.deps.Platform,
	})
	result := compilationResult{Agent: agentVersion, Profile: profileRevision, Compiled: compiled, Report: report}
	if !report.Valid {
		return result, nil
	}
	uses := make([]profileapp.CredentialUse, 0, len(compiled.CredentialUses))
	for _, use := range compiled.CredentialUses {
		uses = append(uses, profileapp.CredentialUse{
			CredentialID: use.CredentialID, Purpose: use.Purpose,
			AudienceDigest: use.AudienceDigest,
		})
	}
	err = s.deps.ProfileCredentials.CheckUsable(ctx, profileapp.CheckProfileCredentialsCommand{
		TenantID: tenantID, ProfileID: profileRevision.ProfileID,
		ActorUserID: actorUserID, ProfileRevisionNumber: profileRevision.RevisionNumber,
		Uses: uses,
	})
	if err == nil {
		return result, nil
	}
	switch {
	case errors.Is(err, profileapp.ErrTenantForbidden):
		return compilationResult{}, ErrTenantForbidden
	case errors.Is(err, profileapp.ErrRuntimeProfileNotFound), errors.Is(err, profileapp.ErrProfileRevisionNotFound):
		return compilationResult{}, ErrProfileRevisionNotFound
	case errors.Is(err, profiledomain.ErrCredentialUnavailable),
		errors.Is(err, profiledomain.ErrCredentialAssociation),
		errors.Is(err, profiledomain.ErrCredentialNotFound),
		errors.Is(err, profiledomain.ErrCredentialInput):
		result.Report = report.WithDiagnostics(domain.Diagnostic{
			Code: domain.DiagnosticCredentialUnavailable, Severity: domain.SeverityError,
			Source: domain.DiagnosticSourceProfile, Path: "/credentials",
			Message: "required profile credentials are unavailable",
		})
		return result, nil
	default:
		return compilationResult{}, fmt.Errorf("%w: %v", ErrCredentialDependencyUnavailable, err)
	}
}

func (s *Service) ValidateDeploymentRevision(ctx context.Context, command ValidateDeploymentCommand) (domain.ValidationReport, error) {
	if err := s.authorizeMember(ctx, command.TenantID, command.ActorUserID); err != nil {
		return domain.ValidationReport{}, err
	}
	result, err := s.compileAndCheck(ctx, command.TenantID, command.DeploymentID, command.ActorUserID, command.Input)
	if err != nil {
		return domain.ValidationReport{}, err
	}
	return result.Report, nil
}
