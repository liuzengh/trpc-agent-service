package runtimehttp

import "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"

type resolvedCredentialResponse struct {
	CredentialID       string `json:"credential_id"`
	Purpose            string `json:"purpose"`
	AudienceDigest     string `json:"audience_digest"`
	CredentialRevision int64  `json:"credential_revision"`
	Value              string `json:"value"`
}

type resolveResponse struct {
	TenantID              string                       `json:"tenant_id"`
	ProfileID             string                       `json:"profile_id"`
	ProfileRevisionNumber int64                        `json:"profile_revision_number"`
	RunID                 string                       `json:"run_id"`
	AttemptID             string                       `json:"attempt_id"`
	WorkerID              string                       `json:"worker_id"`
	LeaseEpoch            int64                        `json:"lease_epoch"`
	ManifestID            string                       `json:"manifest_id"`
	ManifestDigest        string                       `json:"manifest_digest"`
	Credentials           []resolvedCredentialResponse `json:"credentials"`
}

func batchResponse(batch application.CredentialBatch) resolveResponse {
	values := make([]resolvedCredentialResponse, 0, len(batch.Credentials))
	for _, credential := range batch.Credentials {
		values = append(values, resolvedCredentialResponse{CredentialID: credential.Use.CredentialID, Purpose: credential.Use.Purpose, AudienceDigest: credential.Use.AudienceDigest, CredentialRevision: credential.CredentialRevision, Value: string(credential.Value)})
	}
	return resolveResponse{TenantID: batch.TenantID, ProfileID: batch.ProfileID, ProfileRevisionNumber: batch.ProfileRevisionNumber, RunID: batch.RunID, AttemptID: batch.AttemptID, WorkerID: batch.WorkerID, LeaseEpoch: batch.LeaseEpoch, ManifestID: batch.ManifestID, ManifestDigest: batch.ManifestDigest, Credentials: values}
}
