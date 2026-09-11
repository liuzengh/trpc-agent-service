// Package executionhttp queries the current Execution owner using authenticated
// TLS. It performs one request; credential initialization retries use a new Attempt.
package executionhttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
)

type Verifier struct {
	client *http.Client
	url    string
}

func New(client *http.Client, endpoint string) (*Verifier, error) {
	u, err := url.Parse(endpoint)
	if client == nil || client.Timeout <= 0 || client.Timeout > 15*time.Second || err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("execution verifier configuration is invalid")
	}
	owned := *client
	owned.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	u.Path = executionv1.AttemptVerifyPath
	return &Verifier{&owned, u.String()}, nil
}
func (v *Verifier) VerifyAttempt(ctx context.Context, r application.ExecutionAuthorizationRequest) (application.ExecutionAuthorization, error) {
	raw, err := executionv1.EncodeAttemptRequest(executionv1.AttemptRequest{WorkloadIdentity: r.WorkloadIdentity, ExecutionToken: r.ExecutionToken, ManifestID: r.ManifestID, ManifestDigest: r.ManifestDigest})
	if err != nil {
		return application.ExecutionAuthorization{}, application.ErrExecutionUnauthorized
	}
	defer clear(raw)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.url, bytes.NewReader(raw))
	if err != nil {
		return application.ExecutionAuthorization{}, application.ErrExecutionDependencyUnavailable
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return application.ExecutionAuthorization{}, application.ErrExecutionDependencyUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden {
		return application.ExecutionAuthorization{}, application.ErrExecutionUnauthorized
	}
	if resp.StatusCode != http.StatusOK {
		return application.ExecutionAuthorization{}, application.ErrExecutionDependencyUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, executionv1.MaxAttemptProofBytes+1))
	if err != nil {
		return application.ExecutionAuthorization{}, application.ErrExecutionDependencyUnavailable
	}
	defer clear(body)
	proof, err := executionv1.DecodeAttemptResponse(body)
	if err != nil {
		return application.ExecutionAuthorization{}, application.ErrExecutionDependencyUnavailable
	}
	if proof.WorkerID != r.WorkloadIdentity || proof.ManifestID != r.ManifestID || proof.ManifestDigest != r.ManifestDigest {
		return application.ExecutionAuthorization{}, application.ErrExecutionUnauthorized
	}
	uses := make([]application.CredentialUse, len(proof.AllowedUses))
	for i, u := range proof.AllowedUses {
		uses[i] = application.CredentialUse{CredentialID: u.CredentialID, Purpose: u.Purpose, AudienceDigest: u.AudienceDigest}
	}
	return application.ExecutionAuthorization{TenantID: proof.TenantID, ProfileID: proof.ProfileID, ProfileRevisionNumber: proof.ProfileRevisionNumber, RunID: proof.RunID, AttemptID: proof.AttemptID, WorkerID: proof.WorkerID, LeaseEpoch: proof.LeaseEpoch, ExpiresAt: proof.ExpiresAt, ManifestID: proof.ManifestID, ManifestDigest: proof.ManifestDigest, AllowedUses: uses}, nil
}

var _ application.ExecutionAuthorizationVerifier = (*Verifier)(nil)
