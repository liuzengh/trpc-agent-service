package executionhttp

import (
	"bytes"
	"context"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"io"
	"net/http"
	"net/url"
)

func (v *Verifier) VerifyFinal(ctx context.Context, in executionv1.FinalRequest) (executionv1.FinalResponse, error) {
	raw, e := executionv1.EncodeFinalRequest(in)
	if e != nil {
		return executionv1.FinalResponse{}, application.ErrExecutionUnauthorized
	}
	u, _ := url.Parse(v.url)
	u.Path = executionv1.FinalVerifyPath
	req, e := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(raw))
	if e != nil {
		return executionv1.FinalResponse{}, application.ErrExecutionDependencyUnavailable
	}
	req.Header.Set("Content-Type", "application/json")
	resp, e := v.client.Do(req)
	if e != nil {
		return executionv1.FinalResponse{}, application.ErrExecutionDependencyUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode == 403 || resp.StatusCode == 404 {
		return executionv1.FinalResponse{}, application.ErrExecutionUnauthorized
	}
	if resp.StatusCode != 200 {
		return executionv1.FinalResponse{}, application.ErrExecutionDependencyUnavailable
	}
	body, e := io.ReadAll(io.LimitReader(resp.Body, executionv1.MaxFinalProofBytes+1))
	if e != nil {
		return executionv1.FinalResponse{}, application.ErrExecutionDependencyUnavailable
	}
	proof, e := executionv1.DecodeFinalResponse(body)
	if e != nil {
		return executionv1.FinalResponse{}, application.ErrExecutionDependencyUnavailable
	}
	if proof.FinalRequest != in {
		return executionv1.FinalResponse{}, application.ErrExecutionUnauthorized
	}
	return proof, nil
}
