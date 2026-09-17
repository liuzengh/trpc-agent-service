// Package httpdlp implements a strict tenant-scoped DLP scanner adapter.
package httpdlp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Authorizer func(context.Context, string, *http.Request) error

type Scanner struct {
	Endpoint      string
	Client        *http.Client
	Authorize     Authorizer
	ProbeTenantID string
	Timeout       time.Duration
	MaxBytes      int
	AllowInsecure bool
}

type scanRequest struct {
	TenantID  string `json:"tenant_id"`
	MediaType string `json:"media_type"`
	Content   []byte `json:"content"`
}

type scanResponse struct {
	Verdict       preprocess.ScanVerdict `json:"verdict"`
	PolicyVersion string                 `json:"policy_version"`
}

func (s Scanner) Probe(ctx context.Context) error {
	endpoint, err := s.endpoint()
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, s.timeout())
	defer cancel()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, endpoint.ResolveReference(&url.URL{Path: "healthz"}).String(), nil)
	if err != nil {
		return runtime.ErrInvalidEnvelope
	}
	if s.Authorize == nil || strings.TrimSpace(s.ProbeTenantID) == "" {
		return runtime.ErrCapabilityUnsupported
	}
	if err := s.Authorize(probeCtx, s.ProbeTenantID, request); err != nil {
		return err
	}
	response, err := s.client().Do(request)
	if err != nil {
		return runtime.ErrBackendUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return runtime.ErrBackendUnavailable
	}
	return nil
}

func (s Scanner) ScanMediaInput(ctx context.Context, tenantID string, content []byte, mediaType string) (preprocess.ScanResult, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(mediaType) == "" || len(content) == 0 || len(content) > s.maximum() {
		return preprocess.ScanResult{}, runtime.ErrInvalidEnvelope
	}
	endpoint, err := s.endpoint()
	if err != nil {
		return preprocess.ScanResult{}, err
	}
	body, err := json.Marshal(scanRequest{TenantID: tenantID, MediaType: mediaType, Content: content})
	if err != nil {
		return preprocess.ScanResult{}, runtime.ErrInvariantViolation
	}
	requestCtx, cancel := context.WithTimeout(ctx, s.timeout())
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint.ResolveReference(&url.URL{Path: "v1/media:scan"}).String(), bytes.NewReader(body))
	if err != nil {
		return preprocess.ScanResult{}, runtime.ErrInvalidEnvelope
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	if s.Authorize == nil {
		return preprocess.ScanResult{}, runtime.ErrCapabilityUnsupported
	}
	if err := s.Authorize(requestCtx, tenantID, request); err != nil {
		return preprocess.ScanResult{}, err
	}
	response, err := s.client().Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return preprocess.ScanResult{}, ctx.Err()
		}
		return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 16<<10))
	decoder.DisallowUnknownFields()
	var value scanResponse
	if err := decoder.Decode(&value); err != nil {
		return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
	}
	if strings.TrimSpace(value.PolicyVersion) == "" ||
		(value.Verdict != preprocess.ScanClean && value.Verdict != preprocess.ScanRejected && value.Verdict != preprocess.ScanUnknown) {
		return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
	}
	result := preprocess.ScanResult{Verdict: value.Verdict, Version: "dlp:" + value.PolicyVersion}
	if value.Verdict == preprocess.ScanUnknown {
		return result, runtime.ErrBackendUnavailable
	}
	return result, nil
}

func (s Scanner) endpoint() (*url.URL, error) {
	value, err := url.Parse(s.Endpoint)
	if err != nil || value.Host == "" || value.User != nil || value.Fragment != "" ||
		(value.Scheme != "https" && !(s.AllowInsecure && value.Scheme == "http")) {
		return nil, runtime.ErrInvalidEnvelope
	}
	if !strings.HasSuffix(value.Path, "/") {
		value.Path += "/"
	}
	return value, nil
}

func (s Scanner) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return http.DefaultClient
}

func (s Scanner) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return 10 * time.Second
}

func (s Scanner) maximum() int {
	if s.MaxBytes > 0 {
		return s.MaxBytes
	}
	return 16 << 20
}

var _ preprocess.InputDLPScanner = Scanner{}
