package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/httpdlp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

const dlpSecretResourceID = "http-dlp"

func newDLPAuthorizer(provider secrets.Provider, backendVersion int64, ref secrets.SecretRef) (httpdlp.Authorizer, error) {
	if provider == nil || secrets.ValidateRequest(dlpSecretScope("probe", backendVersion), ref) != nil {
		return nil, runtime.ErrInvariantViolation
	}
	return func(ctx context.Context, tenantID string, request *http.Request) error {
		if strings.TrimSpace(tenantID) == "" || request == nil {
			return runtime.ErrTenantScope
		}
		value, err := provider.Resolve(ctx, dlpSecretScope(tenantID, backendVersion), ref)
		if err != nil {
			return err
		}
		defer clear(value.Bytes)
		if value.Version != ref.Version || len(value.Bytes) == 0 {
			return runtime.ErrVersionMismatch
		}
		request.Header.Set("Authorization", "Bearer "+string(value.Bytes))
		request.Header.Set("X-Tenant-ID", tenantID)
		return nil
	}, nil
}

func dlpSecretScope(tenantID string, backendVersion int64) secrets.Scope {
	return secrets.Scope{TenantID: tenantID, Subject: tenantID, Purpose: secrets.PurposeBackendConnect,
		ResourceID: dlpSecretResourceID, ResourceVersion: backendVersion}
}
