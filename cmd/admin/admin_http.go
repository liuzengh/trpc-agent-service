//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func listTenants(w http.ResponseWriter, r *http.Request, service tenant.Service) {
	principal, err := adminauth.PrincipalFromContext(r.Context())
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	var tenants []*tenant.Tenant
	if principal.Role == adminauth.RolePlatformAdmin {
		tenants, err = service.ListTenants(r.Context())
	} else {
		// Scoped principals must never trigger a broad read that decrypts every
		// tenant before the HTTP layer filters the result. Fail closed when the
		// repository does not provide the SQL-constrained capability.
		ids := principal.TenantIDs()
		if len(ids) == 0 {
			tenants = []*tenant.Tenant{}
		} else if scoped, ok := service.(interface {
			ListTenantsForIDs(context.Context, []string) ([]*tenant.Tenant, error)
		}); ok {
			ordered := make([]string, 0, len(ids))
			for id := range ids {
				ordered = append(ordered, id)
			}
			sort.Strings(ordered)
			tenants, err = scoped.ListTenantsForIDs(r.Context(), ordered)
		} else {
			err = tenant.ErrScopedTenantListingUnsupported
		}
	}
	if err != nil {
		log.Printf("failed to list tenants for principal %s: error=%s", principal.ID, telemetry.StableErrorCode(err))
		http.Error(w, "Tenant listing unavailable", http.StatusServiceUnavailable)
		return
	}

	redacted := make([]*tenant.Tenant, 0, len(tenants))
	for _, tn := range tenants {
		if principal.AllowsTenant(tn.ID) {
			redacted = append(redacted, redactTenant(tn))
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(redacted)
}

func createTenant(w http.ResponseWriter, r *http.Request, service tenant.Service) {
	actor, ok := requireAdminActor(w, r)
	if !ok {
		return
	}
	var req struct {
		Name   string              `json:"name"`
		Config tenant.TenantConfig `json:"config"`
	}

	if !decodeJSON(w, r, &req) {
		return
	}
	if tenantConfigContainsRedacted(req.Config) {
		http.Error(w, "redaction placeholders are not valid credentials for a new tenant", http.StatusBadRequest)
		return
	}
	if err := tenant.ValidateDistributedStorage(req.Config.Storage); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := tenant.ContextWithAuditActor(r.Context(), actor)
	t, err := service.CreateTenant(ctx, req.Name, req.Config)
	if err != nil {
		if errors.Is(err, tenant.ErrInvalidTenantConfig) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if errors.Is(err, tenant.ErrTenantAlreadyExists) {
			http.Error(w, "Tenant already exists", http.StatusConflict)
			return
		}
		log.Printf("failed to create tenant: error=%s", telemetry.StableErrorCode(err))
		http.Error(w, "Failed to create tenant", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(redactTenant(t))
}

func getTenant(w http.ResponseWriter, r *http.Request, service tenant.Service, tenantID string) {
	if _, err := adminauth.RequireTenant(r.Context(), tenantID); err != nil {
		writeAdminAuthorizationError(w, err)
		return
	}
	t, err := service.GetTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			http.Error(w, "Tenant not found", http.StatusNotFound)
		} else {
			log.Printf("failed to get tenant: error=%s", telemetry.StableErrorCode(err))
			http.Error(w, "Failed to get tenant", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(redactTenant(t))
}

func updateTenant(w http.ResponseWriter, r *http.Request, service tenant.Service, tenantID string) {
	if _, err := adminauth.RequireTenant(r.Context(), tenantID); err != nil {
		writeAdminAuthorizationError(w, err)
		return
	}
	actor, ok := requireAdminActor(w, r)
	if !ok {
		return
	}
	var t tenant.Tenant
	if !decodeJSON(w, r, &t) {
		return
	}

	// Admin responses intentionally mask credentials. A client commonly edits
	// that response and PUTs it back, so treating the mask as a new credential
	// would permanently destroy the real secret. Merge only masked/omitted
	// credentials from the current plaintext snapshot; an explicit non-mask
	// value still rotates the credential.
	current, err := service.GetTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			http.Error(w, "Tenant not found", http.StatusNotFound)
		} else {
			log.Printf("failed to load tenant before update: error=%s", telemetry.StableErrorCode(err))
			http.Error(w, "Failed to update tenant", http.StatusInternalServerError)
		}
		return
	}
	preserveMaskedSecrets(current, &t)
	if tenantContainsRedacted(&t) {
		http.Error(w, "redaction placeholder does not match an existing credential", http.StatusBadRequest)
		return
	}
	if err := tenant.ValidateDistributedStorage(t.Storage); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t.ID = tenantID
	ctx := tenant.ContextWithAuditActor(r.Context(), actor)
	if err := service.UpdateTenant(ctx, &t); err != nil {
		if errors.Is(err, tenant.ErrTenantConflict) {
			http.Error(w, "Tenant configuration changed; reload and retry", http.StatusConflict)
			return
		}
		if errors.Is(err, tenant.ErrInvalidTenantConfig) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("failed to update tenant: error=%s", telemetry.StableErrorCode(err))
		http.Error(w, "Failed to update tenant", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(redactTenant(&t))
}

func deleteTenant(w http.ResponseWriter, r *http.Request, service tenant.Service, tenantID string) {
	if _, err := adminauth.RequireTenant(r.Context(), tenantID); err != nil {
		writeAdminAuthorizationError(w, err)
		return
	}
	actor, ok := requireAdminActor(w, r)
	if !ok {
		return
	}
	ctx := tenant.ContextWithAuditActor(r.Context(), actor)
	if err := service.DeleteTenant(ctx, tenantID); err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			http.Error(w, "Tenant not found", http.StatusNotFound)
		} else {
			log.Printf("failed to delete tenant: error=%s", telemetry.StableErrorCode(err))
			http.Error(w, "Failed to delete tenant", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
