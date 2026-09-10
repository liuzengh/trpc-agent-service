package postgres

// This file defines the official trpc-agent-go session/postgres reuse
// boundary. The platform's multi-tenant session dimension is encoded into the
// SDK's app_name field (the official table key is app_name + user_id +
// session_id and carries no tenant/agent-app column), and every read is
// guarded by a fail-closed tenant hook. The official service owns the raw
// event/state persistence; the platform coordination contract (fence /
// version / outbox) stays in a service-owned coordination table (see
// docs/design/6.module-boundaries.md §8.1).

import (
	"fmt"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
)

// TenantAppSep separates tenantID from agentAppID inside the SDK app_name
// dimension.
const TenantAppSep = "/"

// TenantKey encodes the platform multi-tenant dimension into the official
// session.Key. userID/sessionID map 1:1 onto the SDK fields; tenantID +
// agentAppID collapse into app_name.
func TenantKey(tenantID, agentAppID, userID, sessionID string) session.Key {
	return session.Key{
		AppName:   tenantID + TenantAppSep + agentAppID,
		UserID:    userID,
		SessionID: sessionID,
	}
}

// ParseTenantApp decodes tenantID / agentAppID from an official app_name.
// It returns ok=false for any malformed value (empty segment, missing
// separator, or an extra separator); callers must fail closed on ok=false.
func ParseTenantApp(appName string) (tenantID, agentAppID string, ok bool) {
	i := strings.Index(appName, TenantAppSep)
	if i <= 0 || i == len(appName)-1 {
		return "", "", false
	}
	rest := appName[i+1:]
	if strings.Contains(rest, TenantAppSep) {
		return "", "", false
	}
	return appName[:i], rest, true
}

// TenantGetSessionHook returns a GetSessionHook that only admits reads whose
// app_name exactly matches the tenant/agent-app scope, failing closed on any
// other key. This enforces cross-tenant read isolation on top of the official
// backend, which has no native tenant column.
func TenantGetSessionHook(tenantID, agentAppID string) session.GetSessionHook {
	want := tenantID + TenantAppSep + agentAppID
	return func(ctx *session.GetSessionContext, next func() (*session.Session, error)) (*session.Session, error) {
		if ctx.Key.AppName != want {
			return nil, fmt.Errorf("session tenant guard: app_name %q outside scope %q", ctx.Key.AppName, want)
		}
		return next()
	}
}

// NewOfficialSessionService builds the shared official session/postgres
// backend used by a Worker role. Tenant scope is enforced by the per-turn
// facade, not by a process-wide static hook: one Worker legitimately serves
// multiple tenant/app pairs. DB init is skipped so table creation stays under
// the platform migration authority rather than the SDK's implicit DDL.
func NewOfficialSessionService(dsn string) (*sessionpostgres.Service, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("official session postgres dsn is empty")
	}
	svc, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(dsn),
		sessionpostgres.WithSkipDBInit(true),
		sessionpostgres.WithEnableAsyncPersist(false),
	)
	if err != nil {
		return nil, fmt.Errorf("new official session postgres service: %w", err)
	}
	return svc, nil
}

// NewTenantScopedOfficialSessionService is retained for single-scope callers
// such as narrowly scoped maintenance jobs. Multi-tenant process roles must
// use NewOfficialSessionService and the checked BufferedTurn facade instead.
func NewTenantScopedOfficialSessionService(tenantID, agentAppID, dsn string) (*sessionpostgres.Service, error) {
	svc, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(dsn),
		sessionpostgres.WithSkipDBInit(true),
		sessionpostgres.WithGetSessionHook(TenantGetSessionHook(tenantID, agentAppID)),
	)
	if err != nil {
		return nil, fmt.Errorf("new official session postgres service: %w", err)
	}
	return svc, nil
}
