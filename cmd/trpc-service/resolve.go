// Operator tooling for the tool ledger and blocked sessions, as one-shot
// commands — the same shape as -migrate and -bootstrap-admin, and for the
// same reason: a deployment that runs roles has no process serving the admin
// HTTP surface, and "a human disposes of an unknown" cannot depend on one.
// The HTTP surface (admin/v2/tool-calls) calls exactly the same services.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// runListBlocked prints every session parked for human review, with the
// unresolved calls that hold it. This is the "unknown 查询" side of the
// approved plan's admin requirement.
func runListBlocked(cfg *config.Config, tenantID string) int {
	cdp, raw, err := openControlPlane(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list-blocked: %v\n", err)
		return 1
	}
	defer raw.Close()

	ctx := context.Background()
	journal := tool.NewJournal(cdp)
	blocked, err := journal.BlockedSessions(ctx, tenantID, 100)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list-blocked: %v\n", err)
		return 1
	}
	out := make([]map[string]any, 0, len(blocked))
	for _, b := range blocked {
		calls, err := journal.List(ctx, tenantID, tool.CallFilter{
			ExecutionID: b.ExecutionID, UnresolvedOnly: true, Limit: 50,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "list-blocked: %v\n", err)
			return 1
		}
		callsJSON := make([]map[string]any, 0, len(calls))
		for _, c := range calls {
			callsJSON = append(callsJSON, map[string]any{
				"call_id": c.CallID, "tool_name": c.ToolName, "status": c.Status,
				"side_effect": c.SideEffect, "error_type": c.ErrorType, "detail": c.Detail,
			})
		}
		out = append(out, map[string]any{
			"session_pk":     b.SessionPK,
			"blocked_reason": b.BlockedReason,
			"execution_id":   b.ExecutionID,
			"unresolved":     callsJSON,
		})
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
	return 0
}

// runResolveSession is the disposition: resolve every unresolved call of the
// session's head execution, then let execution.TryUnblock decide — cancelled
// requeues the message for a re-run, confirmed retires it with a receipt.
func runResolveSession(cfg *config.Config, tenantID string, sessionPK int64, resolution, by string) int {
	if resolution != "confirmed" && resolution != "cancelled" {
		fmt.Fprintln(os.Stderr, "resolve-session: -resolution must be confirmed or cancelled")
		return 1
	}
	cdp, raw, err := openControlPlane(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve-session: %v\n", err)
		return 1
	}
	defer raw.Close()

	ctx := context.Background()
	journal := tool.NewJournal(cdp)

	var executionID string
	row, err := cdp.MustScope(tenantID).QueryRow(ctx, `
		SELECT im.execution_id
		FROM sessions s
		JOIN inbox_messages im
		  ON im.tenant_id = s.tenant_id AND im.session_pk = s.session_pk AND im.in_seq = s.head_seq
		WHERE s.tenant_id = ? AND s.session_pk = ?`, tenantID, sessionPK)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve-session: %v\n", err)
		return 1
	}
	if err := row.Scan(&executionID); err != nil {
		fmt.Fprintf(os.Stderr, "resolve-session: no head message for session %d: %v\n", sessionPK, err)
		return 1
	}

	calls, err := journal.List(ctx, tenantID, tool.CallFilter{
		ExecutionID: executionID, UnresolvedOnly: true, Limit: 50,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve-session: %v\n", err)
		return 1
	}
	for _, c := range calls {
		if _, err := journal.Resolve(ctx, tenantID, c.CallID, resolution, by); err != nil {
			if errors.Is(err, tool.ErrAlreadyResolved) {
				continue
			}
			fmt.Fprintf(os.Stderr, "resolve-session: resolve %s: %v\n", c.CallID, err)
			return 1
		}
	}

	svc := execution.NewService(cdp, 0)
	outcome, err := svc.TryUnblock(ctx, tenantID, sessionPK)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve-session: %v\n", err)
		return 1
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(map[string]any{
		"resolved_calls":    len(calls),
		"unblocked":         outcome.Unblocked,
		"remaining":         outcome.Remaining,
		"disposition":       string(outcome.Disposition),
		"head_execution_id": outcome.HeadExecutionID,
	})
	return 0
}

// openControlPlane is the shared guard: every one-shot command that talks to
// the durable plane needs the same config check and the same pool. The raw
// handle is returned alongside so the caller closes exactly what it opened.
func openControlPlane(cfg *config.Config) (*controlplane.DB, *sql.DB, error) {
	if cfg.ControlPlane.Mode != config.ControlPlaneMySQL {
		return nil, nil, errors.New("control_plane.mode must be mysql for this command")
	}
	db, err := tasmysql.Open(context.Background(), cfg.ControlPlane.MySQLDSN)
	if err != nil {
		return nil, nil, err
	}
	return controlplane.NewDB(db), db, nil
}
