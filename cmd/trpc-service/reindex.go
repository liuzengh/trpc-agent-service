package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// runReindexKB resets every ready document belonging to one knowledge base
// to 'uploaded' and triggers re-indexing. The operator supplies the KB's
// public id and the tenant; the CLI opens the same stores the jobs role
// reads from.
func runReindexKB(cfg *config.Config, tenantID, kbPublicID string) int {
	if cfg.ControlPlane.Mode != config.ControlPlaneMySQL {
		fmt.Fprintln(os.Stderr, "reindex-kb: control_plane.mode must be mysql")
		return 1
	}
	if !cfg.Knowledge.Enabled() {
		fmt.Fprintln(os.Stderr, "reindex-kb: knowledge is not configured")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, err := tasmysql.Open(ctx, cfg.ControlPlane.MySQLDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reindex-kb: open: %v\n", err)
		return 1
	}
	defer db.Close()
	cdp := controlplane.NewDB(db)

	parts, _, err := buildKnowledgeStack(cfg, cdp, secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: []string{"MODEL_API_KEY"}}))
	if err != nil {
		fmt.Fprintf(os.Stderr, "reindex-kb: build knowledge stack: %v\n", err)
		return 1
	}
	scope, err := cdp.Scope(tenantID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reindex-kb: %v\n", err)
		return 1
	}

	// Resolve the KB public id to an internal id.
	var kbID int64
	row, err := scope.QueryRow(ctx,
		"SELECT kb_id FROM knowledge_bases WHERE tenant_id=? AND public_id=?", tenantID, kbPublicID)
	if err != nil {
		return 1
	}
	if err := row.Scan(&kbID); err != nil {
		fmt.Fprintf(os.Stderr, "reindex-kb: resolve kb %q: %v\n", kbPublicID, err)
		return 1
	}

	// Collect ready documents.
	rows, err := scope.Query(ctx,
		"SELECT doc_id FROM documents WHERE tenant_id=? AND kb_id=? AND status='ready'",
		tenantID, kbID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reindex-kb: list docs: %v\n", err)
		return 1
	}
	var docIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			fmt.Fprintf(os.Stderr, "reindex-kb: scan doc: %v\n", err)
			return 1
		}
		docIDs = append(docIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "reindex-kb: rows: %v\n", err)
		return 1
	}

	fmt.Printf("reindex-kb: %d document(s) to re-index for kb %q\n", len(docIDs), kbPublicID)
	for _, id := range docIDs {
		// Reset to uploaded and call the index handler directly.
		if _, err := scope.Exec(ctx,
			"UPDATE documents SET status='uploaded' WHERE tenant_id=? AND doc_id=? AND status='ready'",
			tenantID, id); err != nil {
			fmt.Fprintf(os.Stderr, "reindex-kb: reset doc %d: %v\n", id, err)
			continue
		}
		if err := parts.knowledge.HandleJob(ctx, tenantID, "doc_index",
			[]byte(fmt.Sprintf(`{"doc_id":%d,"generation":0}`, id))); err != nil {
			fmt.Fprintf(os.Stderr, "reindex-kb: index doc %d: %v\n", id, err)
			continue
		}
		fmt.Printf("  doc %d re-indexed\n", id)
	}
	fmt.Println("reindex-kb: done")
	return 0
}
