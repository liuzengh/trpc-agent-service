// One-shot knowledge operator commands, the deployment-time shape of the
// upload and download paths.
//
// The HTTP upload surface belongs to the gateway role (not mounted yet, same
// slice as the callback mount); until it lands, an operator ingests
// documents through this command, which calls exactly the same service the
// HTTP handler will call. Downloads deliberately do NOT get a presigned URL
// anywhere: this command performs the same per-request authorization the
// admin route does (tenant scope, ready status) and prints or writes the
// bytes itself.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

// runIngestDoc uploads one file into a knowledge base.
func runIngestDoc(cfg *config.Config, tenantID, appPublicID, kbPublicID, docPublicID, title, mime, path string) int {
	cdp, raw, err := openControlPlane(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest-doc: %v\n", err)
		return 1
	}
	defer raw.Close()
	ctx := context.Background()

	resolver := secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: allowedSecretEnv()})
	stack, ok, err := buildKnowledgeStack(cfg, cdp, resolver)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest-doc: %v\n", err)
		return 1
	}
	if !ok {
		fmt.Fprintln(os.Stderr, "ingest-doc: the knowledge section is not configured")
		return 1
	}
	scope := cdp.MustScope(tenantID)
	var appID int64
	row, err := scope.QueryRow(ctx, "SELECT app_id FROM agent_apps WHERE tenant_id = ? AND public_id = ?", tenantID, appPublicID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest-doc: %v\n", err)
		return 1
	}
	if err := row.Scan(&appID); err != nil {
		fmt.Fprintf(os.Stderr, "ingest-doc: no app %q in tenant %q: %v\n", appPublicID, tenantID, err)
		return 1
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest-doc: %v\n", err)
		return 1
	}
	if title == "" {
		title = filepath.Base(path)
	}
	doc, err := stack.knowledge.IngestDoc(ctx, tenantID, appID, kbPublicID, docPublicID, title, mime, data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest-doc: %v\n", err)
		return 1
	}
	fmt.Printf("ingest-doc: doc_id=%d public_id=%s generation=%d status=%s (the jobs role indexes it)\n",
		doc.DocID, doc.PublicID, doc.Generation, doc.Status)
	return 0
}

// runArtifactGet downloads one committed artifact through the same
// tenant-scoped check the download route uses.
func runArtifactGet(cfg *config.Config, tenantID, publicID, outPath string) int {
	cdp, raw, err := openControlPlane(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "artifact-get: %v\n", err)
		return 1
	}
	defer raw.Close()
	resolver := secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: allowedSecretEnv()})
	stack, ok, err := buildKnowledgeStack(cfg, cdp, resolver)
	if err != nil {
		fmt.Fprintf(os.Stderr, "artifact-get: %v\n", err)
		return 1
	}
	if !ok {
		fmt.Fprintln(os.Stderr, "artifact-get: the knowledge section (object store) is not configured")
		return 1
	}
	a, data, err := stack.artifacts.Open(context.Background(), tenantID, publicID)
	if err != nil {
		if errors.Is(err, artifact.ErrNotFound) {
			fmt.Fprintf(os.Stderr, "artifact-get: %s is not available to tenant %s\n", publicID, tenantID)
			return 1
		}
		fmt.Fprintf(os.Stderr, "artifact-get: %v\n", err)
		return 1
	}
	if outPath == "" || outPath == "-" {
		_, _ = os.Stdout.Write(data)
		return 0
	}
	if err := os.WriteFile(outPath, data, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "artifact-get: %v\n", err)
		return 1
	}
	fmt.Printf("artifact-get: wrote %d bytes of %q to %s\n", len(data), a.Name, outPath)
	return 0
}

// runDocStatus prints one document's lifecycle state — the operator's view
// of "did the index job finish".
func runDocStatus(cfg *config.Config, tenantID, publicID string) int {
	cdp, raw, err := openControlPlane(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "doc-status: %v\n", err)
		return 1
	}
	defer raw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scope := cdp.MustScope(tenantID)
	var (
		status  string
		gen     int
		chunks  int
		indexed int
		errText string
	)
	row, err := scope.QueryRow(ctx, `
		SELECT status, generation, chunk_count, indexed_count, error FROM documents
		WHERE tenant_id = ? AND public_id = ?`, tenantID, publicID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "doc-status: %v\n", err)
		return 1
	}
	if err := row.Scan(&status, &gen, &chunks, &indexed, &errText); err != nil {
		fmt.Fprintf(os.Stderr, "doc-status: no document %s in tenant %s\n", publicID, tenantID)
		return 1
	}
	fmt.Printf("doc-status: public_id=%s status=%s generation=%d chunks=%d indexed=%d error=%q\n",
		publicID, status, gen, chunks, indexed, strings.TrimSpace(errText))
	return 0
}
