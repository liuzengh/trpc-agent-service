package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	toolmcp "github.com/liuzengh/trpc-agent-service/trpcservice/tool/mcp"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// runMCPDeclarationDigest prints the canonical digest of one reviewed MCP
// tools/list entry. It is an offline operator utility: it never opens a
// network connection or reads a secret.
func runMCPDeclarationDigest(arguments []string, output io.Writer) error {
	if output == nil {
		return errors.New("MCP declaration digest output is required")
	}
	flags := flag.NewFlagSet("mcp-declaration-digest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("declaration-file", "", "reviewed MCP tool declaration JSON file")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || strings.TrimSpace(*path) == "" {
		return errors.New("usage: mcp-declaration-digest --declaration-file <reviewed-tool.json>")
	}
	contents, err := os.ReadFile(*path)
	if err != nil {
		return fmt.Errorf("read MCP declaration: %w", err)
	}
	var declaration agenttool.Declaration
	if err := json.Unmarshal(contents, &declaration); err != nil {
		return fmt.Errorf("decode MCP declaration: %w", err)
	}
	digest, err := toolmcp.DeclarationDigest(&declaration)
	if err != nil {
		return fmt.Errorf("MCP declaration rejected: %w", err)
	}
	_, err = fmt.Fprintln(output, digest)
	return err
}

// runMCPBindingDigests prints the immutable ToolRef content_digest for every
// configured MCP endpoint. It only parses TRPC_MCP_ENDPOINTS and therefore
// neither contacts a remote server nor resolves secret material.
func runMCPBindingDigests(arguments []string, output io.Writer, getenv func(string) string) error {
	if output == nil || getenv == nil {
		return errors.New("MCP binding digest dependencies are required")
	}
	if len(arguments) != 0 {
		return errors.New("usage: mcp-binding-digests")
	}
	endpoints, err := parseMCPEndpoints(getenv("TRPC_MCP_ENDPOINTS"))
	if err != nil || len(endpoints) == 0 {
		return errors.New("TRPC_MCP_ENDPOINTS must contain at least one valid endpoint")
	}
	for _, endpoint := range endpoints {
		digest, digestErr := toolmcp.BindingDigest(toolmcp.Config{Transport: endpoint.Transport, ServerURL: endpoint.ServerURL,
			RemoteToolName: endpoint.ToolID, ExpectedDeclarationDigest: endpoint.DeclarationDigest, Timeout: endpoint.Timeout,
			SecretHeader: endpoint.SecretHeader, SecretPrefix: endpoint.SecretPrefix},
			secrets.SecretRef{Ref: endpoint.SecretRef, Version: endpoint.SecretVersion})
		if digestErr != nil {
			return fmt.Errorf("MCP endpoint %q rejected: %w", endpoint.ToolID, digestErr)
		}
		if _, err = fmt.Fprintf(output, "%s\t%d\t%s\n", endpoint.ToolID, endpoint.Version, digest); err != nil {
			return err
		}
	}
	return nil
}
