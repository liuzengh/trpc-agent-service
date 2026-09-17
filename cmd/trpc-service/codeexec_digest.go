package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	toolcodeexec "github.com/liuzengh/trpc-agent-service/trpcservice/tool/codeexec"
)

// runCodeExecutorBindingDigest prints one binding digest before an operator has
// a complete TRPC_CODE_EXECUTORS entry. Unlike the plural command it does not
// parse that JSON, so it is safe to use during first-time configuration.
func runCodeExecutorBindingDigest(arguments []string, output io.Writer, getenv func(string) string) error {
	if output == nil || getenv == nil {
		return errors.New("code executor binding digest dependencies are required")
	}
	flags := flag.NewFlagSet("code-executor-binding-digest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	toolID := flags.String("tool-id", "", "fixed ToolRef ID")
	version := flags.Int64("version", 0, "fixed ToolRef version")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || strings.TrimSpace(*toolID) == "" || *version < 1 {
		return errors.New("usage: code-executor-binding-digest --tool-id <id> --version <n>")
	}
	digest, err := toolcodeexec.BindingDigest(strings.TrimSpace(*toolID), *version,
		toolcodeexec.DefaultConfig(strings.TrimSpace(getenv("TRPC_CODE_EXECUTOR_WORKSPACE_ROOT"))))
	if err != nil {
		return fmt.Errorf("code executor %q rejected: %w", *toolID, err)
	}
	_, err = fmt.Fprintln(output, digest)
	return err
}

// runCodeExecutorBindingDigests prints the immutable ToolRef content_digest for
// each configured sandbox tool. It is an offline operator utility: it parses
// configuration only and never creates a workspace or starts a process.
func runCodeExecutorBindingDigests(arguments []string, output io.Writer, getenv func(string) string) error {
	if output == nil || getenv == nil {
		return errors.New("code executor binding digest dependencies are required")
	}
	if len(arguments) != 0 {
		return errors.New("usage: code-executor-binding-digests")
	}
	endpoints, err := parseCodeExecutorEndpoints(getenv("TRPC_CODE_EXECUTORS"))
	if err != nil || len(endpoints) == 0 {
		return errors.New("TRPC_CODE_EXECUTORS must contain at least one valid endpoint")
	}
	config := toolcodeexec.DefaultConfig(getenv("TRPC_CODE_EXECUTOR_WORKSPACE_ROOT"))
	for _, endpoint := range endpoints {
		digest, digestErr := toolcodeexec.BindingDigest(endpoint.ToolID, endpoint.Version, config)
		if digestErr != nil {
			return fmt.Errorf("code executor %q rejected: %w", endpoint.ToolID, digestErr)
		}
		if _, err = fmt.Fprintf(output, "%s\t%d\t%s\n", endpoint.ToolID, endpoint.Version, digest); err != nil {
			return err
		}
	}
	return nil
}
