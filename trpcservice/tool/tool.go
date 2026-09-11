// Package tool registers platform tools and tenant-scoped function tools.
package tool

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

const Echo = "echo"

type echoInput struct {
	Text string `json:"text" jsonschema:"description=text to echo"`
}

type echoOutput struct {
	Text string `json:"text"`
}

// Builtins exposes the safe, platform-owned tools. No shell, filesystem, or
// network write tool is registered until a concrete sandbox/approval provider
// is configured.
func Builtins() map[string]tool.Tool {
	echo := function.NewFunctionTool(func(_ context.Context, in echoInput) (echoOutput, error) {
		return echoOutput{Text: in.Text}, nil
	}, function.WithName(Echo), function.WithDescription("Repeat supplied text exactly."))
	return map[string]tool.Tool{Echo: echo}
}

func Select(allowed []string) []tool.Tool {
	all := Builtins()
	out := make([]tool.Tool, 0, len(allowed))
	for _, name := range allowed {
		if t, ok := all[name]; ok {
			out = append(out, t)
		}
	}
	return out
}
