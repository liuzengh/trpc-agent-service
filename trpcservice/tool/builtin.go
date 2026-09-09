package tool

import (
	"context"
	"errors"
	"fmt"
	"math"

	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

// Tool refs. Each is also the function name the model is given, so it stays
// inside the OpenAI naming constraint; see functionNamePattern.
const (
	// RefEcho returns its input unchanged.
	RefEcho = "builtin_echo"
	// RefAdd adds two integers.
	RefAdd = "builtin_add"
)

// PolicySafeTools authorizes the tools that read nothing, write nothing and
// return the same answer for the same arguments. A revision that names it is
// asking for computation, not for access. Unlike a tool ref this name never
// reaches a model, so it keeps the platform's dotted namespace.
const PolicySafeTools = "builtin.safe-tools"

// EchoInput is what the model sends to builtin_echo.
type EchoInput struct {
	Text string `json:"text" jsonschema:"description=The text to return unchanged,required"`
}

// EchoOutput is what builtin_echo returns.
type EchoOutput struct {
	Text string `json:"text" jsonschema:"description=The text exactly as it was received"`
}

// AddInput is what the model sends to builtin_add.
type AddInput struct {
	A int `json:"a" jsonschema:"description=The first integer addend,required"`
	B int `json:"b" jsonschema:"description=The second integer addend,required"`
}

// AddOutput is what builtin_add returns.
type AddOutput struct {
	Sum int `json:"sum" jsonschema:"description=The sum of a and b"`
}

// echo returns its argument. It exists to prove the tool path end to end
// without giving the model anything to reach.
func echo(_ context.Context, input EchoInput) (EchoOutput, error) {
	return EchoOutput{Text: input.Text}, nil
}

// add returns the sum of two integers.
//
// Go wraps silently on overflow, and a wrapped sum is a plausible-looking wrong
// answer that neither the model nor the caller can detect. Out-of-range is
// reported as a tool error instead, which the model sees and can act on.
func add(_ context.Context, input AddInput) (AddOutput, error) {
	if (input.B > 0 && input.A > math.MaxInt-input.B) ||
		(input.B < 0 && input.A < math.MinInt-input.B) {
		return AddOutput{}, errors.New("builtin_add: the sum is out of integer range")
	}
	return AddOutput{Sum: input.A + input.B}, nil
}

// builtinDefinitions is the registered tool set. A new platform tool is added
// here; nothing else in the resolution path changes.
func builtinDefinitions() []Definition {
	return []Definition{
		{
			Ref:         RefEcho,
			Description: "Return the supplied text unchanged.",
			New: func() agenttool.Tool {
				return function.NewFunctionTool(
					echo,
					function.WithName(RefEcho),
					function.WithDescription("Return the supplied text unchanged."),
				)
			},
		},
		{
			Ref:         RefAdd,
			Description: "Add two integers and return their sum.",
			New: func() agenttool.Tool {
				return function.NewFunctionTool(
					add,
					function.WithName(RefAdd),
					function.WithDescription("Add two integers and return their sum."),
				)
			},
		},
	}
}

// builtinPolicies is the registered policy set.
func builtinPolicies() []PolicyDefinition {
	return []PolicyDefinition{{
		Ref:             PolicySafeTools,
		Description:     "Allow side-effect-free, deterministic platform tools.",
		AllowedToolRefs: []string{RefEcho, RefAdd},
	}}
}

// builtinRegistry is the process registry.
//
// It is built at init and panics on a bad definition, like the package-level
// regexps elsewhere in this service: the input is compiled into the binary, so
// a failure here means the binary is wrong and every request would fail the
// same way. Failing at startup reports that once, instead of once per request.
var builtinRegistry = mustRegistry(builtinDefinitions(), builtinPolicies())

// Builtin returns the process tool registry.
func Builtin() *Registry {
	return builtinRegistry
}

func mustRegistry(tools []Definition, policies []PolicyDefinition) *Registry {
	registry, err := NewRegistry(tools, policies)
	if err != nil {
		panic(fmt.Sprintf("tool: build builtin registry: %v", err))
	}
	return registry
}
