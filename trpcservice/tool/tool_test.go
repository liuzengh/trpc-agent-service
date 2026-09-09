package tool_test

import (
	"context"
	"regexp"
	"testing"

	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/stretchr/testify/require"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

// openAIFunctionName is the constraint an OpenAI-compatible provider applies to
// a function name. It is restated here rather than imported so the test fails
// if the registry's own pattern is loosened.
var openAIFunctionName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// Every name the platform hands a model has to survive the provider that
// receives it. A rejected name fails the whole request, including turns that
// never wanted a tool, so this is asserted on the declaration the model is
// actually given rather than on the ref.
func TestBuiltinToolNamesAreAcceptedByOpenAIProviders(t *testing.T) {
	tools, err := servicetool.Builtin().Resolve(
		[]string{servicetool.RefEcho, servicetool.RefAdd},
		[]string{servicetool.PolicySafeTools},
	)
	require.NoError(t, err)
	require.Len(t, tools, 2)

	for index, expected := range []string{servicetool.RefEcho, servicetool.RefAdd} {
		declaration := tools[index].Declaration()
		require.NotNil(t, declaration)
		require.Equal(t, expected, declaration.Name)
		require.Regexp(t, openAIFunctionName, declaration.Name)
		require.NotContains(t, declaration.Name, ".", "a dot is rejected by several providers")
		require.NotEmpty(t, declaration.Description)
		require.NotNil(t, declaration.InputSchema)
	}
}

// The declared schema is the only description of a tool the model gets, so the
// arguments it is told to send have to be the ones the tool reads.
func TestBuiltinToolSchemas(t *testing.T) {
	for name, expected := range map[string]struct {
		ref        string
		properties []string
	}{
		"echo": {servicetool.RefEcho, []string{"text"}},
		"add":  {servicetool.RefAdd, []string{"a", "b"}},
	} {
		t.Run(name, func(t *testing.T) {
			tools := resolveBuiltin(t, expected.ref)
			schema := tools[0].Declaration().InputSchema

			require.Equal(t, "object", schema.Type)
			require.Len(t, schema.Properties, len(expected.properties))
			for _, property := range expected.properties {
				require.Contains(t, schema.Properties, property)
				require.NotEmpty(t, schema.Properties[property].Description)
			}
			require.ElementsMatch(t, expected.properties, schema.Required)
		})
	}
}

// Both tools have to be side-effect free and answer the same way every time:
// that is the property the safe-tools policy is asserting about them.
func TestBuiltinToolsAreDeterministic(t *testing.T) {
	t.Run("echo returns its input unchanged", func(t *testing.T) {
		echo := callable(t, resolveBuiltin(t, servicetool.RefEcho)[0])

		for range 2 {
			result, err := echo.Call(context.Background(), []byte(`{"text":"hello 世界"}`))
			require.NoError(t, err)
			require.Equal(t, servicetool.EchoOutput{Text: "hello 世界"}, result)
		}
	})

	t.Run("add sums two integers", func(t *testing.T) {
		add := callable(t, resolveBuiltin(t, servicetool.RefAdd)[0])

		for name, testCase := range map[string]struct {
			arguments string
			sum       int
		}{
			"positive": {`{"a":2,"b":3}`, 5},
			"negative": {`{"a":-7,"b":3}`, -4},
			"zero":     {`{"a":0,"b":0}`, 0},
			"missing":  {`{"a":4}`, 4},
		} {
			t.Run(name, func(t *testing.T) {
				result, err := add.Call(context.Background(), []byte(testCase.arguments))
				require.NoError(t, err)
				require.Equal(t, servicetool.AddOutput{Sum: testCase.sum}, result)
			})
		}
	})
}

// A wrapped sum is a wrong answer that looks like a right one, and neither the
// model nor the caller can tell. It has to be reported instead.
func TestBuiltinAddRejectsOutOfRangeSum(t *testing.T) {
	add := callable(t, resolveBuiltin(t, servicetool.RefAdd)[0])

	for name, arguments := range map[string]string{
		"positive overflow": `{"a":9223372036854775807,"b":1}`,
		"negative overflow": `{"a":-9223372036854775808,"b":-1}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := add.Call(context.Background(), []byte(arguments))
			require.ErrorContains(t, err, "out of integer range")
		})
	}
}

// Every revision published before tools existed has no tool_refs, and must
// build exactly as it did: no tools, no error, and no policy required.
func TestResolveWithoutToolRefsStaysEmpty(t *testing.T) {
	for name, testCase := range map[string]struct{ toolRefs, policyRefs []string }{
		"both nil":            {nil, nil},
		"both empty":          {[]string{}, []string{}},
		"policy but no tools": {nil, []string{servicetool.PolicySafeTools}},
	} {
		t.Run(name, func(t *testing.T) {
			tools, err := servicetool.Builtin().Resolve(testCase.toolRefs, testCase.policyRefs)
			require.NoError(t, err)
			require.Empty(t, tools)
		})
	}
}

// The whole authorization decision, stated as the cases that must not produce a
// runtime. None of these may resolve to a reduced tool set: a revision that
// names something the platform cannot honor is not a revision that runs with
// fewer tools, it is a revision that does not run.
func TestResolveFailsClosed(t *testing.T) {
	registry := twoPolicyRegistry(t)

	for name, testCase := range map[string]struct {
		toolRefs   []string
		policyRefs []string
		wantErr    error
		wantText   string
	}{
		"unknown tool": {
			[]string{"builtin_shell"}, []string{policyAll},
			servicetool.ErrUnknownTool, `"builtin_shell"`,
		},
		"empty tool ref": {
			[]string{""}, []string{policyAll},
			servicetool.ErrUnknownTool, `""`,
		},
		"duplicate tool": {
			[]string{testRefEcho, testRefEcho}, []string{policyAll},
			servicetool.ErrDuplicateRef, `tool "test_echo"`,
		},
		"tools without any policy": {
			[]string{testRefEcho}, nil,
			servicetool.ErrPolicyRequired, "1 tool ref",
		},
		"tools with empty policy list": {
			[]string{testRefEcho, testRefAdd}, []string{},
			servicetool.ErrPolicyRequired, "2 tool ref",
		},
		"unknown policy": {
			[]string{testRefEcho}, []string{"test.does-not-exist"},
			servicetool.ErrUnknownPolicy, `"test.does-not-exist"`,
		},
		// A policy list is validated even when it authorizes nothing, so a
		// policy deleted from the binary cannot go unnoticed.
		"unknown policy without tools": {
			nil, []string{"test.does-not-exist"},
			servicetool.ErrUnknownPolicy, `"test.does-not-exist"`,
		},
		"duplicate policy": {
			[]string{testRefEcho}, []string{policyAll, policyAll},
			servicetool.ErrDuplicateRef, `policy "test.all"`,
		},
		"duplicate policy without tools": {
			nil, []string{policyAll, policyAll},
			servicetool.ErrDuplicateRef, `policy "test.all"`,
		},
		// The narrowing rule: policies intersect, so a tool one policy allows
		// and another does not is refused.
		"tool refused by one of two policies": {
			[]string{testRefEcho, testRefAdd}, []string{policyAll, policyEchoOnly},
			servicetool.ErrNotAuthorized, `policy "test.echo-only" does not allow tool "test_add"`,
		},
		"tool allowed by no policy": {
			[]string{testRefAdd}, []string{policyNone},
			servicetool.ErrNotAuthorized, `policy "test.none" does not allow tool "test_add"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			tools, err := registry.Resolve(testCase.toolRefs, testCase.policyRefs)
			require.Nil(t, tools)
			require.ErrorIs(t, err, testCase.wantErr)
			require.ErrorContains(t, err, testCase.wantText)
		})
	}
}

// A policy that allows nothing is a policy, not a mistake: it authorizes an
// empty tool set, which is what a revision with no tool_refs asks for.
func TestResolveAcceptsKnownPolicyThatAllowsNothing(t *testing.T) {
	tools, err := twoPolicyRegistry(t).Resolve(nil, []string{policyNone})
	require.NoError(t, err)
	require.Empty(t, tools)
}

// The order the model sees is the order the revision wrote, so a revision does
// not silently change the prompt it produces between builds.
func TestResolveKeepsToolRefOrder(t *testing.T) {
	for _, order := range [][]string{
		{servicetool.RefEcho, servicetool.RefAdd},
		{servicetool.RefAdd, servicetool.RefEcho},
	} {
		tools, err := servicetool.Builtin().Resolve(order, []string{servicetool.PolicySafeTools})
		require.NoError(t, err)
		require.Equal(t, order, declaredNames(tools))
	}
}

// A Runtime is per (tenant, app, revision) and outlives a request. Two
// Runtimes must not share a tool instance, or a tool that later holds state
// would carry it across tenants.
func TestResolveReturnsIndependentInstances(t *testing.T) {
	first, err := servicetool.Builtin().Resolve(
		[]string{servicetool.RefEcho}, []string{servicetool.PolicySafeTools})
	require.NoError(t, err)
	second, err := servicetool.Builtin().Resolve(
		[]string{servicetool.RefEcho}, []string{servicetool.PolicySafeTools})
	require.NoError(t, err)

	require.NotSame(t, first[0], second[0])
}

// These are faults in the binary rather than in a revision, and each is caught
// once at build time instead of becoming a per-tenant failure later.
func TestNewRegistryRejectsInvalidDefinitions(t *testing.T) {
	valid := servicetool.Definition{
		Ref:         testRefEcho,
		Description: "Echo.",
		New:         func() agenttool.Tool { return testEchoTool(testRefEcho) },
	}

	for name, testCase := range map[string]struct {
		tools    []servicetool.Definition
		policies []servicetool.PolicyDefinition
		wantText string
	}{
		"dotted tool ref": {
			// The reason the platform's dotted ids are not reused as function
			// names: several providers reject a dot.
			[]servicetool.Definition{{
				Ref: "builtin.echo",
				New: func() agenttool.Tool { return testEchoTool("builtin.echo") },
			}},
			nil,
			`tool ref "builtin.echo" must match`,
		},
		"empty tool ref": {
			[]servicetool.Definition{{New: func() agenttool.Tool { return testEchoTool("") }}},
			nil,
			`tool ref "" must match`,
		},
		"duplicate tool ref": {
			[]servicetool.Definition{valid, valid}, nil,
			`tool ref "test_echo" is registered twice`,
		},
		"missing constructor": {
			[]servicetool.Definition{{Ref: testRefEcho}}, nil,
			`tool ref "test_echo" has no constructor`,
		},
		"constructor returns nothing": {
			[]servicetool.Definition{{
				Ref: testRefEcho,
				New: func() agenttool.Tool { return nil },
			}},
			nil,
			`tool ref "test_echo" constructed no tool`,
		},
		// The ref and the declared function name are two strings that must be
		// one, because only the declaration reaches the provider.
		"declaration disagrees with ref": {
			[]servicetool.Definition{{
				Ref: testRefEcho,
				New: func() agenttool.Tool { return testEchoTool("something_else") },
			}},
			nil,
			`tool ref "test_echo" declares function name "something_else"`,
		},
		"policy allows unregistered tool": {
			[]servicetool.Definition{valid},
			[]servicetool.PolicyDefinition{{
				Ref: policyAll, AllowedToolRefs: []string{"test_missing"},
			}},
			`policy "test.all" allows unregistered tool "test_missing"`,
		},
		"duplicate policy ref": {
			[]servicetool.Definition{valid},
			[]servicetool.PolicyDefinition{{Ref: policyAll}, {Ref: policyAll}},
			`policy ref "test.all" is registered twice`,
		},
		"policy without ref": {
			[]servicetool.Definition{valid},
			[]servicetool.PolicyDefinition{{}},
			"a policy has no ref",
		},
	} {
		t.Run(name, func(t *testing.T) {
			registry, err := servicetool.NewRegistry(testCase.tools, testCase.policies)
			require.Nil(t, registry)
			require.ErrorIs(t, err, servicetool.ErrInvalidRegistry)
			require.ErrorContains(t, err, testCase.wantText)
		})
	}
}

func TestResolveOnNilRegistry(t *testing.T) {
	tools, err := (*servicetool.Registry)(nil).Resolve(nil, nil)
	require.Nil(t, tools)
	require.ErrorIs(t, err, servicetool.ErrInvalidRegistry)
}

// Test-only refs and policies. They exist because the builtin registry holds a
// single policy, which cannot express the intersection rule.
const (
	testRefEcho = "test_echo"
	testRefAdd  = "test_add"

	policyAll      = "test.all"
	policyEchoOnly = "test.echo-only"
	policyNone     = "test.none"
)

func twoPolicyRegistry(t *testing.T) *servicetool.Registry {
	t.Helper()
	registry, err := servicetool.NewRegistry(
		[]servicetool.Definition{
			{Ref: testRefEcho, New: func() agenttool.Tool { return testEchoTool(testRefEcho) }},
			{Ref: testRefAdd, New: func() agenttool.Tool { return testEchoTool(testRefAdd) }},
		},
		[]servicetool.PolicyDefinition{
			{Ref: policyAll, AllowedToolRefs: []string{testRefEcho, testRefAdd}},
			{Ref: policyEchoOnly, AllowedToolRefs: []string{testRefEcho}},
			{Ref: policyNone},
		},
	)
	require.NoError(t, err)
	return registry
}

func testEchoTool(name string) agenttool.Tool {
	return function.NewFunctionTool(
		func(_ context.Context, input servicetool.EchoInput) (servicetool.EchoOutput, error) {
			return servicetool.EchoOutput{Text: input.Text}, nil
		},
		function.WithName(name),
		function.WithDescription("Test tool."),
	)
}

func resolveBuiltin(t *testing.T, refs ...string) []agenttool.Tool {
	t.Helper()
	tools, err := servicetool.Builtin().Resolve(refs, []string{servicetool.PolicySafeTools})
	require.NoError(t, err)
	require.Len(t, tools, len(refs))
	return tools
}

func callable(t *testing.T, registered agenttool.Tool) agenttool.CallableTool {
	t.Helper()
	callableTool, ok := registered.(agenttool.CallableTool)
	require.True(t, ok, "a registered tool must be callable")
	return callableTool
}

func declaredNames(tools []agenttool.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, registered := range tools {
		names = append(names, registered.Declaration().Name)
	}
	return names
}
