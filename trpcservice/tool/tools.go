package tool

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

type calculatorArgs struct {
	Operation string  `json:"operation" jsonschema:"description=Operation,enum=add,enum=subtract,enum=multiply,enum=divide,enum=power"`
	A         float64 `json:"a" jsonschema:"description=First operand"`
	B         float64 `json:"b" jsonschema:"description=Second operand"`
}

type calculatorResult struct {
	Result float64 `json:"result"`
}

type timeArgs struct {
	Timezone string `json:"timezone" jsonschema:"description=IANA timezone such as Asia/Shanghai or UTC"`
}

type timeResult struct {
	Timezone string `json:"timezone"`
	RFC3339  string `json:"rfc3339"`
}

type adminArgs struct {
	Action string `json:"action" jsonschema:"description=Simulated administrative action"`
}

type adminResult struct {
	Status string `json:"status"`
}

// BuiltInTools returns the platform tool surface available to every tenant.
// Tenant tool policy further filters which of these the model may call.
func BuiltInTools() []tool.Tool {
	calculator := function.NewFunctionTool(
		func(_ context.Context, args calculatorArgs) (calculatorResult, error) {
			var result float64
			switch strings.ToLower(args.Operation) {
			case "add":
				result = args.A + args.B
			case "subtract":
				result = args.A - args.B
			case "multiply":
				result = args.A * args.B
			case "divide":
				if args.B == 0 {
					return calculatorResult{}, errors.New("division by zero")
				}
				result = args.A / args.B
			case "power":
				result = math.Pow(args.A, args.B)
			default:
				return calculatorResult{}, errors.New("unsupported operation")
			}
			return calculatorResult{Result: result}, nil
		},
		function.WithName("calculator"),
		function.WithDescription("Perform a basic arithmetic calculation."),
	)
	currentTime := function.NewFunctionTool(
		func(_ context.Context, args timeArgs) (timeResult, error) {
			zone := args.Timezone
			if zone == "" {
				zone = "UTC"
			}
			location, err := time.LoadLocation(zone)
			if err != nil {
				return timeResult{}, errors.New("invalid IANA timezone")
			}
			return timeResult{Timezone: zone, RFC3339: time.Now().In(location).Format(time.RFC3339)}, nil
		},
		function.WithName("current_time"),
		function.WithDescription("Read the current time in an IANA timezone."),
	)
	admin := function.NewFunctionTool(
		func(_ context.Context, args adminArgs) (adminResult, error) {
			// This is a non-mutating demonstration tool. The policy still treats it
			// as dangerous so the approval path can be exercised safely.
			return adminResult{Status: "simulated: " + args.Action}, nil
		},
		function.WithName("tenant_admin_action"),
		function.WithDescription("Simulate a tenant administrative action; explicit confirmation is required."),
		function.WithConcurrencySafe(false),
	)
	return []tool.Tool{calculator, currentTime, admin}
}
