package runtime

import (
	"context"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
)

// GovernancePlugin applies tenant redaction inside Runner's event pipeline,
// before events can be persisted by the Session service or exposed to an
// observer. Authorization and budgets remain in the request-scoped Tool
// filters because they require the durable policy ledger.
type GovernancePlugin struct {
	redactor *servicelog.Redactor
}

func NewGovernancePlugin(fields []string) *GovernancePlugin {
	return &GovernancePlugin{redactor: servicelog.NewRedactor(fields, nil)}
}

func (*GovernancePlugin) Name() string { return "tenant-governance" }

func (guard *GovernancePlugin) Register(registry *plugin.Registry) {
	registry.OnEvent(func(_ context.Context, _ *agent.Invocation, item *event.Event) (*event.Event, error) {
		if guard == nil || guard.redactor == nil || item == nil || item.Response == nil {
			return item, nil
		}
		for index := range item.Response.Choices {
			choice := &item.Response.Choices[index]
			choice.Message.Content = guard.redactor.RedactString(choice.Message.Content)
			choice.Delta.Content = guard.redactor.RedactString(choice.Delta.Content)
		}
		return item, nil
	})
}

var _ plugin.Plugin = (*GovernancePlugin)(nil)
