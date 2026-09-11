package tool

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type fakeCallableTool struct {
	name  string
	calls int
}

func (t *fakeCallableTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{Name: t.name}
}

func (t *fakeCallableTool) Call(_ context.Context, _ []byte) (any, error) {
	t.calls++
	return "ok", nil
}

type recordingAuditSink struct {
	events []governance.ToolAuditEvent
}

func (s *recordingAuditSink) RecordToolAudit(_ context.Context, event governance.ToolAuditEvent) error {
	s.events = append(s.events, event)
	return nil
}
