package agent

import (
	"context"
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// DisabledModel is used only on roles without model execution authority.
// It fails closed if accidentally invoked; it never falls back to a mock reply.
type DisabledModel struct{}

func (DisabledModel) Info() model.Info { return model.Info{Name: "disabled-for-role"} }
func (DisabledModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	return nil, errors.New("model execution is disabled for this service role")
}
