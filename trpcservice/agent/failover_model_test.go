package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type failoverStub struct {
	calls atomic.Int32
	err   error
	value *model.Response
}

func (m *failoverStub) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	m.calls.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	output := make(chan *model.Response, 1)
	output <- m.value
	close(output)
	return output, nil
}

func (*failoverStub) Info() model.Info { return model.Info{Name: "stub"} }

func TestFailoverModelUsesNextCandidateOnlyBeforeAStream(t *testing.T) {
	primary := &failoverStub{err: context.DeadlineExceeded}
	secondary := &failoverStub{value: &model.Response{Model: "provider-model"}}
	value := newFailoverModel([]profile.VersionedRef{{ID: "primary", Version: 1}, {ID: "secondary", Version: 2}}, []model.Model{primary, secondary})
	responses, err := value.GenerateContent(context.Background(), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	response := <-responses
	ref, ok := ModelProfileRefFromResponse(response.Model)
	if !ok || ref != (profile.VersionedRef{ID: "secondary", Version: 2}) {
		t.Fatalf("response model=%q ref=%#v ok=%t", response.Model, ref, ok)
	}
	if primary.calls.Load() != 1 || secondary.calls.Load() != 1 {
		t.Fatalf("calls primary=%d secondary=%d", primary.calls.Load(), secondary.calls.Load())
	}
}

func TestFailoverModelDoesNotRetryNonTimeout(t *testing.T) {
	primary := &failoverStub{err: errors.New("denied")}
	secondary := &failoverStub{value: &model.Response{}}
	value := newFailoverModel([]profile.VersionedRef{{ID: "primary", Version: 1}, {ID: "secondary", Version: 1}}, []model.Model{primary, secondary})
	_, err := value.GenerateContent(context.Background(), &model.Request{})
	if err == nil || secondary.calls.Load() != 0 {
		t.Fatalf("err=%v secondary calls=%d", err, secondary.calls.Load())
	}
}
