package agent

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// Failover is deliberately limited to failures that occur before a response
// channel has been obtained. Retrying a stream after it has started could
// duplicate assistant content or tool calls.
type failoverModel struct {
	refs   []profile.VersionedRef
	models []model.Model
}

func newFailoverModel(refs []profile.VersionedRef, models []model.Model) model.Model {
	return failoverModel{refs: append([]profile.VersionedRef(nil), refs...), models: append([]model.Model(nil), models...)}
}

func (m failoverModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	for index, candidate := range m.models {
		responses, err := candidate.GenerateContent(ctx, request)
		if err == nil {
			return taggedResponses(ctx, responses, m.refs[index]), nil
		}
		if index == len(m.models)-1 || ctx.Err() != nil || !isTimeout(err) {
			return nil, err
		}
	}
	return nil, runtime.ErrBackendUnavailable
}

func (m failoverModel) Info() model.Info { return m.models[0].Info() }

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

const modelProfileTagPrefix = "trpc-profile:v1:"

func taggedResponses(ctx context.Context, input <-chan *model.Response, ref profile.VersionedRef) <-chan *model.Response {
	output := make(chan *model.Response)
	go func() {
		defer close(output)
		for response := range input {
			if response != nil {
				response = response.Clone()
				response.Model = modelProfileTag(ref)
			}
			select {
			case output <- response:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func modelProfileTag(ref profile.VersionedRef) string {
	return modelProfileTagPrefix + base64.RawURLEncoding.EncodeToString([]byte(ref.ID)) + ":" + strconv.FormatInt(ref.Version, 10)
}

// ModelProfileRefFromResponse decodes the internal model profile tag attached
// to responses from a failover model. It avoids a process-local side channel,
// so one run with several model calls can be settled per actual profile.
func ModelProfileRefFromResponse(value string) (profile.VersionedRef, bool) {
	if !strings.HasPrefix(value, modelProfileTagPrefix) {
		return profile.VersionedRef{}, false
	}
	parts := strings.Split(strings.TrimPrefix(value, modelProfileTagPrefix), ":")
	if len(parts) != 2 {
		return profile.VersionedRef{}, false
	}
	id, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(id) == 0 {
		return profile.VersionedRef{}, false
	}
	version, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || version < 1 {
		return profile.VersionedRef{}, false
	}
	return profile.VersionedRef{ID: string(id), Version: version}, true
}
