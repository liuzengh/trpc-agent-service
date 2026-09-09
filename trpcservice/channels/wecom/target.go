package wecom

import (
	"encoding/json"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// ErrTargetInvalid reports a stored delivery target this binding will not send
// on: another channel, another version, another tenant or another binding. It
// never repeats the payload.
var ErrTargetInvalid = errors.New("wecom: delivery target is not usable by this binding")

// targetVersion is the version of the payload below. It is stored beside every
// target, so a later shape can be added without guessing at what an old row
// meant: an unknown version is refused rather than parsed hopefully.
const targetVersion = 1

// targetPayload is the version-1 WeCom delivery target.
//
// It carries the tenant and the binding as well as the connection fields
// because a stored target is read back by whatever process picks the row up,
// and that process must be able to refuse a row that is not its own without
// consulting anything else. The generation is what makes the refusal work
// across a reconnect: it belongs to the connection that received the message,
// so a target from an earlier connection is recognisably dead rather than
// merely old.
type targetPayload struct {
	TenantID   string `json:"tenant_id"`
	BindingID  string `json:"binding_id"`
	Generation string `json:"generation"`
	ReqID      string `json:"req_id"`
	StreamID   string `json:"stream_id"`
}

// encodeTarget serializes the reply address of one accepted message.
func encodeTarget(binding Binding, reply ReplyTarget) (channels.DeliveryTarget, error) {
	payload, err := json.Marshal(targetPayload{
		TenantID:   binding.TenantID,
		BindingID:  binding.BindingID,
		Generation: reply.generation,
		ReqID:      reply.reqID,
		StreamID:   reply.streamID,
	})
	if err != nil {
		return channels.DeliveryTarget{}, ErrTargetInvalid
	}
	target := channels.DeliveryTarget{
		Channel: channels.ChannelWeCom,
		Version: targetVersion,
		Payload: payload,
	}
	if err := target.Validate(); err != nil {
		return channels.DeliveryTarget{}, ErrTargetInvalid
	}
	return target, nil
}

// decodeTarget rebuilds a reply address from a stored row, for this binding
// only.
//
// Every field of the scope is checked before the target exists as a value,
// rather than after: a ReplyTarget is a capability to answer a conversation,
// and one that has been assembled from another tenant's row is not made safe by
// a later comparison.
func decodeTarget(binding Binding, target channels.DeliveryTarget) (ReplyTarget, error) {
	if target.Channel != channels.ChannelWeCom || target.Version != targetVersion {
		return ReplyTarget{}, ErrTargetInvalid
	}
	var payload targetPayload
	if err := json.Unmarshal(target.Payload, &payload); err != nil {
		return ReplyTarget{}, ErrTargetInvalid
	}
	if payload.TenantID != binding.TenantID || payload.BindingID != binding.BindingID {
		return ReplyTarget{}, ErrTargetInvalid
	}
	if payload.Generation == "" || payload.ReqID == "" || payload.StreamID == "" {
		return ReplyTarget{}, ErrTargetInvalid
	}
	return ReplyTarget{
		generation: payload.Generation,
		reqID:      payload.ReqID,
		streamID:   payload.StreamID,
	}, nil
}
