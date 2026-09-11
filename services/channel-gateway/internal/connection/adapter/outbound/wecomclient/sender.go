package wecomclient

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
)

// MaxReservations bounds prepared/in-flight/evidence-persistence work per Client.
// This is a local initial budget, not a claim about Provider capacity.
const MaxReservations = 128

type reservation struct {
	client *Client
	target connection.ReplyTarget
	// All lifecycle state is protected by client.mu.
	used, released, calling, completed bool
}

func (c *Client) ReserveFinal(ctx context.Context, t connection.ReplyTarget) (connection.ReservedSender, error) {
	if ctx == nil || t.Validate() != nil {
		return nil, connection.ErrInvalidSender
	}
	if ctx.Err() != nil {
		return nil, connection.ErrSenderUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.accepting || c.complete.Load() || c.replaced.Load() {
		return nil, connection.ErrSenderUnavailable
	}
	state := c.client.State()
	if state.State != wecom.StateReady {
		return nil, connection.ErrSenderUnavailable
	}
	if state.Generation != t.SocketGeneration {
		return nil, connection.ErrStaleOrigin
	}
	if c.reservations >= MaxReservations {
		return nil, connection.ErrSenderCapacity
	}
	if c.active == 0 {
		c.zero = make(chan struct{})
	}
	c.active++
	c.reservations++
	return &reservation{client: c, target: t}, nil
}
func (r *reservation) SendFinal(ctx context.Context, cmd connection.FinalCommand) connection.SendResult {
	c := r.client
	c.mu.Lock()
	code := connection.SendOK
	switch {
	case r.released:
		code = connection.SendReleased
	case r.used:
		code = connection.SendUsed
	}
	if code != "" {
		c.mu.Unlock()
		return connection.SendResult{Certainty: connection.NotSent, Code: code}
	}
	r.used = true
	switch {
	case ctx == nil:
		code = connection.SendInvalid
	case ctx.Err() != nil:
		code = connection.SendCanceled
	case !c.accepting:
		code = connection.SendQuiescing
	}
	if code != "" {
		c.mu.Unlock()
		return connection.SendResult{Certainty: connection.NotSent, Code: code}
	}
	state := c.client.State()
	if c.complete.Load() || c.replaced.Load() || state.State != wecom.StateReady {
		c.mu.Unlock()
		return connection.SendResult{Certainty: connection.NotSent, Code: connection.SendUnavailable}
	}
	if state.Generation != r.target.SocketGeneration {
		c.mu.Unlock()
		return connection.SendResult{Certainty: connection.NotSent, Code: connection.SendStale}
	}
	r.calling = true
	c.mu.Unlock()
	// The SDK owns exact pending req_id/generation matching and the entering-Write
	// certainty boundary. Neither shutdown nor owner loss may overwrite its ACK.
	ack, err := c.client.Reply(ctx, wecom.ReplyRequest{RequestID: r.target.RequestID, Generation: r.target.SocketGeneration, StreamID: cmd.StreamID, Content: cmd.Content})
	result := translateResult(r.target, ack, err)
	c.mu.Lock()
	r.calling = false
	if r.released {
		r.finishLocked()
	}
	c.mu.Unlock()
	return result
}
func (r *reservation) Release() {
	c := r.client
	c.mu.Lock()
	defer c.mu.Unlock()
	r.released = true
	if !r.calling {
		r.finishLocked()
	}
}
func (r *reservation) finishLocked() {
	if r.completed {
		return
	}
	r.completed = true
	c := r.client
	c.reservations--
	c.active--
	if c.active == 0 {
		close(c.zero)
	}
}
func translateResult(target connection.ReplyTarget, ack wecom.CommandAck, err error) connection.SendResult {
	result := connection.SendResult{Certainty: connection.Unknown, Code: connection.SendUnknown}
	if err == nil {
		if ack.RequestID == target.RequestID && ack.Generation == target.SocketGeneration && ack.ErrCode == 0 {
			code := ack.ErrCode
			return connection.SendResult{Certainty: connection.Accepted, ProviderCode: &code}
		}
		return result
	}
	var failure *wecom.CommandError
	if !errors.As(err, &failure) {
		return result
	}
	switch failure.Certainty {
	case wecom.NotSent:
		result.Certainty = connection.NotSent
	case wecom.Unknown:
		result.Certainty = connection.Unknown
	case wecom.Rejected:
		if ack.RequestID != target.RequestID || ack.Generation != target.SocketGeneration || ack.ErrCode == 0 {
			return result
		}
		result.Certainty = connection.Rejected
		code := ack.ErrCode
		result.ProviderCode = &code
	default:
		return result
	}
	result.Code = connection.SendCode(failure.Code)
	return result
}
