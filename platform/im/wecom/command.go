package wecom

import (
	"context"
	"encoding/json"
	"unicode/utf8"

	"github.com/coder/websocket"
)

func (c *Client) Reply(ctx context.Context, req ReplyRequest) (CommandAck, error) {
	if ctx == nil || !validID(req.RequestID) || !validID(req.StreamID) || req.Generation == 0 || req.Content == "" || len(req.Content) > 20480 || !utf8.ValidString(req.Content) {
		return CommandAck{}, commandError(NotSent, CodeInvalid)
	}
	c.mu.Lock()
	s := c.current
	closed := c.closed
	ready := c.snapshot.State == StateReady
	c.mu.Unlock()
	if closed {
		return CommandAck{}, commandError(NotSent, CodeClosed)
	}
	if s == nil || !ready {
		return CommandAck{}, commandError(NotSent, CodeNotReady)
	}
	if s.gen != req.Generation {
		return CommandAck{}, commandError(NotSent, CodeStaleGeneration)
	}
	return c.command(ctx, s, req.RequestID, "reply", map[string]any{"cmd": "aibot_respond_msg", "body": map[string]any{"msgtype": "stream", "stream": map[string]any{"id": req.StreamID, "content": req.Content, "finish": true}}})
}
func (c *Client) command(ctx context.Context, s *session, id, kind string, frame map[string]any) (CommandAck, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.cfg.AckTimeout)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	// One bounded waiter per caller; there is no unbounded library-side goroutine
	// or queued command. Pending registration occurs before the socket write.
	select {
	case s.writer <- struct{}{}:
	case <-callCtx.Done():
		return CommandAck{}, commandError(NotSent, CodeCanceled)
	}
	release := func() { <-s.writer }
	c.mu.Lock()
	var code ErrorCode
	switch {
	case c.closed:
		code = CodeClosed
	case callCtx.Err() != nil || s.ctx.Err() != nil:
		code = CodeCanceled
	case c.current != s:
		code = CodeStaleGeneration
	case kind == "reply" && c.snapshot.State != StateReady:
		code = CodeNotReady
	case s.pending[id] != nil:
		code = CodeInFlight
	case kind == "reply" && s.used[id] != "":
		code = s.used[id]
	case kind == "reply" && s.replyPending >= c.cfg.MaxPending:
		code = CodeCapacity
	case kind != "reply" && len(s.pending)-s.replyPending >= 1:
		code = CodeCapacity
	case kind == "reply" && len(s.used) >= c.cfg.MaxRequestIDs:
		code = CodeCapacity
	}
	if code != "" {
		c.mu.Unlock()
		release()
		return CommandAck{}, commandError(NotSent, code)
	}
	p := &pending{id: id, kind: kind, done: make(chan struct{})}
	s.pending[id] = p
	if kind == "reply" {
		s.replyPending++
		s.used[id] = CodeInFlight
	}
	frame["headers"] = map[string]string{"req_id": id}
	payload, err := json.Marshal(frame)
	if err != nil {
		c.completeLocked(s, p, commandResult{err: commandError(NotSent, CodeInvalid)})
		c.mu.Unlock()
		release()
		return p.result.ack, p.result.err
	}
	if callCtx.Err() != nil {
		c.completeLocked(s, p, commandResult{err: commandError(NotSent, CodeCanceled)})
		c.mu.Unlock()
		release()
		return p.result.ack, p.result.err
	}
	// The certainty boundary is entering Write, not its return value. A failed
	// Write may already have transferred bytes; no transport error is exposed.
	p.attempted = true
	c.mu.Unlock()
	writeCtx, writeCancel := context.WithTimeout(callCtx, c.cfg.WriteTimeout)
	err = s.conn.Write(writeCtx, websocket.MessageText, payload)
	writeCancel()
	release()
	if err != nil {
		c.mu.Lock()
		c.completeLocked(s, p, commandResult{err: commandError(Unknown, CodeWrite)})
		c.mu.Unlock()
	}
	select {
	case <-p.done:
	case <-callCtx.Done():
		code := CodeAckTimeout
		if ctx.Err() != nil || s.ctx.Err() != nil {
			code = CodeCanceled
		}
		c.mu.Lock()
		c.completeLocked(s, p, commandResult{err: commandError(Unknown, code)})
		c.mu.Unlock()
	}
	c.mu.Lock()
	result := p.result
	c.mu.Unlock()
	return result.ack, result.err
}
func (c *Client) completeLocked(s *session, p *pending, result commandResult) {
	if p.completed {
		return
	}
	p.completed = true
	p.result = result
	delete(s.pending, p.id)
	if p.kind == "reply" {
		s.replyPending--
		if !p.attempted {
			delete(s.used, p.id)
		} else {
			s.used[p.id] = CodeFinalAttempted
			if e, ok := result.err.(*CommandError); ok && e.Certainty == Unknown {
				s.used[p.id] = CodePoisoned
			}
		}
	}
	close(p.done)
}
