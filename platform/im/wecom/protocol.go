package wecom

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/coder/websocket"
)

func (s *session) read() {
	defer close(s.readDone)
	for {
		kind, raw, err := s.conn.Read(s.ctx)
		if err != nil {
			if errors.Is(err, websocket.ErrMessageTooBig) {
				s.fail(ErrProtocol)
			} else {
				s.fail(ErrDisconnected)
			}
			return
		}
		if kind != websocket.MessageText {
			s.fail(ErrProtocol)
			return
		}
		frame, err := decodeObject(raw)
		if err != nil {
			s.fail(ErrProtocol)
			return
		}
		headers, ok := frame["headers"].(map[string]any)
		if !ok {
			s.fail(ErrProtocol)
			return
		}
		id, ok := headers["req_id"].(string)
		if !ok || !validID(id) {
			s.fail(ErrProtocol)
			return
		}
		cmd := ""
		if raw, exists := frame["cmd"]; exists {
			var valid bool
			cmd, valid = raw.(string)
			if !valid || cmd == "" {
				s.fail(ErrProtocol)
				return
			}
		}
		switch cmd {
		case "aibot_msg_callback", "aibot_event_callback":
			event, err := s.event(cmd, id, frame)
			if err != nil {
				s.fail(ErrProtocol)
				return
			}
			if event.EventType == "disconnected_event" {
				// Control state changes before any slow user handler. This event lacks
				// sender/chat fields in the official service fixture.
				s.stop(ErrReplaced, &event)
				return
			}
			s.client.mu.Lock()
			ready := s.client.snapshot.State == StateReady && s.client.current == s
			s.client.mu.Unlock()
			if !ready {
				s.fail(ErrProtocol)
				return
			}
			select {
			case s.client.events <- queuedEvent{ctx: s.ctx, event: event}:
			default:
				s.fail(ErrEventOverflow)
				return
			}
		default:
			number, ok := frame["errcode"].(json.Number)
			if !ok {
				s.fail(ErrProtocol)
				return
			}
			code, err := strconv.ParseInt(string(number), 10, 64)
			if err != nil {
				s.fail(ErrProtocol)
				return
			}
			c := s.client
			c.mu.Lock()
			p := s.pending[id]
			if p != nil && c.current == s {
				expected := map[string]string{"auth": "aibot_subscribe", "ping": "ping", "reply": "aibot_respond_msg"}[p.kind]
				if cmd != "" && cmd != expected {
					c.mu.Unlock()
					s.fail(ErrProtocol)
					return
				}
				result := commandResult{ack: CommandAck{RequestID: id, Generation: s.gen, ErrCode: code}}
				if code != 0 {
					result.err = commandError(Rejected, CodeRejected)
				}
				if p.kind == "auth" {
					ack := result.ack
					c.authAck = &ack
					if code == 0 {
						c.setStateLocked(StateReady, s.gen, "")
					}
				}
				c.completeLocked(s, p, result)
			}
			c.mu.Unlock()
		}
	}
}
func (s *session) event(cmd, id string, frame map[string]any) (Event, error) {
	body, ok := frame["body"].(map[string]any)
	if !ok {
		return Event{}, ErrProtocol
	}
	canonical, err := json.Marshal(body)
	if err != nil {
		return Event{}, ErrProtocol
	}
	digest := sha256.Sum256(canonical)
	event := Event{BodyDigest: hex.EncodeToString(digest[:]), RequestID: id, Generation: s.gen, MessageID: str(body, "msgid"), BotID: str(body, "aibotid"), ChatID: str(body, "chatid"), ChatType: str(body, "chattype")}
	if !validID(event.MessageID) || event.BotID != s.client.cfg.BotID {
		return Event{}, ErrProtocol
	}
	msgtype := str(body, "msgtype")
	if !validID(msgtype) {
		return Event{}, ErrProtocol
	}
	if cmd == "aibot_event_callback" {
		item, ok := body["event"].(map[string]any)
		if !ok || msgtype != "event" {
			return Event{}, ErrProtocol
		}
		event.EventType = str(item, "eventtype")
		if !validID(event.EventType) {
			return Event{}, ErrProtocol
		}
		event.Kind = EventNotice
		if event.EventType == "disconnected_event" {
			return event, nil
		}
		switch event.EventType {
		case "enter_chat", "template_card_event", "feedback_event":
		default:
			event.Kind = EventUnsupported
		}
	} else if msgtype == "text" {
		text, ok := body["text"].(map[string]any)
		if !ok {
			return Event{}, ErrProtocol
		}
		content, ok := text["content"].(string)
		if !ok {
			return Event{}, ErrProtocol
		}
		event.Kind = EventText
		event.Text = content
	} else {
		event.Kind = EventUnsupported
		event.EventType = msgtype
	}
	from, _ := body["from"].(map[string]any)
	event.SenderID = str(from, "userid")
	if !validID(event.SenderID) {
		return Event{}, ErrProtocol
	}
	if cmd == "aibot_msg_callback" && (event.ChatType != "single" && event.ChatType != "group" || event.ChatType == "group" && !validID(event.ChatID)) {
		return Event{}, ErrProtocol
	}
	return event, nil
}
func str(m map[string]any, key string) string { s, _ := m[key].(string); return s }

// Reject duplicate keys, invalid UTF-8 and multiple values before interpreting
// security-relevant protocol fields. Unknown fields are intentionally ignored.
func decodeObject(raw []byte) (map[string]any, error) {
	if !utf8.Valid(raw) {
		return nil, ErrProtocol
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	value, err := decodeValue(dec, 0)
	if err != nil {
		return nil, ErrProtocol
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, ErrProtocol
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, ErrProtocol
	}
	return object, nil
}
func decodeValue(dec *json.Decoder, depth int) (any, error) {
	if depth > 32 {
		return nil, ErrProtocol
	}
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, ErrProtocol
			}
			if _, exists := object[name]; exists {
				return nil, ErrProtocol
			}
			value, err := decodeValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return object, nil
	case '[':
		var values []any
		for dec.More() {
			value, err := decodeValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return values, nil
	default:
		return nil, ErrProtocol
	}
}
