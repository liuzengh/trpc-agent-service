package wecom

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// callbackFrame builds an inbound frame from a body a test wrote by hand.
func callbackFrame(t *testing.T, reqID string, body map[string]any) frame {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	return frame{Cmd: cmdMsgCallback, Headers: headers{ReqID: reqID}, Body: encoded}
}

// An accepted callback carries platform identity and a target that addresses
// the answer back to the same connection and the same req_id.
func TestNormalizeAcceptsSingleChatText(t *testing.T) {
	binding := testBinding()
	now := time.Now()

	msg, err := normalizeDirectText(
		binding, callbackFrame(t, "callback-req-1", textCallback()), "generation-1", now)

	require.NoError(t, err)
	require.Equal(t, binding.TenantID, msg.TenantID)
	require.Equal(t, binding.AgentAppID, msg.AgentAppID)
	require.Equal(t, binding.BindingID, msg.BindingID)
	require.Equal(t, msgIDMarker, msg.ExternalMessageID)
	require.Equal(t, bodyMarker, msg.Text)
	require.Equal(t, now, msg.ReceivedAt)
	require.Equal(t, ReplyTarget{
		generation: "generation-1",
		reqID:      "callback-req-1",
		streamID:   streamID(binding.TenantID, binding.BindingID, msgIDMarker),
	}, msg.Reply)
}

// Everything this adapter does not implement is refused here rather than half
// understood later, and no refusal repeats what it refused.
func TestNormalizeRejectsEverythingElse(t *testing.T) {
	binding := testBinding()
	for _, tc := range []struct {
		name  string
		reqID string
		// mutate edits a well formed body into the case under test.
		mutate func(body map[string]any)
		// rawBody replaces the body entirely, for shapes a map cannot express.
		rawBody json.RawMessage
		want    string
	}{
		{
			name:   "a message for another bot",
			mutate: func(b map[string]any) { b["aibotid"] = "another-" + botIDMarker },
			want:   "aibotid",
		},
		{
			name:   "a group message",
			mutate: func(b map[string]any) { b["chattype"] = "group"; b["chatid"] = "room-1" },
			want:   "chattype",
		},
		{
			name:   "a message with no chattype",
			mutate: func(b map[string]any) { delete(b, "chattype") },
			want:   "chattype",
		},
		{
			name:   "a single chat carrying a group id",
			mutate: func(b map[string]any) { b["chatid"] = "room-1" },
			want:   "chatid",
		},
		{
			name:   "an image message",
			mutate: func(b map[string]any) { b["msgtype"] = "image"; delete(b, "text") },
			want:   "msgtype",
		},
		{
			name:   "a mixed message",
			mutate: func(b map[string]any) { b["msgtype"] = "mixed" },
			want:   "msgtype",
		},
		{
			name:   "an event",
			mutate: func(b map[string]any) { b["msgtype"] = "event" },
			want:   "msgtype",
		},
		{
			name:   "a text message with no text",
			mutate: func(b map[string]any) { delete(b, "text") },
			want:   "text is missing",
		},
		{
			name:   "blank text",
			mutate: func(b map[string]any) { b["text"] = map[string]any{"content": "  \n "} },
			want:   "text is blank",
		},
		{
			name: "text past the bound",
			mutate: func(b map[string]any) {
				b["text"] = map[string]any{"content": strings.Repeat("a", maxInboundTextBytes+1)}
			},
			want: "text is longer",
		},
		{
			name:   "no message id",
			mutate: func(b map[string]any) { delete(b, "msgid") },
			want:   "msgid is empty",
		},
		{
			name: "a message id past the bound",
			mutate: func(b map[string]any) {
				b["msgid"] = strings.Repeat("m", maxExternalIDBytes+1)
			},
			want: "msgid is longer",
		},
		{
			name:   "no sender",
			mutate: func(b map[string]any) { delete(b, "from") },
			want:   "from.userid is empty",
		},
		{
			name:   "an empty sender",
			mutate: func(b map[string]any) { b["from"] = map[string]any{"userid": ""} },
			want:   "from.userid is empty",
		},
		{
			name:    "a body that is not an object",
			rawBody: json.RawMessage(`"` + bodyMarker + `"`),
			want:    "body is not an object",
		},
		{
			name:   "no req_id to reply on",
			reqID:  "-",
			mutate: func(map[string]any) {},
			want:   "req_id is empty",
		},
		{
			name:   "a req_id past the bound",
			reqID:  strings.Repeat("r", maxExternalIDBytes+1),
			mutate: func(map[string]any) {},
			want:   "req_id is longer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reqID := "callback-req-1"
			switch tc.reqID {
			case "":
			case "-":
				reqID = ""
			default:
				reqID = tc.reqID
			}
			body := textCallback()
			if tc.mutate != nil {
				tc.mutate(body)
			}
			in := callbackFrame(t, reqID, body)
			if tc.rawBody != nil {
				in.Body = tc.rawBody
			}

			msg, err := normalizeDirectText(binding, in, "generation-1", time.Now())

			require.ErrorIs(t, err, errRejected)
			require.Contains(t, err.Error(), tc.want)
			require.Equal(t, DirectText{}, msg)
			requireRedacted(t, err)
		})
	}
}

// checkExternalID is the guard on everything that reaches a digest. The
// UTF-8 rule cannot be reached through encoding/json, which substitutes
// U+FFFD for invalid bytes while decoding, so it is exercised directly: it is
// there for any other decoder, and for the day one is added.
func TestCheckExternalIDGuardsDigestInputs(t *testing.T) {
	require.NoError(t, checkExternalID("msgid", msgIDMarker))
	require.ErrorIs(t, checkExternalID("msgid", ""), errRejected)
	require.ErrorIs(t,
		checkExternalID("msgid", strings.Repeat("m", maxExternalIDBytes+1)), errRejected)
	err := checkExternalID("from.userid", "user-\xff\xfe")
	require.ErrorIs(t, err, errRejected)
	require.Contains(t, err.Error(), "not valid UTF-8")
}

// The reply limit is a byte count, and the boundary is exact. Text past it is
// refused rather than truncated: cutting bytes out of an answer either splits
// a rune or ships something the caller did not write.
func TestValidateReplyTextBoundary(t *testing.T) {
	require.NoError(t, validateReplyText(strings.Repeat("a", maxReplyTextBytes)))
	require.ErrorIs(t, validateReplyText(strings.Repeat("a", maxReplyTextBytes+1)), ErrTextTooLong)

	// Multi-byte text is measured in bytes, not runes: 6826 three-byte runes
	// plus two ASCII characters is exactly the limit, and one more rune is
	// three bytes past it.
	multibyte := strings.Repeat("测", 6826) + "ab"
	require.Len(t, multibyte, maxReplyTextBytes)
	require.NoError(t, validateReplyText(multibyte))
	require.ErrorIs(t, validateReplyText(multibyte+"测"), ErrTextTooLong)

	require.ErrorIs(t, validateReplyText(""), ErrTextInvalid)
	require.ErrorIs(t, validateReplyText("   \n"), ErrTextInvalid)
	require.ErrorIs(t, validateReplyText("answer\xff"), ErrTextInvalid)
}

// A receipt is a success only when it says so. The three states are kept
// apart because two of them are answers and one of them is not.
func TestReceiptRequiresAnExplicitErrcode(t *testing.T) {
	zero, refusal := 0, 40001
	require.True(t, receipt{code: &zero}.ok())
	require.False(t, receipt{code: &zero}.refused())

	require.False(t, receipt{code: &refusal}.ok())
	require.True(t, receipt{code: &refusal}.refused())

	require.False(t, receipt{}.ok())
	require.False(t, receipt{}.refused())
	require.False(t, receipt{}.readable())
}

// A takeover is recognized by the event type and nothing else; other events
// are not takeovers and must not end a working connection.
func TestIsTakeover(t *testing.T) {
	takeover := frame{Cmd: cmdEventCallback,
		Body: json.RawMessage(`{"event":{"eventtype":"` + eventDisconnected + `"}}`)}
	require.True(t, isTakeover(takeover))

	for _, body := range []string{
		`{"event":{"eventtype":"enter_chat"}}`,
		`{"event":{}}`,
		`{}`,
		`"not an object"`,
	} {
		require.Falsef(t, isTakeover(frame{Cmd: cmdEventCallback, Body: json.RawMessage(body)}),
			"body %s is not a takeover", body)
	}
}

// An outbound frame carries the command, the req_id and the body, and a ping
// carries no body at all.
func TestEncodeFrame(t *testing.T) {
	payload, err := encodeFrame(cmdPing, "ping-1", nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"cmd":"ping","headers":{"req_id":"ping-1"}}`, string(payload))

	payload, err = encodeFrame(cmdRespond, "callback-req-1", respondBody{
		MsgType: msgTypeStream,
		Stream:  streamReply{ID: "stream-1", Content: replyMarker, Finish: true},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"cmd":"aibot_respond_msg","headers":{"req_id":"callback-req-1"},`+
		`"body":{"msgtype":"stream","stream":{"id":"stream-1","content":"`+replyMarker+
		`","finish":true}}}`, string(payload))
}

// Request ids are random and unique per frame: receipts are matched by exact
// equality, so a repeated id would let one answer satisfy another wait.
func TestNewReqIDIsUniqueAndPrefixed(t *testing.T) {
	seen := make(map[string]struct{}, 64)
	for i := 0; i < 64; i++ {
		id, err := newReqID(cmdPing)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(id, cmdPing+"-"))
		require.NotContains(t, seen, id)
		seen[id] = struct{}{}
	}
}
