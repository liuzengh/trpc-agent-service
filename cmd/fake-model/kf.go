package main

// The 微信客服 (WeChat customer service) stub of fake-model.
//
// The reliable path's jobs role pulls from a KF host and the delivery role
// sends to one; both need an endpoint that answers the official wire shapes
// (gettoken / kf/sync_msg / kf/send_msg) or a Compose deployment could never
// exercise them. This is the same idea as the chat-completion stub one file
// over: an upstream that is reachable, scriptable, and answers exactly what
// the platform expects — nothing more.
//
//	POST /__kf_script   queue pages the next sync_msg calls will serve
//	GET  /__kf_sent     everything kf/send_msg has been asked to deliver
//
// A script is a queue, not a fixture: the first sync_msg consumes the first
// page, so a test can prove pagination by queueing two. When the queue is
// empty sync_msg answers with an empty page and has_more=0, which is also the
// honest answer for "a script forgot to set pages".

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

const (
	kfTokenPath   = "/cgi-bin/gettoken"
	kfSyncPath    = "/cgi-bin/kf/sync_msg"
	kfSendPath    = "/cgi-bin/kf/send_msg"
	kfScriptPath  = "/__kf_script"
	kfSentPath    = "/__kf_sent"
	kfAccessToken = "FAKE-KF-TOKEN"
	kfSentMsgID   = "FAKE-KF-MSGID"
	kfTokenTTL    = 7200
	kfDefaultUser = "ext-user-1"
	kfDefaultKfID = "wk-fake-1"
)

// kfScriptMsg is one scripted inbound message.
type kfScriptMsg struct {
	Msgid          string `json:"msgid"`
	OpenKfID       string `json:"open_kfid,omitempty"`
	ExternalUserID string `json:"external_userid"`
	Content        string `json:"content"`
	// SendTime defaults to now; scripts that want old messages set it.
	SendTime int64 `json:"send_time,omitempty"`
}

// kfScriptPage is one page the next sync_msg call will serve.
type kfScriptPage struct {
	NextCursor string        `json:"next_cursor"`
	HasMore    int           `json:"has_more"`
	Messages   []kfScriptMsg `json:"messages"`
	// ErrCode, when non-zero, makes sync_msg fail with this official error
	// code: fault injection at the KF layer, the same way modes do it at the
	// model layer.
	ErrCode int    `json:"errcode,omitempty"`
	ErrMsg  string `json:"errmsg,omitempty"`
}

// kfScript is the POST /__kf_script body.
type kfScript struct {
	Pages []kfScriptPage `json:"pages"`
	// Reset clears the sent log too; scripts start each case with one call.
	Reset bool `json:"reset,omitempty"`
}

// kfSentMsg is one recorded kf/send_msg delivery.
type kfSentMsg struct {
	ToUser   string `json:"touser"`
	OpenKfID string `json:"open_kfid"`
	Content  string `json:"content"`
}

// kfState is the KF stub's mutable side, guarded because the handlers run on
// concurrent request goroutines (the delivery role may send while the jobs
// role pulls).
type kfState struct {
	log *log.Logger

	mu    sync.Mutex
	pages []kfScriptPage
	sent  []kfSentMsg
}

func (s *server) handleKF(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case kfScriptPath:
		s.kf.setScript(w, r)
	case kfSentPath:
		s.kf.getSent(w, r)
	case kfTokenPath:
		writeJSON(w, http.StatusOK, map[string]any{
			"errcode": 0, "errmsg": "ok",
			"access_token": kfAccessToken, "expires_in": kfTokenTTL,
		})
	case kfSyncPath:
		s.kf.syncMsg(w, r)
	case kfSendPath:
		s.kf.sendMsg(w, r)
	default:
		writeErr(w, http.StatusNotFound, "unknown KF endpoint "+r.URL.Path)
	}
}

func (s *kfState) setScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST a script")
		return
	}
	var script kfScript
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&script); err != nil {
		writeErr(w, http.StatusBadRequest, "decode script: "+err.Error())
		return
	}
	s.mu.Lock()
	s.pages = append([]kfScriptPage(nil), script.Pages...)
	if script.Reset {
		s.sent = nil
	}
	queued := len(s.pages)
	s.mu.Unlock()
	s.log.Printf("kf: script set with %d page(s)", queued)
	writeJSON(w, http.StatusOK, map[string]any{"pages": queued})
}

func (s *kfState) getSent(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	sent := append([]kfSentMsg(nil), s.sent...)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"sent": sent})
}

type kfSyncRequest struct {
	Cursor   string `json:"cursor,omitempty"`
	Token    string `json:"token,omitempty"`
	Limit    int    `json:"limit"`
	OpenKfID string `json:"open_kfid"`
}

type kfSyncMessage struct {
	Msgid          string `json:"msgid"`
	OpenKfID       string `json:"open_kfid"`
	ExternalUserID string `json:"external_userid"`
	SendTime       int64  `json:"send_time"`
	Origin         int    `json:"origin"`
	MsgType        string `json:"msgtype"`
	Text           struct {
		Content string `json:"content"`
	} `json:"text"`
}

func (s *kfState) syncMsg(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST a sync_msg request")
		return
	}
	var req kfSyncRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "decode sync_msg: "+err.Error())
		return
	}

	s.mu.Lock()
	var page kfScriptPage
	if len(s.pages) > 0 {
		page, s.pages = s.pages[0], s.pages[1:]
	}
	s.mu.Unlock()
	s.log.Printf("kf: sync_msg cursor=%q token=%q open_kfid=%q -> %d msg(s), has_more=%d",
		req.Cursor, req.Token, req.OpenKfID, len(page.Messages), page.HasMore)

	if page.ErrCode != 0 {
		writeJSON(w, http.StatusOK, map[string]any{
			"errcode": page.ErrCode, "errmsg": page.ErrMsg,
		})
		return
	}

	msgs := make([]kfSyncMessage, 0, len(page.Messages))
	for _, m := range page.Messages {
		openKfID := m.OpenKfID
		if openKfID == "" {
			openKfID = req.OpenKfID
		}
		user := m.ExternalUserID
		if user == "" {
			user = kfDefaultUser
		}
		entry := kfSyncMessage{
			Msgid: m.Msgid, OpenKfID: openKfID, ExternalUserID: user,
			SendTime: m.SendTime, Origin: 3, MsgType: "text",
		}
		entry.Text.Content = m.Content
		msgs = append(msgs, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"errcode": 0, "errmsg": "ok",
		"next_cursor": page.NextCursor, "has_more": page.HasMore,
		"msg_list": msgs,
	})
}

func (s *kfState) sendMsg(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST a send_msg request")
		return
	}
	var req struct {
		ToUser   string `json:"touser"`
		OpenKfID string `json:"open_kfid"`
		MsgType  string `json:"msgtype"`
		Text     struct {
			Content string `json:"content"`
		} `json:"text"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "decode send_msg: "+err.Error())
		return
	}
	s.mu.Lock()
	s.sent = append(s.sent, kfSentMsg{ToUser: req.ToUser, OpenKfID: req.OpenKfID, Content: req.Text.Content})
	total := len(s.sent)
	s.mu.Unlock()
	s.log.Printf("kf: send_msg #%d to=%q open_kfid=%q bytes=%d",
		total, req.ToUser, req.OpenKfID, len(req.Text.Content))
	writeJSON(w, http.StatusOK, map[string]any{"errcode": 0, "errmsg": "ok", "msgid": kfSentMsgID})
}
